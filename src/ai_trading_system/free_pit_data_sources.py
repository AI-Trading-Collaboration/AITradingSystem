from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START
from ai_trading_system.features.event_risk_free import build_event_risk_free_features
from ai_trading_system.features.macro_event_calendar_free import (
    build_macro_event_calendar_free_features,
)
from ai_trading_system.features.rates_liquidity_free import (
    build_rates_liquidity_free_features,
)
from ai_trading_system.features.volatility_compression_free import (
    build_volatility_compression_free_features,
)
from ai_trading_system.free_data_connectors import (
    CboeVixConnector,
    FredConnector,
    OfficialMacroCalendarConnector,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "data" / "free_data_source_registry.yaml"
)
DEFAULT_FREE_FEATURE_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "free_feature_policy.yaml"
DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "participation_proxy_free_registry.yaml"
)
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv"
DEFAULT_FREE_SOURCE_OUTPUT_ROOT = PROJECT_ROOT / "data" / "processed" / "free_sources"
DEFAULT_FREE_FEATURE_OUTPUT_ROOT = PROJECT_ROOT / "data" / "features"
DEFAULT_RESEARCH_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_RESEARCH_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"

FRED_SERIES = ("DGS2", "DGS10", "DGS3MO", "FEDFUNDS", "SOFR", "DTWEXBGS", "VIXCLS")
REVISION_SENSITIVE_MACRO = frozenset({"CPIAUCSL", "PCEPI", "PAYEMS", "UNRATE"})
REQUIRED_CONTRACT_FIELDS = (
    "source_id",
    "provider",
    "free_or_paid",
    "official_source",
    "api_required",
    "api_key_required",
    "earliest_available_date",
    "update_frequency",
    "timestamp_timezone",
    "PIT_status",
    "revision_risk",
    "vintage_support",
    "allowed_usage",
    "blocked_usage",
    "caveats",
)
SAFETY_BOUNDARY = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def load_free_data_source_registry(
    path: Path = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError("free data source registry must be a mapping")
    return raw


def validate_free_data_source_registry(payload: Mapping[str, Any]) -> dict[str, Any]:
    sources = _records(payload.get("sources"))
    issues: list[dict[str, str]] = []
    seen: set[str] = set()
    for source in sources:
        source_id = _string(source.get("source_id"))
        if not source_id:
            issues.append(_issue("ERROR", "missing_source_id", "source is missing source_id"))
            continue
        if source_id in seen:
            issues.append(_issue("ERROR", "duplicate_source_id", f"duplicate {source_id}"))
        seen.add(source_id)
        for field in REQUIRED_CONTRACT_FIELDS:
            if field not in source:
                issues.append(
                    _issue(
                        "ERROR",
                        "missing_contract_field",
                        f"{source_id} missing {field}",
                        source_id=source_id,
                    )
                )
        if source.get("free_or_paid") != "free":
            issues.append(
                _issue(
                    "ERROR",
                    "non_free_source_in_free_registry",
                    f"{source_id} is not free",
                    source_id=source_id,
                )
            )
        allowed = set(_strings(source.get("allowed_usage")))
        request_series = set(_strings(_mapping(source.get("request_parameters")).get("series")))
        if request_series & REVISION_SENSITIVE_MACRO and not bool(source.get("vintage_support")):
            issues.append(
                _issue(
                    "ERROR",
                    "revision_sensitive_macro_requires_vintage",
                    f"{source_id} includes revision-sensitive macro series without vintage support",
                    source_id=source_id,
                )
            )
        if request_series & REVISION_SENSITIVE_MACRO and any(
            "model_ready" in item for item in allowed
        ):
            issues.append(
                _issue(
                    "ERROR",
                    "revision_sensitive_macro_model_ready_without_vintage_audit",
                    f"{source_id} cannot mark revision-sensitive macro model-ready here",
                    source_id=source_id,
                )
            )
    error_count = len([issue for issue in issues if issue["severity"] == "ERROR"])
    warning_count = len([issue for issue in issues if issue["severity"] == "WARNING"])
    return {
        "schema_version": "free_data_source_registry_validation.v1",
        "status": "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS",
        "summary": {
            "source_count": len(sources),
            "error_count": error_count,
            "warning_count": warning_count,
        },
        "issues": issues,
    }


def run_free_data_source_ingestion(
    *,
    registry_path: Path = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    feature_policy_path: Path = DEFAULT_FREE_FEATURE_POLICY_PATH,
    participation_proxy_registry_path: Path = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    output_root: Path = DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    feature_output_root: Path = DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    calendar_input_path: Path | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    run_date = as_of_date or date.today()
    registry = load_free_data_source_registry(registry_path)
    feature_policy = _load_mapping(feature_policy_path)
    participation = _load_mapping(participation_proxy_registry_path)
    registry_validation = validate_free_data_source_registry(registry)
    if registry_validation["status"] == "FAIL":
        raise ValueError("free data source registry validation failed")
    data_quality = _validate_cached_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        as_of_date=run_date,
    )
    if not data_quality.passed:
        raise ValueError(f"cached data quality gate failed: {data_quality.status}")

    output_root.mkdir(parents=True, exist_ok=True)
    feature_output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    fred = (
        FredConnector().read_cached_series(rates_path, series_ids=list(FRED_SERIES))
        if rates_path.exists()
        else _empty_fred_frame()
    )
    fred_path = output_root / "fred_market_series.parquet"
    _write_parquet(fred, fred_path)

    prices = pd.read_csv(prices_path) if prices_path.exists() else pd.DataFrame()
    vix = CboeVixConnector().from_price_cache(prices_path) if prices_path.exists() else _empty_vix()
    vix_path = output_root / "vix_history.parquet"
    _write_parquet(vix, vix_path)

    calendar_events = _load_calendar_events(calendar_input_path)
    macro_calendar = OfficialMacroCalendarConnector().normalize_events(calendar_events)
    calendar_path = output_root / "macro_event_calendar.parquet"
    _write_parquet(macro_calendar, calendar_path)

    rates_features = build_rates_liquidity_free_features(fred, feature_policy)
    rates_features_path = feature_output_root / "rates_liquidity_free_v1.parquet"
    _write_parquet(rates_features, rates_features_path)

    volatility_features = build_volatility_compression_free_features(vix, prices, feature_policy)
    volatility_features_path = feature_output_root / "volatility_compression_free_v1.parquet"
    _write_parquet(volatility_features, volatility_features_path)

    macro_features = build_macro_event_calendar_free_features(macro_calendar)
    macro_features_path = feature_output_root / "macro_event_calendar_free_v1.parquet"
    _write_parquet(macro_features, macro_features_path)

    event_risk_features = build_event_risk_free_features(
        calendar_features=macro_features,
        rates_features=rates_features,
        volatility_features=volatility_features,
        policy=feature_policy,
    )
    event_risk_features_path = feature_output_root / "event_risk_free_v1.parquet"
    _write_parquet(event_risk_features, event_risk_features_path)

    artifacts = _artifact_paths(
        output_root=output_root,
        feature_output_root=feature_output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        fred_path=fred_path,
        vix_path=vix_path,
        calendar_path=calendar_path,
        rates_features_path=rates_features_path,
        volatility_features_path=volatility_features_path,
        macro_features_path=macro_features_path,
        event_risk_features_path=event_risk_features_path,
    )
    summary = _summary(
        fred=fred,
        vix=vix,
        macro_calendar=macro_calendar,
        rates_features=rates_features,
        volatility_features=volatility_features,
        macro_features=macro_features,
        event_risk_features=event_risk_features,
        data_quality=data_quality,
    )
    scope_payload = _scope_payload(run_date)
    registry_review = _registry_review_payload(registry, registry_validation)
    fred_summary = _fred_summary_payload(fred, summary)
    vix_review = _vix_review_payload(vix, fred)
    calendar_review = _calendar_review_payload(macro_calendar)
    pit_contract = _calendar_pit_contract_payload()
    pit_audit = _pit_audit_payload(summary, participation)
    coverage = _coverage_matrix_payload(summary, fred, vix, macro_calendar)
    readiness = _readiness_payload(summary, pit_audit, coverage)
    final_matrix = _final_matrix_payload(readiness)

    _write_yaml(artifacts["free_pit_data_source_scope_yaml"], scope_payload)
    _write_markdown(artifacts["free_pit_data_source_scope_doc"], _render_scope(scope_payload))
    _write_markdown(
        artifacts["free_data_source_registry_review"],
        _render_registry(registry_review),
    )
    _write_yaml(artifacts["fred_series_ingestion_summary"], fred_summary)
    _write_markdown(artifacts["fred_series_ingestion_review"], _render_fred_summary(fred_summary))
    _write_markdown(artifacts["vix_free_source_crosscheck_review"], _render_vix_review(vix_review))
    _write_markdown(
        artifacts["official_macro_calendar_ingestion_review"],
        _render_calendar_review(calendar_review),
    )
    _write_yaml(artifacts["calendar_event_pit_contract_yaml"], pit_contract)
    _write_markdown(
        artifacts["calendar_event_pit_contract_doc"], _render_calendar_pit_contract(pit_contract)
    )
    _write_markdown(
        artifacts["rates_liquidity_free_v1_feature_review"],
        _render_feature_review("rates_liquidity_free_v1", rates_features, summary),
    )
    _write_markdown(
        artifacts["volatility_compression_free_v1_feature_review"],
        _render_feature_review("volatility_compression_free_v1", volatility_features, summary),
    )
    _write_markdown(
        artifacts["macro_event_calendar_free_v1_feature_review"],
        _render_feature_review("macro_event_calendar_free_v1", macro_features, summary),
    )
    _write_markdown(
        artifacts["event_risk_free_v1_feature_review"],
        _render_feature_review("event_risk_free_v1", event_risk_features, summary),
    )
    _write_yaml(artifacts["free_feature_pit_audit_yaml"], pit_audit)
    _write_markdown(artifacts["free_feature_pit_audit_doc"], _render_pit_audit(pit_audit))
    _write_yaml(artifacts["free_data_feature_coverage_matrix_yaml"], coverage)
    _write_markdown(
        artifacts["free_data_feature_coverage_matrix_doc"],
        _render_coverage_matrix(coverage),
    )
    _write_yaml(artifacts["free_feature_family_reopen_readiness_yaml"], readiness)
    _write_markdown(
        artifacts["free_feature_family_reopen_readiness_doc"],
        _render_readiness(readiness),
    )
    _write_markdown(
        artifacts["participation_proxy_free_registry_review"],
        _render_participation_registry(participation),
    )
    _write_markdown(artifacts["free_pit_data_source_owner_brief"], _render_owner_brief(readiness))
    _write_yaml(artifacts["free_pit_data_source_ingestion_final_matrix"], final_matrix)
    _write_markdown(
        artifacts["free_pit_data_source_ingestion_closeout"],
        _render_closeout(final_matrix),
    )

    status = _overall_status(readiness)
    payload: dict[str, Any] = {
        "schema_version": "free_pit_data_source_ingestion.v1",
        "report_type": "free_pit_data_source_ingestion",
        "status": status,
        "as_of": run_date.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "market_regime": "ai_after_chatgpt",
        "actual_requested_date_range": _date_range_label(fred, vix),
        "data_quality_status": data_quality.status,
        "summary": summary,
        "registry_validation": registry_validation,
        "safety_boundary": SAFETY_BOUNDARY,
        "artifact_paths": {key: str(value) for key, value in artifacts.items()},
        **SAFETY_BOUNDARY,
    }
    return payload


def run_free_data_source_validation(
    *,
    registry_path: Path = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    participation_proxy_registry_path: Path = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_free_data_source_registry(registry_path)
    registry_validation = validate_free_data_source_registry(registry)
    participation = _load_mapping(participation_proxy_registry_path)
    proxy_issues = _validate_participation_proxies(participation)
    errors = [
        *[issue for issue in registry_validation["issues"] if issue["severity"] == "ERROR"],
        *[issue for issue in proxy_issues if issue["severity"] == "ERROR"],
    ]
    warnings = [
        *[issue for issue in registry_validation["issues"] if issue["severity"] == "WARNING"],
        *[issue for issue in proxy_issues if issue["severity"] == "WARNING"],
    ]
    return {
        "schema_version": "free_data_source_validation.v1",
        "report_type": "free_data_source_validation",
        "status": "FAIL" if errors else "PASS_WITH_WARNINGS" if warnings else "PASS",
        "summary": {
            "registry_source_count": registry_validation["summary"]["source_count"],
            "error_count": len(errors),
            "warning_count": len(warnings),
            "participation_proxy_count": len(_records(participation.get("proxies"))),
        },
        "issues": [*errors, *warnings],
        **SAFETY_BOUNDARY,
    }


def run_free_feature_readiness(
    *,
    registry_path: Path = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    feature_policy_path: Path = DEFAULT_FREE_FEATURE_POLICY_PATH,
    participation_proxy_registry_path: Path = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    output_root: Path = DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    feature_output_root: Path = DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    calendar_input_path: Path | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    payload = run_free_data_source_ingestion(
        registry_path=registry_path,
        feature_policy_path=feature_policy_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
        rates_path=rates_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        output_root=output_root,
        feature_output_root=feature_output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        calendar_input_path=calendar_input_path,
        as_of_date=as_of_date,
    )
    payload["report_type"] = "free_feature_family_reopen_readiness"
    return payload


def _validate_cached_data(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    manifest_path: Path,
    as_of_date: date,
) -> DataQualityReport:
    universe = load_universe()
    return validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of_date,
        manifest_path=manifest_path if manifest_path.exists() else None,
        secondary_prices_path=marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None,
        require_secondary_prices=False,
    )


def _load_calendar_events(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() in {".yaml", ".yml"}:
        raw = safe_load_yaml_path(path)
        return pd.DataFrame(_records(raw.get("events") if isinstance(raw, Mapping) else raw))
    return pd.read_csv(path)


def _summary(
    *,
    fred: pd.DataFrame,
    vix: pd.DataFrame,
    macro_calendar: pd.DataFrame,
    rates_features: pd.DataFrame,
    volatility_features: pd.DataFrame,
    macro_features: pd.DataFrame,
    event_risk_features: pd.DataFrame,
    data_quality: DataQualityReport,
) -> dict[str, Any]:
    available_series = sorted(set(fred["series"].dropna().astype(str))) if not fred.empty else []
    missing_series = [series for series in FRED_SERIES if series not in set(available_series)]
    return {
        "data_quality_status": data_quality.status,
        "fred_market_series_rows": len(fred),
        "available_fred_series": available_series,
        "missing_fred_series": missing_series,
        "vix_row_count": len(vix),
        "macro_calendar_event_count": len(macro_calendar),
        "rates_liquidity_feature_rows": len(rates_features),
        "volatility_compression_feature_rows": len(volatility_features),
        "macro_event_calendar_feature_rows": len(macro_features),
        "event_risk_feature_rows": len(event_risk_features),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _artifact_paths(
    *,
    output_root: Path,
    feature_output_root: Path,
    docs_root: Path,
    inputs_root: Path,
    fred_path: Path,
    vix_path: Path,
    calendar_path: Path,
    rates_features_path: Path,
    volatility_features_path: Path,
    macro_features_path: Path,
    event_risk_features_path: Path,
) -> dict[str, Path]:
    return {
        "fred_market_series": fred_path,
        "vix_history": vix_path,
        "macro_event_calendar": calendar_path,
        "rates_liquidity_free_v1": rates_features_path,
        "volatility_compression_free_v1": volatility_features_path,
        "macro_event_calendar_free_v1": macro_features_path,
        "event_risk_free_v1": event_risk_features_path,
        "free_pit_data_source_scope_doc": docs_root / "free_pit_data_source_scope.md",
        "free_pit_data_source_scope_yaml": inputs_root / "free_pit_data_source_scope.yaml",
        "free_data_source_registry_review": docs_root / "free_data_source_registry_review.md",
        "fred_series_ingestion_summary": inputs_root / "fred_series_ingestion_summary.yaml",
        "fred_series_ingestion_review": docs_root / "fred_series_ingestion_review.md",
        "vix_free_source_crosscheck_review": docs_root / "vix_free_source_crosscheck_review.md",
        "official_macro_calendar_ingestion_review": docs_root
        / "official_macro_calendar_ingestion_review.md",
        "calendar_event_pit_contract_yaml": inputs_root / "calendar_event_pit_contract.yaml",
        "calendar_event_pit_contract_doc": docs_root / "calendar_event_pit_contract.md",
        "rates_liquidity_free_v1_feature_review": docs_root
        / "rates_liquidity_free_v1_feature_review.md",
        "volatility_compression_free_v1_feature_review": docs_root
        / "volatility_compression_free_v1_feature_review.md",
        "macro_event_calendar_free_v1_feature_review": docs_root
        / "macro_event_calendar_free_v1_feature_review.md",
        "event_risk_free_v1_feature_review": docs_root / "event_risk_free_v1_feature_review.md",
        "participation_proxy_free_registry_review": docs_root
        / "participation_proxy_free_registry_review.md",
        "free_feature_pit_audit_yaml": inputs_root / "free_feature_pit_audit.yaml",
        "free_feature_pit_audit_doc": docs_root / "free_feature_pit_audit.md",
        "free_data_feature_coverage_matrix_yaml": inputs_root
        / "free_data_feature_coverage_matrix.yaml",
        "free_data_feature_coverage_matrix_doc": docs_root
        / "free_data_feature_coverage_matrix.md",
        "free_feature_family_reopen_readiness_yaml": inputs_root
        / "free_feature_family_reopen_readiness.yaml",
        "free_feature_family_reopen_readiness_doc": docs_root
        / "free_feature_family_reopen_readiness.md",
        "free_pit_data_source_owner_brief": docs_root / "free_pit_data_source_owner_brief.md",
        "free_pit_data_source_ingestion_closeout": docs_root
        / "free_pit_data_source_ingestion_closeout.md",
        "free_pit_data_source_ingestion_final_matrix": inputs_root
        / "free_pit_data_source_ingestion_final_matrix.yaml",
    }


def _scope_payload(run_date: date) -> dict[str, Any]:
    return {
        "schema_version": "free_pit_data_source_scope.v1",
        "report_type": "free_pit_data_source_scope",
        "status": "FREE_PIT_DATA_SOURCE_SCOPE_READY_RESEARCH_ONLY",
        "as_of": run_date.isoformat(),
        "market_regime": {
            "regime_id": "ai_after_chatgpt",
            "anchor_event": "ChatGPT public launch",
            "anchor_date": "2022-11-30",
            "default_backtest_start": PRIMARY_RESEARCH_START,
        },
        "scope": {
            "free_data_only": True,
            "paid_data_sources_allowed": False,
            "true_pit_breadth_built": False,
            "research_only": True,
            "promotion_status": "BLOCKED",
        },
        "non_goals": [
            "no_paid_data_purchase",
            "no_true_pit_breadth",
            "no_current_constituent_history_backfill",
            "no_first_layer_research_restart",
            "no_second_layer_probe_change",
            "no_promotion_or_broker",
        ],
        **SAFETY_BOUNDARY,
    }


def _registry_review_payload(
    registry: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "free_data_source_registry_review.v1",
        "report_type": "free_data_source_registry_review",
        "status": "FREE_DATA_SOURCE_REGISTRY_READY",
        "summary": validation.get("summary", {}),
        "source_ids": [
            _string(source.get("source_id")) for source in _records(registry.get("sources"))
        ],
        "validation": validation,
        **SAFETY_BOUNDARY,
    }


def _fred_summary_payload(fred: pd.DataFrame, summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "fred_series_ingestion_summary.v1",
        "report_type": "fred_series_ingestion_summary",
        "status": "FRED_SERIES_INGESTED_WITH_COVERAGE_WARNINGS"
        if summary.get("missing_fred_series")
        else "FRED_SERIES_INGESTED",
        "summary": {
            "row_count": len(fred),
            "available_series": summary.get("available_fred_series", []),
            "missing_series": summary.get("missing_fred_series", []),
        },
        **SAFETY_BOUNDARY,
    }


def _vix_review_payload(vix: pd.DataFrame, fred: pd.DataFrame) -> dict[str, Any]:
    has_fred_vix = not fred.empty and "VIXCLS" in set(fred["series"].astype(str))
    return {
        "schema_version": "vix_free_source_crosscheck_review.v1",
        "report_type": "vix_free_source_crosscheck_review",
        "status": "VIX_FREE_SOURCE_AVAILABLE_CROSSCHECK_PENDING"
        if not has_fred_vix
        else "VIX_FREE_SOURCE_CROSSCHECK_READY",
        "summary": {
            "vix_row_count": len(vix),
            "fred_vixcls_available": has_fred_vix,
            "primary_source": "cboe_vix_historical",
        },
        **SAFETY_BOUNDARY,
    }


def _calendar_review_payload(macro_calendar: pd.DataFrame) -> dict[str, Any]:
    warning_count = (
        int((macro_calendar["PIT_status"] == "PIT_WARNING").sum())
        if not macro_calendar.empty and "PIT_status" in macro_calendar
        else 0
    )
    return {
        "schema_version": "official_macro_calendar_ingestion_review.v1",
        "report_type": "official_macro_calendar_ingestion_review",
        "status": "OFFICIAL_MACRO_CALENDAR_DIAGNOSTIC_ONLY"
        if warning_count or macro_calendar.empty
        else "OFFICIAL_MACRO_CALENDAR_INGESTED",
        "summary": {
            "event_count": len(macro_calendar),
            "pit_warning_count": warning_count,
            "diagnostic_only": bool(warning_count or macro_calendar.empty),
        },
        **SAFETY_BOUNDARY,
    }


def _calendar_pit_contract_payload() -> dict[str, Any]:
    return {
        "schema_version": "calendar_event_pit_contract.v1",
        "report_type": "calendar_event_pit_contract",
        "status": "CALENDAR_EVENT_PIT_CONTRACT_READY",
        "required_fields": [
            "event_date",
            "scheduled_release_time",
            "source_published_at",
            "known_at",
            "available_at",
            "timezone",
            "revision_policy",
        ],
        "missing_source_published_at_policy": {
            "PIT_status": "PIT_WARNING",
            "allowed_usage": "diagnostic_only",
            "model_ready": False,
        },
        **SAFETY_BOUNDARY,
    }


def _pit_audit_payload(
    summary: Mapping[str, Any],
    participation: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        _pit_row(
            "rates_liquidity_free_v1",
            "PIT_APPROVED" if summary.get("rates_liquidity_feature_rows", 0) else "PIT_BLOCKED",
            "model_research_if_required_series_present",
        ),
        _pit_row(
            "volatility_compression_free_v1",
            "PIT_APPROVED"
            if summary.get("volatility_compression_feature_rows", 0)
            else "PIT_BLOCKED",
            "model_research_if_vix_and_qqq_present",
        ),
        _pit_row(
            "macro_event_calendar_free_v1",
            "PIT_WARNING_DIAGNOSTIC_ONLY",
            "known_at_or_warning_required",
        ),
        _pit_row(
            "event_risk_free_v1",
            "PIT_WARNING_DIAGNOSTIC_ONLY",
            "calendar_based_not_news_event_risk",
        ),
        _pit_row(
            "participation_proxy_free_v1",
            "PIT_WARNING_DIAGNOSTIC_ONLY",
            "not_true_pit_breadth",
        ),
    ]
    return {
        "schema_version": "free_feature_pit_audit.v1",
        "report_type": "free_feature_pit_audit",
        "status": "FREE_FEATURE_PIT_AUDIT_READY_WITH_WARNINGS",
        "rows": rows,
        "participation_proxy_caveats": [
            caveat
            for proxy in _records(participation.get("proxies"))
            for caveat in _strings(proxy.get("caveats"))
        ],
        **SAFETY_BOUNDARY,
    }


def _coverage_matrix_payload(
    summary: Mapping[str, Any],
    fred: pd.DataFrame,
    vix: pd.DataFrame,
    macro_calendar: pd.DataFrame,
) -> dict[str, Any]:
    rows = [
        _coverage_row(
            "rates_liquidity_free_v1",
            fred,
            row_count=int(summary.get("rates_liquidity_feature_rows", 0)),
            missing_rate=_missing_rate(summary.get("missing_fred_series", []), FRED_SERIES),
            revision_risk="low",
            allowed_usage="risk_on_veto_research",
            blocked_usage="promotion,paper_shadow,production,broker",
        ),
        _coverage_row(
            "volatility_compression_free_v1",
            vix,
            row_count=int(summary.get("volatility_compression_feature_rows", 0)),
            missing_rate=0.0 if summary.get("vix_row_count", 0) else 1.0,
            revision_risk="none",
            allowed_usage="risk_on_veto_research",
            blocked_usage="promotion,paper_shadow,production,broker",
        ),
        _coverage_row(
            "macro_event_calendar_free_v1",
            macro_calendar,
            row_count=int(summary.get("macro_event_calendar_feature_rows", 0)),
            missing_rate=1.0 if not summary.get("macro_calendar_event_count", 0) else 0.0,
            revision_risk="low",
            allowed_usage="diagnostic_only",
            blocked_usage="model_ready_without_known_at,promotion,paper_shadow,production,broker",
        ),
        _coverage_row(
            "event_risk_free_v1",
            macro_calendar,
            row_count=int(summary.get("event_risk_feature_rows", 0)),
            missing_rate=1.0 if not summary.get("macro_calendar_event_count", 0) else 0.0,
            revision_risk="mixed",
            allowed_usage="diagnostic_only",
            blocked_usage="promotion,paper_shadow,production,broker",
        ),
        {
            "feature_family": "participation_proxy_free_v1",
            "earliest_available_date": "source_dependent",
            "primary_window_coverage": "registry_only",
            "legacy_window_coverage": "registry_only",
            "sensitivity_window_coverage": "registry_only",
            "missing_rate": None,
            "calendar_coverage": "not_applicable",
            "revision_risk": "none_for_etf_price_only",
            "allowed_usage": "diagnostic_only",
            "blocked_usage": "true_pit_breadth,promotion,paper_shadow,production,broker",
        },
    ]
    return {
        "schema_version": "free_data_feature_coverage_matrix.v1",
        "report_type": "free_data_feature_coverage_matrix",
        "status": "FREE_DATA_FEATURE_COVERAGE_MATRIX_READY_WITH_WARNINGS",
        "rows": rows,
        **SAFETY_BOUNDARY,
    }


def _readiness_payload(
    summary: Mapping[str, Any],
    pit_audit: Mapping[str, Any],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    rows = [
        {
            "feature_family": "rates_liquidity_free_v1",
            "readiness_status": "READY_FOR_RATES_LIQUIDITY_RESEARCH"
            if {"DGS2", "DGS10", "DTWEXBGS"} <= set(summary.get("available_fred_series", []))
            else "DIAGNOSTIC_ONLY",
            "reason": "DGS2/DGS10/DTWEXBGS local PIT market series coverage evaluated.",
        },
        {
            "feature_family": "volatility_compression_free_v1",
            "readiness_status": "READY_FOR_VOLATILITY_COMPRESSION_RESEARCH"
            if summary.get("vix_row_count", 0)
            else "BLOCKED",
            "reason": "VIX index level and QQQ realized volatility baseline evaluated.",
        },
        {
            "feature_family": "macro_event_calendar_free_v1",
            "readiness_status": "READY_FOR_EVENT_RISK_DIAGNOSTIC"
            if summary.get("macro_calendar_event_count", 0)
            else "BLOCKED",
            "reason": (
                "Official calendar rows require captured known_at/source_published_at evidence."
            ),
        },
        {
            "feature_family": "event_risk_free_v1",
            "readiness_status": "READY_FOR_EVENT_RISK_DIAGNOSTIC"
            if summary.get("macro_calendar_event_count", 0)
            else "DIAGNOSTIC_ONLY",
            "reason": (
                "Overlay score can be produced, but calendar event risk remains incomplete "
                "without official rows."
            ),
        },
        {
            "feature_family": "participation_proxy_free_v1",
            "readiness_status": "DIAGNOSTIC_ONLY",
            "reason": "ETF ratios are not true PIT breadth and cannot enter promotion evidence.",
        },
    ]
    return {
        "schema_version": "free_feature_family_reopen_readiness.v1",
        "report_type": "free_feature_family_reopen_readiness",
        "status": "FREE_FEATURE_FAMILY_REOPEN_READINESS_READY_WITH_BLOCKERS",
        "rows": rows,
        "pit_audit_status": pit_audit.get("status"),
        "coverage_status": coverage.get("status"),
        **SAFETY_BOUNDARY,
    }


def _final_matrix_payload(readiness: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(readiness.get("rows"))
    statuses = {row.get("feature_family"): row.get("readiness_status") for row in rows}
    return {
        "schema_version": "free_pit_data_source_ingestion_final_matrix.v1",
        "report_type": "free_pit_data_source_ingestion_final_matrix",
        "status": "FREE_PIT_DATA_SOURCES_READY_WITH_BLOCKERS",
        "final_status": [
            "FREE_PIT_DATA_SOURCES_READY",
            "RATES_LIQUIDITY_FREE_READY"
            if statuses.get("rates_liquidity_free_v1") == "READY_FOR_RATES_LIQUIDITY_RESEARCH"
            else "RATES_LIQUIDITY_FREE_DIAGNOSTIC_ONLY",
            "VOLATILITY_COMPRESSION_FREE_READY"
            if statuses.get("volatility_compression_free_v1")
            == "READY_FOR_VOLATILITY_COMPRESSION_RESEARCH"
            else "VOLATILITY_COMPRESSION_FREE_BLOCKED",
            "MACRO_EVENT_CALENDAR_FREE_READY"
            if statuses.get("macro_event_calendar_free_v1") == "READY_FOR_EVENT_RISK_DIAGNOSTIC"
            else "MACRO_EVENT_CALENDAR_FREE_BLOCKED",
            "EVENT_RISK_FREE_DIAGNOSTIC_READY",
            "PARTICIPATION_PROXY_DIAGNOSTIC_ONLY",
            "PIT_BLOCKERS_REMAIN",
        ],
        "readiness_rows": rows,
        **SAFETY_BOUNDARY,
    }


def _overall_status(readiness: Mapping[str, Any]) -> str:
    rows = _records(readiness.get("rows"))
    if any(row.get("readiness_status") == "BLOCKED" for row in rows):
        return "FREE_PIT_DATA_SOURCES_READY_WITH_BLOCKERS"
    return "FREE_PIT_DATA_SOURCES_READY"


def _pit_row(feature_family: str, pit_status: str, rationale: str) -> dict[str, Any]:
    return {
        "feature_family": feature_family,
        "PIT_status": pit_status,
        "rationale": rationale,
        "allowed_usage": "diagnostic_only" if "WARNING" in pit_status else "research_only",
        "blocked_usage": "promotion,paper_shadow,production,broker",
    }


def _coverage_row(
    feature_family: str,
    frame: pd.DataFrame,
    *,
    row_count: int,
    missing_rate: float,
    revision_risk: str,
    allowed_usage: str,
    blocked_usage: str,
) -> dict[str, Any]:
    dates = _date_range(frame)
    coverage_label = "covered" if row_count else "missing"
    return {
        "feature_family": feature_family,
        "earliest_available_date": dates[0],
        "latest_available_date": dates[1],
        "primary_window_coverage": coverage_label,
        "legacy_window_coverage": coverage_label,
        "sensitivity_window_coverage": coverage_label,
        "missing_rate": missing_rate,
        "calendar_coverage": "not_applicable",
        "revision_risk": revision_risk,
        "allowed_usage": allowed_usage,
        "blocked_usage": blocked_usage,
    }


def _date_range(frame: pd.DataFrame) -> tuple[str, str]:
    if frame.empty:
        return ("missing", "missing")
    if "date" in frame.columns:
        column = "date"
    elif "event_date" in frame.columns:
        column = "event_date"
    else:
        column = ""
    if not column:
        return ("unknown", "unknown")
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    if values.empty:
        return ("missing", "missing")
    return (values.min().date().isoformat(), values.max().date().isoformat())


def _date_range_label(*frames: pd.DataFrame) -> str:
    ranges = [_date_range(frame) for frame in frames if not frame.empty]
    if not ranges:
        return "missing"
    starts = [item[0] for item in ranges if item[0] != "missing"]
    ends = [item[1] for item in ranges if item[1] != "missing"]
    return f"{min(starts)} to {max(ends)}" if starts and ends else "missing"


def _missing_rate(missing: object, expected: Sequence[str]) -> float:
    missing_count = len(_strings(missing))
    return missing_count / len(expected) if expected else 0.0


def _validate_participation_proxies(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for proxy in _records(payload.get("proxies")):
        proxy_id = _string(proxy.get("proxy_id"))
        caveats = set(_strings(proxy.get("caveats")))
        status = _string(proxy.get("status"))
        if "NOT_TRUE_PIT_BREADTH" not in caveats:
            issues.append(
                _issue(
                    "ERROR",
                    "participation_proxy_missing_true_breadth_caveat",
                    f"{proxy_id} missing NOT_TRUE_PIT_BREADTH",
                    source_id=proxy_id,
                )
            )
        if status not in {"DIAGNOSTIC_ONLY", "REGISTRY_ONLY"}:
            issues.append(
                _issue(
                    "ERROR",
                    "participation_proxy_not_diagnostic",
                    f"{proxy_id} is not diagnostic-only",
                    source_id=proxy_id,
                )
            )
    return issues


def _render_scope(payload: Mapping[str, Any]) -> str:
    scope = _mapping(payload.get("scope"))
    return "\n".join(
        [
            "# Free PIT Data Source Scope",
            "",
            f"- 状态：`{payload.get('status')}`",
            "- 市场 regime：`unified_primary_2021`，anchor=`2021-02-22`，"
            "default backtest start=`2021-02-22`。",
            f"- 只接免费数据：`{scope.get('free_data_only')}`",
            f"- true PIT breadth built：`{scope.get('true_pit_breadth_built')}`",
            f"- promotion status：`{scope.get('promotion_status')}`",
            "",
            "本批只建立 free data ingestion、PIT contract、coverage audit 和 "
            "reopen readiness；不恢复策略研究。",
            "",
        ]
    )


def _render_registry(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Free Data Source Registry Review",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- 来源数量：{_mapping(payload.get('summary')).get('source_count', 0)}",
        f"- promotion_allowed：`{payload.get('promotion_allowed')}`",
        "",
        "## Sources",
        "",
    ]
    for source_id in _strings(payload.get("source_ids")):
        lines.append(f"- `{source_id}`")
    return "\n".join(lines) + "\n"


def _render_fred_summary(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# FRED Series Ingestion Review",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- 行数：{summary.get('row_count', 0)}",
            f"- 已覆盖 series：`{', '.join(_strings(summary.get('available_series')))}`",
            f"- 缺失 series：`{', '.join(_strings(summary.get('missing_series')))}`",
            "- 数据质量状态写入 downstream summary；缺失 revision-sensitive vintage "
            "时不得进入 model-ready。",
            "",
        ]
    )


def _render_vix_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# VIX Free Source Crosscheck Review",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- VIX 行数：{summary.get('vix_row_count', 0)}",
            f"- FRED `VIXCLS` 可用：`{summary.get('fred_vixcls_available')}`",
            f"- Primary source：`{summary.get('primary_source')}`",
            "- 本批不使用 VIX futures term structure、skew、VVIX 或 option surface。",
            "",
        ]
    )


def _render_calendar_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Official Macro Calendar Ingestion Review",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- 事件数：{summary.get('event_count', 0)}",
            f"- PIT warning：{summary.get('pit_warning_count', 0)}",
            f"- diagnostic_only：`{summary.get('diagnostic_only')}`",
            "- 缺少 `source_published_at` 的事件只能作为 diagnostic calendar risk。",
            "",
        ]
    )


def _render_calendar_pit_contract(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Calendar Event PIT Contract",
            "",
            f"- 状态：`{payload.get('status')}`",
            "",
            "必需字段：",
            "",
            *[f"- `{field}`" for field in _strings(payload.get("required_fields"))],
            "",
            "如果无法确认 `source_published_at`，事件必须标记 `PIT_WARNING` 且 `diagnostic_only`。",
            "",
        ]
    )


def _render_feature_review(
    feature_family: str,
    frame: pd.DataFrame,
    summary: Mapping[str, Any],
) -> str:
    status = "READY_WITH_WARNINGS" if len(frame) else "BLOCKED_OR_EMPTY"
    return "\n".join(
        [
            f"# {feature_family} Feature Review",
            "",
            f"- 状态：`{status}`",
            f"- 特征行数：{len(frame)}",
            f"- 数据质量状态：`{summary.get('data_quality_status')}`",
            "- market_regime：`ai_after_chatgpt`；actual requested date range "
            "写入 closeout payload。",
            "- promotion_allowed=`false`；paper_shadow_allowed=`false`；"
            "production_allowed=`false`；broker_action=`none`。",
            "",
        ]
    )


def _render_pit_audit(payload: Mapping[str, Any]) -> str:
    lines = ["# Free Feature PIT Audit", "", f"- 状态：`{payload.get('status')}`", ""]
    lines.extend(
        [
            "| Feature family | PIT status | Allowed usage | Blocked usage |",
            "|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("rows")):
        lines.append(
            "| "
            f"`{row.get('feature_family')}` | `{row.get('PIT_status')}` | "
            f"`{row.get('allowed_usage')}` | `{row.get('blocked_usage')}` |"
        )
    return "\n".join(lines) + "\n"


def _render_coverage_matrix(payload: Mapping[str, Any]) -> str:
    lines = ["# Free Data Feature Coverage Matrix", "", f"- 状态：`{payload.get('status')}`", ""]
    lines.extend(
        [
            "| Family | Earliest | Latest | Missing rate | Allowed | Blocked |",
            "|---|---|---|---:|---|---|",
        ]
    )
    for row in _records(payload.get("rows")):
        lines.append(
            "| "
            f"`{row.get('feature_family')}` | {row.get('earliest_available_date')} | "
            f"{row.get('latest_available_date', '')} | {row.get('missing_rate')} | "
            f"`{row.get('allowed_usage')}` | `{row.get('blocked_usage')}` |"
        )
    return "\n".join(lines) + "\n"


def _render_readiness(payload: Mapping[str, Any]) -> str:
    lines = ["# Free Feature Family Reopen Readiness", "", f"- 状态：`{payload.get('status')}`", ""]
    lines.extend(["| Family | Readiness | Reason |", "|---|---|---|"])
    for row in _records(payload.get("rows")):
        lines.append(
            "| "
            f"`{row.get('feature_family')}` | `{row.get('readiness_status')}` | "
            f"{row.get('reason')} |"
        )
    lines.append("")
    lines.append("所有 readiness 只允许后续 research；promotion 仍 blocked。")
    return "\n".join(lines) + "\n"


def _render_participation_registry(payload: Mapping[str, Any]) -> str:
    lines = ["# Participation Proxy Free Registry Review", "", "- 状态：`DIAGNOSTIC_ONLY`", ""]
    lines.extend(["| Proxy | Ratio | Status | Caveats |", "|---|---|---|---|"])
    for proxy in _records(payload.get("proxies")):
        lines.append(
            "| "
            f"`{proxy.get('proxy_id')}` | `{proxy.get('numerator')}/{proxy.get('denominator')}` | "
            f"`{proxy.get('status')}` | `{', '.join(_strings(proxy.get('caveats')))}` |"
        )
    lines.append("")
    lines.append("这些 proxy 不是 true PIT breadth，不允许进入 promotion evidence。")
    return "\n".join(lines) + "\n"


def _render_owner_brief(readiness: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Free PIT Data Source Owner Brief",
            "",
            "## 结论",
            "",
            "先补免费数据是为了在不购买供应商数据的前提下，把 rates、VIX "
            "和官方 calendar risk 的 PIT contract 做成可审计输入。",
            "",
            "## PIT-approved",
            "",
            "- `rates_liquidity_free_v1`：在 DGS2/DGS10/DTWEXBGS 覆盖满足时可用于 research。",
            "- `volatility_compression_free_v1`：VIX index + QQQ realized vol 可用于 research。",
            "",
            "## Diagnostic-only",
            "",
            "- calendar rows 缺少 `source_published_at` 时只能 diagnostic。",
            "- `participation_proxy_free_v1` 不是 true PIT breadth。",
            "",
            "## 仍需付费或 owner 输入的数据",
            "",
            "- true PIT breadth / historical constituents / survivorship-free universe。",
            "- analyst revision / earnings estimate PIT data。",
            "",
            "## Safety",
            "",
            f"- readiness status：`{readiness.get('status')}`",
            "- promotion、paper-shadow、production、broker 全部继续 disabled。",
            "",
        ]
    )


def _render_closeout(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Free PIT Data Source Ingestion Closeout",
        "",
        f"- 状态：`{payload.get('status')}`",
        "",
    ]
    lines.append("## Final Status")
    lines.append("")
    for status in _strings(payload.get("final_status")):
        lines.append(f"- `{status}`")
    lines.append("")
    lines.append(
        "本 closeout 不恢复 first-layer channel research，"
        "不进入 promotion、paper-shadow、production 或 broker。"
    )
    return "\n".join(lines) + "\n"


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"YAML file must be a mapping: {path}")
    return raw


def _records(value: object) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    if value is None:
        return []
    return [str(value)] if str(value) else []


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _issue(
    severity: str,
    code: str,
    message: str,
    *,
    source_id: str | None = None,
) -> dict[str, str]:
    payload = {"severity": severity, "code": code, "message": message}
    if source_id is not None:
        payload["source_id"] = source_id
    return payload


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def _empty_fred_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "series",
            "value",
            "source_id",
            "provider",
            "known_at",
            "available_at",
            "PIT_status",
            "revision_risk",
        ]
    )


def _empty_vix() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "vix_level",
            "close",
            "source_id",
            "provider",
            "known_at",
            "available_at",
            "PIT_status",
            "revision_risk",
        ]
    )


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
