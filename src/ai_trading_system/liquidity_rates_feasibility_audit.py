from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2311_LIQUIDITY_RATES_PRESSURE_DATA_FEASIBILITY_AUDIT"
REPORT_TYPE = "liquidity_rates_data_feasibility_audit"
ARTIFACT_ROLE = "liquidity_rates_feasibility_audit"
MODE = "feasibility_audit"
STATUS = "LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY"

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("10d", "20d", "1m")
REQUIRED_PRICE_ANCHORS = ("QQQ", "SMH", "TLT", "SHY")
PRICE_PROXY_SYMBOLS = ("TLT", "IEF", "SHY", "UUP", "HYG", "LQD", "QQQ", "SMH")
MACRO_RATE_SERIES = ("DGS10", "DGS2", "DTWEXBGS", "DFII10", "T10YIE", "SOFR")
FULLY_AVAILABLE_PRICE_INPUTS = {"TLT", "SHY", "QQQ", "SMH"}
MISSING_PRICE_INPUTS = {"IEF", "UUP", "HYG", "LQD"}
AVAILABLE_MACRO_SERIES = {"DGS10", "DGS2", "DTWEXBGS"}

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "generator_implemented": False,
    "candidate_generation_allowed": False,
    "candidate_artifact_generated": False,
    "candidate_signal_series_generated": False,
    "prediction_artifact_generated": False,
    "actual_path_validation_executed": False,
    "scope_review_executed": False,
    "forward_observe_started": False,
}


class LiquidityRatesFeasibilityAuditError(ValueError):
    pass


def run_liquidity_rates_pressure_feasibility_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    horizons: str | Sequence[str] = DEFAULT_HORIZONS,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise LiquidityRatesFeasibilityAuditError(
            "liquidity / rates feasibility audit only supports feasibility_audit mode"
        )
    asset_ids = _parse_list(target_assets, uppercase=True)
    horizon_ids = _parse_list(horizons, uppercase=False)
    resolved_as_of = _resolve_date(
        quality_as_of,
        default=_latest_price_date(prices_path),
    )
    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        quality_as_of=resolved_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise LiquidityRatesFeasibilityAuditError(
            f"TRADING-2311 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    price_frame = _read_price_cache(prices_path)
    rate_frame = _read_rates_cache(rates_path)
    price_rows = build_price_proxy_coverage_matrix(price_frame, prices_path)
    macro_rows = build_macro_rates_coverage_matrix(rate_frame, rates_path)
    inventory_rows = build_rates_proxy_inventory(price_rows, macro_rows)
    design_sketch = build_liquidity_pressure_candidate_design_sketch(
        target_assets=asset_ids,
        horizons=horizon_ids,
        price_rows=price_rows,
        macro_rows=macro_rows,
    )
    validation_route = build_liquidity_rates_validation_route(
        design_sketch=design_sketch
    )
    safety_boundary = build_liquidity_rates_safety_boundary(
        quality_report=quality_report,
        quality_report_path=quality_report_path,
    )
    summary = _summary_payload(
        generated_at=generated_at,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        target_assets=asset_ids,
        horizons=horizon_ids,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        price_rows=price_rows,
        macro_rows=macro_rows,
        inventory_rows=inventory_rows,
        design_sketch=design_sketch,
        validation_route=validation_route,
    )
    common = _common_payload(
        generated_at=generated_at,
        target_assets=asset_ids,
        horizons=horizon_ids,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
    )
    paths = write_liquidity_rates_feasibility_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        common=common,
        summary=summary,
        inventory_rows=inventory_rows,
        price_rows=price_rows,
        macro_rows=macro_rows,
        design_sketch=design_sketch,
        validation_route=validation_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml(
        {
            **common,
            "summary": summary,
            "artifact_paths": paths,
            "rates_proxy_inventory": inventory_rows,
            "price_proxy_coverage_rows": price_rows,
            "macro_rates_coverage_rows": macro_rows,
            "candidate_design_sketch": design_sketch,
            "validation_route": validation_route,
        }
    )


def build_price_proxy_coverage_matrix(
    price_frame: pd.DataFrame,
    prices_path: Path,
) -> list[dict[str, Any]]:
    rows = []
    for symbol in PRICE_PROXY_SYMBOLS:
        subset = price_frame.loc[price_frame["ticker"].astype(str) == symbol].copy()
        available = not subset.empty
        source_status = (
            "AVAILABLE_AFTER_DATA_QUALITY_GATE"
            if available
            else "SOURCE_GAP_MISSING_LOCAL_PRICE_CACHE"
        )
        usage_role = _price_usage_role(symbol)
        rows.append(
            clean_for_yaml(
                {
                    "input_id": f"{symbol.lower()}_price_proxy",
                    "symbol": symbol,
                    "input_category": "price_proxy",
                    "usage_role": usage_role,
                    "provider_name": "local_price_cache",
                    "provider_class": "cached_price_vendor_or_public_price_source",
                    "endpoint": "recorded_in_download_manifest_if_available",
                    "request_parameters": "historical_daily_adjusted_prices",
                    "local_cache_path": str(prices_path),
                    "download_timestamp": "see data/raw/download_manifest.csv",
                    "row_count": int(len(subset)),
                    "checksum": _frame_checksum(subset),
                    "history_start": _min_text(subset, "date"),
                    "history_end": _max_text(subset, "date"),
                    "source_status": source_status,
                    "pit_status": "PRICE_PROXY_PIT_APPROXIMATION_READY_AFTER_DQ"
                    if available
                    else "BLOCKED_MISSING_LOCAL_CACHE",
                    "known_at_status": "daily_close_known_after_market_close"
                    if available
                    else "missing_source",
                    "recommended_usage": _price_recommended_usage(symbol, available),
                    "blocking_issue": _price_blocker(symbol, available),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_macro_rates_coverage_matrix(
    rate_frame: pd.DataFrame,
    rates_path: Path,
) -> list[dict[str, Any]]:
    rows = []
    for series in MACRO_RATE_SERIES:
        subset = rate_frame.loc[rate_frame["series"].astype(str) == series].copy()
        available = not subset.empty
        rows.append(
            clean_for_yaml(
                {
                    "input_id": f"{series.lower()}_macro_rate",
                    "series": series,
                    "input_category": "macro_rates_or_liquidity_proxy",
                    "usage_role": _macro_usage_role(series),
                    "provider_name": "FRED_or_local_rates_cache",
                    "provider_class": "public_macro_convenience_source",
                    "endpoint": "FRED series endpoint recorded in source connector",
                    "request_parameters": f"series={series}",
                    "local_cache_path": str(rates_path),
                    "download_timestamp": "see data/raw/download_manifest.csv",
                    "row_count": int(len(subset)),
                    "checksum": _frame_checksum(subset),
                    "history_start": _min_text(subset, "date"),
                    "history_end": _max_text(subset, "date"),
                    "source_status": "AVAILABLE_AFTER_DATA_QUALITY_GATE"
                    if available
                    else "SOURCE_GAP_MISSING_LOCAL_RATE_CACHE",
                    "pit_status": "OBSERVATION_DATE_PIT_APPROX_KNOWN_AT_REVIEW_REQUIRED"
                    if available
                    else "BLOCKED_MISSING_LOCAL_CACHE",
                    "known_at_status": "release_timestamp_not_cached_observation_date_only"
                    if available
                    else "missing_source",
                    "revision_risk": "macro_series_revision_or_late_release_possible"
                    if available
                    else "not_evaluable_until_source_exists",
                    "recommended_usage": _macro_recommended_usage(series, available),
                    "blocking_issue": _macro_blocker(series, available),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_rates_proxy_inventory(
    price_rows: Sequence[Mapping[str, Any]],
    macro_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_symbol = {str(row["symbol"]): row for row in price_rows}
    by_series = {str(row["series"]): row for row in macro_rows}
    specs = [
        (
            "duration_pressure_proxy",
            "Duration pressure proxy",
            ["TLT", "SHY", "DGS10", "DGS2"],
            "PARTIAL_PROXY_READY_AFTER_DQ",
            "duration_pressure_proxy_v1",
            "rates_pressure_and_duration_asset_pressure",
        ),
        (
            "intermediate_treasury_proxy",
            "IEF intermediate Treasury proxy",
            ["IEF"],
            "BLOCKED_MISSING_PRICE_PROXY",
            "duration_pressure_proxy_v1",
            "intermediate_duration_cross_check",
        ),
        (
            "usd_liquidity_proxy",
            "UUP / DXY USD liquidity proxy",
            ["UUP", "DTWEXBGS"],
            "PARTIAL_MACRO_PROXY_ONLY_PRICE_PROXY_MISSING",
            "liquidity_headwind_proxy_v1",
            "usd_liquidity_headwind",
        ),
        (
            "credit_liquidity_proxy",
            "HYG vs LQD credit liquidity proxy",
            ["HYG", "LQD"],
            "BLOCKED_MISSING_CREDIT_ETF_PROXIES",
            "liquidity_headwind_proxy_v1",
            "credit_spread_risk_appetite_pressure",
        ),
        (
            "real_rate_proxy",
            "Real rate proxy",
            ["DFII10", "T10YIE"],
            "BLOCKED_MISSING_REAL_RATE_SERIES",
            "rates_pressure_exposure_cap_modifier_v1",
            "real_rate_valuation_pressure",
        ),
        (
            "qqq_smh_valuation_pressure_context",
            "QQQ / SMH valuation pressure context",
            ["QQQ", "SMH", "TLT", "DGS10", "DGS2"],
            "PARTIAL_PROXY_READY_AFTER_DQ",
            "rates_pressure_exposure_cap_modifier_v1",
            "qqq_smh_rates_pressure_context",
        ),
    ]
    rows = []
    for input_id, title, dependencies, expected_status, candidate_id, usage_role in specs:
        available_dependencies = [
            item for item in dependencies if _dependency_available(item, by_symbol, by_series)
        ]
        missing_dependencies = [
            item for item in dependencies if item not in available_dependencies
        ]
        rows.append(
            clean_for_yaml(
                {
                    "input_id": input_id,
                    "title": title,
                    "candidate_id": candidate_id,
                    "usage_role": usage_role,
                    "dependencies": list(dependencies),
                    "available_dependencies": available_dependencies,
                    "missing_dependencies": missing_dependencies,
                    "source_status": expected_status
                    if missing_dependencies
                    else "AVAILABLE_AFTER_DATA_QUALITY_GATE",
                    "generator_ready": False,
                    "generator_blocker": _inventory_blocker(missing_dependencies),
                    "recommended_usage": _inventory_usage(input_id, missing_dependencies),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_liquidity_pressure_candidate_design_sketch(
    *,
    target_assets: Sequence[str],
    horizons: Sequence[str],
    price_rows: Sequence[Mapping[str, Any]],
    macro_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    available_prices = [
        str(row["symbol"])
        for row in price_rows
        if row.get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    missing_prices = [
        str(row["symbol"])
        for row in price_rows
        if row.get("source_status") != "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    available_macro = [
        str(row["series"])
        for row in macro_rows
        if row.get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    missing_macro = [
        str(row["series"])
        for row in macro_rows
        if row.get("source_status") != "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.candidate_design_sketch.v1",
            "task_id": TASK_ID,
            "candidate_family": "liquidity_rates_pressure",
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "candidate_ids": [
                "duration_pressure_proxy_v1",
                "liquidity_headwind_proxy_v1",
                "rates_pressure_exposure_cap_modifier_v1",
            ],
            "available_price_proxies": available_prices,
            "missing_price_proxies": missing_prices,
            "available_macro_series": available_macro,
            "missing_macro_series": missing_macro,
            "partial_poc_possible": True,
            "full_liquidity_pressure_poc_ready": False,
            "recommended_partial_scope": [
                "duration_pressure_proxy_v1_using_TLT_SHY_DGS10_DGS2",
                "rates_pressure_exposure_cap_modifier_v1_using_TLT_DGS10_DGS2",
            ],
            "blocked_full_scope": [
                "liquidity_headwind_proxy_v1_requires_UUP_or_DXY_and_HYG_LQD",
                "real_rate_proxy_requires_DFII10_or_equivalent_real_rate_source",
            ],
            "horizon_rationale": (
                "Liquidity / rates pressure is slower-moving than one-day price "
                "noise; 10d, 20d and 1m are retained as research horizons."
            ),
            "overlap_with_volatility_risk_cap": (
                "Expected to act as exposure-cap modifier or risk-cap enhancer, "
                "not a standalone directional signal."
            ),
            "recommended_next_task": (
                "TRADING-2312_LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_PARTIAL_RATES_ONLY"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_liquidity_rates_validation_route(
    *,
    design_sketch: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        _route_row(
            candidate_id="duration_pressure_proxy_v1",
            readiness_status="PARTIAL_GENERATOR_POC_READY_AFTER_DQ",
            data_mode="TLT_SHY_DGS10_DGS2_PRICE_AND_FRED_PROXY",
            required_inputs=["TLT", "SHY", "DGS10", "DGS2"],
            missing_inputs=[],
            allowed_next_step="TRADING-2312_PARTIAL_RATES_GENERATOR_POC",
            blocked_validation="actual_path_validation; scope_review; promotion",
        ),
        _route_row(
            candidate_id="liquidity_headwind_proxy_v1",
            readiness_status="SOURCE_GAP_BLOCKED",
            data_mode="USD_AND_CREDIT_LIQUIDITY_PROXY",
            required_inputs=["UUP_or_DXY", "HYG", "LQD", "DTWEXBGS"],
            missing_inputs=["UUP_or_DXY", "HYG", "LQD"],
            allowed_next_step="source_gap_resolution_before_full_generator",
            blocked_validation="generator_poc_full_scope; actual_path_validation; promotion",
        ),
        _route_row(
            candidate_id="rates_pressure_exposure_cap_modifier_v1",
            readiness_status="PARTIAL_GENERATOR_POC_READY_AFTER_DQ",
            data_mode="TLT_DGS10_DGS2_WITH_REAL_RATE_GAP_DISCLOSURE",
            required_inputs=["TLT", "DGS10", "DGS2", "DFII10_or_real_rate_proxy"],
            missing_inputs=["DFII10_or_real_rate_proxy"],
            allowed_next_step="TRADING-2312_PARTIAL_RATES_GENERATOR_POC",
            blocked_validation="actual_path_validation; scope_review; promotion",
        ),
    ]


def build_liquidity_rates_safety_boundary(
    *,
    quality_report: DataQualityReport,
    quality_report_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "status": STATUS,
        "data_quality_status": quality_report.status,
        "data_quality_report_path": str(quality_report_path),
        "does_not_generate_candidate_artifacts": True,
        "does_not_run_actual_path_validation": True,
        "does_not_run_scope_review": True,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        **SAFETY_FIELDS,
    }


def write_liquidity_rates_feasibility_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    common: Mapping[str, Any],
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    price_rows: Sequence[Mapping[str, Any]],
    macro_rows: Sequence[Mapping[str, Any]],
    design_sketch: Mapping[str, Any],
    validation_route: Sequence[Mapping[str, Any]],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "liquidity_rates_data_feasibility_summary.json",
        "rates_proxy_inventory_json": output_dir / "rates_proxy_inventory.json",
        "rates_proxy_inventory_csv": output_dir / "rates_proxy_inventory.csv",
        "price_proxy_coverage_json": output_dir
        / "liquidity_rates_price_proxy_coverage_matrix.json",
        "price_proxy_coverage_csv": output_dir
        / "liquidity_rates_price_proxy_coverage_matrix.csv",
        "macro_rates_coverage_json": output_dir / "macro_rates_coverage_matrix.json",
        "macro_rates_coverage_csv": output_dir / "macro_rates_coverage_matrix.csv",
        "candidate_design_sketch": output_dir
        / "liquidity_pressure_candidate_design_sketch.json",
        "validation_route": output_dir / "liquidity_rates_validation_route.json",
        "safety_boundary": output_dir / "liquidity_rates_safety_boundary.json",
        "audit_doc": docs_root / "liquidity_rates_data_feasibility_audit.md",
    }
    write_json(paths["summary"], {**dict(common), "summary": summary})
    write_json(paths["rates_proxy_inventory_json"], {**dict(common), "rows": inventory_rows})
    write_csv_rows(paths["rates_proxy_inventory_csv"], inventory_rows)
    write_json(paths["price_proxy_coverage_json"], {**dict(common), "rows": price_rows})
    write_csv_rows(paths["price_proxy_coverage_csv"], price_rows)
    write_json(paths["macro_rates_coverage_json"], {**dict(common), "rows": macro_rows})
    write_csv_rows(paths["macro_rates_coverage_csv"], macro_rows)
    write_json(paths["candidate_design_sketch"], {**dict(common), **dict(design_sketch)})
    write_json(paths["validation_route"], {**dict(common), "rows": validation_route})
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(
        paths["audit_doc"],
        _render_report(summary, inventory_rows, price_rows, macro_rows, validation_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _summary_payload(
    *,
    generated_at: datetime,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    target_assets: Sequence[str],
    horizons: Sequence[str],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    price_rows: Sequence[Mapping[str, Any]],
    macro_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
    design_sketch: Mapping[str, Any],
    validation_route: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    available_price_count = sum(
        1 for row in price_rows if row.get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    )
    missing_price_symbols = [
        row["symbol"]
        for row in price_rows
        if row.get("source_status") != "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    available_macro_count = sum(
        1 for row in macro_rows if row.get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    )
    missing_macro_series = [
        row["series"]
        for row in macro_rows
        if row.get("source_status") != "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    ]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "report_type": REPORT_TYPE,
            "title": "Liquidity / Rates Pressure Data Feasibility Audit",
            "task_id": TASK_ID,
            "status": STATUS,
            "artifact_role": ARTIFACT_ROLE,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": "2022-12-01..2026-06-29",
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "marketstack_prices_path": str(marketstack_prices_path or ""),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "data_quality": _data_quality_payload(quality_report, quality_report_path),
            "data_quality_status": quality_report.status,
            "data_quality_report_path": str(quality_report_path),
            "required_price_anchors": list(REQUIRED_PRICE_ANCHORS),
            "available_price_proxy_count": available_price_count,
            "missing_price_proxy_symbols": missing_price_symbols,
            "available_macro_series_count": available_macro_count,
            "missing_macro_series": missing_macro_series,
            "inventory_row_count": len(inventory_rows),
            "candidate_route_count": len(validation_route),
            "partial_poc_possible": design_sketch["partial_poc_possible"],
            "full_liquidity_pressure_poc_ready": design_sketch[
                "full_liquidity_pressure_poc_ready"
            ],
            "recommended_next_task": design_sketch["recommended_next_task"],
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    generated_at: datetime,
    target_assets: Sequence[str],
    horizons: Sequence[str],
    quality_report: DataQualityReport,
    quality_report_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "title": "Liquidity / Rates Pressure Data Feasibility Audit",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "2022-12-01..2026-06-29",
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "data_quality_status": quality_report.status,
        "data_quality_report_path": str(quality_report_path),
        **SAFETY_FIELDS,
    }


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    universe = load_universe()
    secondary_path = (
        marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(REQUIRED_PRICE_ANCHORS),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=quality_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, quality_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def _data_quality_payload(report: DataQualityReport, report_path: Path) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "report_path": str(report_path),
    }


def _render_report(
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    price_rows: Sequence[Mapping[str, Any]],
    macro_rows: Sequence[Mapping[str, Any]],
    validation_route: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Liquidity / Rates Pressure Data Feasibility Audit",
            "",
            "TRADING-2311 只审计 liquidity / rates pressure 输入可行性，不生成 "
            "candidate-bound artifacts。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- available_price_proxy_count: `{summary['available_price_proxy_count']}`",
            "- missing_price_proxy_symbols: `{}`".format(
                ",".join(summary["missing_price_proxy_symbols"])
            ),
            f"- available_macro_series_count: `{summary['available_macro_series_count']}`",
            "- missing_macro_series: `{}`".format(
                ",".join(summary["missing_macro_series"])
            ),
            f"- partial_poc_possible: `{summary['partial_poc_possible']}`",
            (
                "- full_liquidity_pressure_poc_ready: "
                f"`{summary['full_liquidity_pressure_poc_ready']}`"
            ),
            f"- recommended_next_task: `{summary['recommended_next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "- dynamic_promotion_status: `BLOCKED`",
            "",
            "## Rates Proxy Inventory",
            "",
            "|input_id|source_status|available_dependencies|missing_dependencies|candidate_id|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['input_id']}`|`{row['source_status']}`|"
                    f"`{','.join(row['available_dependencies'])}`|"
                    f"`{','.join(row['missing_dependencies'])}`|"
                    f"`{row['candidate_id']}`|"
                )
                for row in inventory_rows
            ],
            "",
            "## Price Proxy Coverage",
            "",
            "|symbol|row_count|history_start|history_end|source_status|",
            "|---|---:|---|---|---|",
            *[
                (
                    f"|`{row['symbol']}`|{row['row_count']}|"
                    f"{row['history_start']}|{row['history_end']}|"
                    f"`{row['source_status']}`|"
                )
                for row in price_rows
            ],
            "",
            "## Macro Rates Coverage",
            "",
            "|series|row_count|history_start|history_end|source_status|known_at_status|",
            "|---|---:|---|---|---|---|",
            *[
                (
                    f"|`{row['series']}`|{row['row_count']}|"
                    f"{row['history_start']}|{row['history_end']}|"
                    f"`{row['source_status']}`|`{row['known_at_status']}`|"
                )
                for row in macro_rows
            ],
            "",
            "## Validation Route",
            "",
            "|candidate_id|readiness_status|allowed_next_step|blocked_validation|",
            "|---|---|---|---|",
            *[
                (
                    f"|`{row['candidate_id']}`|`{row['readiness_status']}`|"
                    f"`{row['allowed_next_step']}`|{row['blocked_validation']}|"
                )
                for row in validation_route
            ],
            "",
            "## Safety",
            "",
            "本报告只记录输入可行性和 source gap。缺失的 IEF / UUP / HYG / LQD / "
            "real-rate series 不得被平滑成可用输入；当前不允许 generator、actual-path "
            "validation、scope review、promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )


def _route_row(
    *,
    candidate_id: str,
    readiness_status: str,
    data_mode: str,
    required_inputs: Sequence[str],
    missing_inputs: Sequence[str],
    allowed_next_step: str,
    blocked_validation: str,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "candidate_family": "liquidity_rates_pressure",
        "readiness_status": readiness_status,
        "data_mode": data_mode,
        "required_inputs": list(required_inputs),
        "missing_inputs": list(missing_inputs),
        "allowed_next_step": allowed_next_step,
        "blocked_validation": blocked_validation,
        **SAFETY_FIELDS,
    }


def _read_price_cache(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"date", "ticker", "adj_close"}
    missing = required - set(frame.columns)
    if missing:
        raise LiquidityRatesFeasibilityAuditError(
            f"price cache missing columns: {sorted(missing)}"
        )
    return frame


def _read_rates_cache(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"date", "series", "value"}
    missing = required - set(frame.columns)
    if missing:
        raise LiquidityRatesFeasibilityAuditError(
            f"rates cache missing columns: {sorted(missing)}"
        )
    return frame


def _latest_price_date(prices_path: Path) -> date:
    price_dates = pd.read_csv(prices_path, usecols=["date"])["date"]
    latest = pd.to_datetime(price_dates).max()
    return pd.Timestamp(latest).date()


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _resolve_date(value: str | date | None, *, default: date) -> date:
    if value is None or value == "":
        return default
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise LiquidityRatesFeasibilityAuditError(
            f"date must use YYYY-MM-DD: {value}"
        ) from exc


def _parse_list(value: str | Sequence[str], *, uppercase: bool) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    cleaned = [part.strip() for part in parts if part.strip()]
    if not cleaned:
        raise LiquidityRatesFeasibilityAuditError("list option must be non-empty")
    if uppercase:
        return tuple(part.upper() for part in cleaned)
    return tuple(cleaned)


def _frame_checksum(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    normalized = frame.sort_index(axis=1).sort_values(list(frame.columns)).to_csv(
        index=False
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _min_text(frame: pd.DataFrame, column: str) -> str:
    if frame.empty:
        return ""
    return str(frame[column].min())


def _max_text(frame: pd.DataFrame, column: str) -> str:
    if frame.empty:
        return ""
    return str(frame[column].max())


def _dependency_available(
    dependency: str,
    by_symbol: Mapping[str, Mapping[str, Any]],
    by_series: Mapping[str, Mapping[str, Any]],
) -> bool:
    if dependency in by_symbol:
        return by_symbol[dependency].get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    if dependency in by_series:
        return by_series[dependency].get("source_status") == "AVAILABLE_AFTER_DATA_QUALITY_GATE"
    return False


def _inventory_blocker(missing_dependencies: Sequence[str]) -> str:
    if not missing_dependencies:
        return "policy_manifest_required_before_generator_poc"
    return "missing_source_inputs: " + ",".join(missing_dependencies)


def _inventory_usage(input_id: str, missing_dependencies: Sequence[str]) -> str:
    if missing_dependencies:
        return "source_gap_record_only_until_missing_inputs_are_ingested"
    if input_id in {"duration_pressure_proxy", "qqq_smh_valuation_pressure_context"}:
        return "partial_generator_poc_candidate_after_policy_manifest"
    return "diagnostic_context_only"


def _price_usage_role(symbol: str) -> str:
    return {
        "TLT": "long_duration_pressure_price_proxy",
        "IEF": "intermediate_duration_cross_check",
        "SHY": "short_duration_anchor",
        "UUP": "usd_liquidity_headwind_price_proxy",
        "HYG": "high_yield_credit_risk_proxy",
        "LQD": "investment_grade_credit_anchor",
        "QQQ": "technology_duration_sensitive_target",
        "SMH": "semiconductor_duration_sensitive_target",
    }[symbol]


def _price_recommended_usage(symbol: str, available: bool) -> str:
    if not available:
        return "missing_source_gap_before_full_generator_poc"
    if symbol in {"TLT", "SHY"}:
        return "partial_duration_pressure_proxy_after_data_quality_gate"
    if symbol in {"QQQ", "SMH"}:
        return "target_asset_context_after_data_quality_gate"
    return "available_proxy_after_data_quality_gate"


def _price_blocker(symbol: str, available: bool) -> str:
    if available:
        return ""
    return f"{symbol}_missing_from_local_price_cache"


def _macro_usage_role(series: str) -> str:
    return {
        "DGS10": "ten_year_yield_pressure_proxy",
        "DGS2": "two_year_yield_policy_pressure_proxy",
        "DTWEXBGS": "broad_usd_liquidity_pressure_proxy",
        "DFII10": "ten_year_real_rate_proxy",
        "T10YIE": "breakeven_inflation_real_rate_context",
        "SOFR": "front_end_liquidity_funding_proxy",
    }[series]


def _macro_recommended_usage(series: str, available: bool) -> str:
    if not available:
        return "missing_macro_source_gap_before_full_generator_poc"
    if series in {"DGS10", "DGS2"}:
        return "partial_rates_pressure_proxy_with_known_at_disclosure"
    if series == "DTWEXBGS":
        return "usd_liquidity_macro_context_with_known_at_disclosure"
    return "macro_context_after_known_at_policy"


def _macro_blocker(series: str, available: bool) -> str:
    if available:
        return "release_timestamp_policy_required_before_generator"
    return f"{series}_missing_from_local_rates_cache"


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "LiquidityRatesFeasibilityAuditError",
    "build_liquidity_pressure_candidate_design_sketch",
    "build_macro_rates_coverage_matrix",
    "build_price_proxy_coverage_matrix",
    "build_rates_proxy_inventory",
    "run_liquidity_rates_pressure_feasibility_audit",
]
