from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.yaml_loader import safe_load_yaml_path

MARKET_REGIME = "ai_after_chatgpt"
ANCHOR_EVENT = "ChatGPT public launch"
ANCHOR_DATE = "2022-11-30"
DEFAULT_BACKTEST_START = "2022-12-01"
PRIMARY_WINDOW_ID = "exact_three_asset_validated"
PRIMARY_WINDOW_ALIAS = "EXACT_THREE_ASSET_VALIDATED_WINDOW"
PRIMARY_WINDOW_START = "2021-02-22"

DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"

SAFETY_BOUNDARY: dict[str, Any] = {
    "research_only": True,
    "actual_path_required": True,
    "target_path_metrics_role": "diagnostic_only",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "manual_review_required": True,
    "dynamic_promotion_status": "BLOCKED",
}


def base_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    modified_channel: str,
    model_version: str,
    selection_rule_version: str,
    rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "research_window_id": PRIMARY_WINDOW_ID,
        "research_window_alias": PRIMARY_WINDOW_ALIAS,
        "requested_start": PRIMARY_WINDOW_START,
        "actual_start": PRIMARY_WINDOW_START,
        "actual_portfolio_start": PRIMARY_WINDOW_START,
        "end": "latest",
        "window_role": "primary_validated",
        "data_quality_contract": "cached_data_validation_required",
        "exact_or_proxy": "proxy" if "proxy" in report_type else "exact",
        "summary": clean_for_yaml(dict(summary)),
        "research_audit_metadata": audit_metadata(
            modified_channel=modified_channel,
            model_version=model_version,
            selection_rule_version=selection_rule_version,
        ),
        **SAFETY_BOUNDARY,
    }
    if rows is not None:
        payload["rows"] = clean_for_yaml(list(rows))
    return payload


def audit_metadata(
    *,
    modified_channel: str,
    model_version: str,
    selection_rule_version: str,
) -> dict[str, Any]:
    return {
        "modified_layer": "validation_only",
        "modified_channel": modified_channel,
        "frozen_first_layer_version": "first_layer_channel_archive_policy_v1",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "post_2085_family_level_diagnostic_v1",
        "feature_set_version": "post_2085_free_feature_and_proxy_registry_v1",
        "model_version": model_version,
        "threshold_policy": selection_rule_version,
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "candidate_count": 0,
        "pre_registered_selection_rule": selection_rule_version,
        "selection_rule_version": selection_rule_version,
        "boundary_contract_version": "post_2085_research_only_boundary_v1",
    }


def validate_cached_market_data(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    as_of_date: date | None,
    expected_price_tickers: Sequence[str] = ("QQQ",),
    expected_rate_series: Sequence[str] = (),
) -> dict[str, Any]:
    resolved_as_of = as_of_date or max_price_date(prices_path)
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(expected_price_tickers),
        expected_rate_series=list(expected_rate_series),
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        secondary_prices_path=marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None,
        require_secondary_prices=False,
    )
    if not report.passed:
        raise ValueError(f"cached data quality gate failed: {report.status}")
    return data_quality_payload(report, prices_path, rates_path, marketstack_prices_path)


def data_quality_payload(
    report: DataQualityReport,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "price_path": str(prices_path),
        "rates_path": str(rates_path),
        "secondary_prices_path": str(marketstack_prices_path) if marketstack_prices_path else "",
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
    }


def max_price_date(path: Path) -> date:
    if not path.exists():
        return date.today()
    frame = pd.read_csv(path, usecols=["date"])
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return date.today()
    return pd.Timestamp(dates.max()).date()


def load_adjusted_price_matrix(path: Path, tickers: Sequence[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    missing = {"date", "ticker", "adj_close"} - set(frame.columns)
    if missing:
        raise ValueError(f"price cache missing columns: {sorted(missing)}")
    frame = frame.loc[frame["ticker"].astype(str).isin(set(tickers))].copy()
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    pivot = frame.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last")
    pivot = pivot.sort_index()
    for ticker in tickers:
        if ticker not in pivot.columns:
            pivot[ticker] = pd.NA
    return pivot.reindex(columns=list(tickers)).ffill()


def future_outcomes(price_series: pd.Series, ts: pd.Timestamp) -> dict[str, float | None]:
    if ts not in price_series.index:
        return _empty_outcomes()
    pos = price_series.index.get_loc(ts)
    if not isinstance(pos, int):
        return _empty_outcomes()
    start_price = to_float(price_series.iloc[pos])
    if start_price <= 0.0:
        return _empty_outcomes()
    outcomes: dict[str, float | None] = {}
    for horizon in (1, 5, 10, 20):
        key = f"future_{horizon}d_return"
        if pos + horizon >= len(price_series):
            outcomes[key] = None
        else:
            outcomes[key] = round_float(
                to_float(price_series.iloc[pos + horizon]) / start_price - 1.0
            )
    window = price_series.iloc[pos : min(pos + 21, len(price_series))].dropna()
    if len(window) < 2:
        outcomes["future_max_drawdown"] = None
    else:
        path = window.astype(float) / start_price - 1.0
        outcomes["future_max_drawdown"] = round_float(float(path.min()))
    return outcomes


def json_feature_values(record: Mapping[str, Any], *, excluded: set[str]) -> str:
    values: dict[str, Any] = {}
    for key, value in record.items():
        if key in excluded:
            continue
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        if isinstance(value, pd.Timestamp):
            values[key] = value.date().isoformat()
        elif hasattr(value, "item"):
            try:
                values[key] = value.item()
            except (TypeError, ValueError):
                values[key] = str(value)
        else:
            values[key] = value
    return json.dumps(clean_for_yaml(values), ensure_ascii=False, sort_keys=True, default=str)


def write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(clean_for_yaml(dict(payload)), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(clean_for_yaml(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_csv_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([clean_for_yaml(dict(row)) for row in rows]).to_csv(path, index=False)


def write_matrix_artifacts(
    json_path: Path,
    csv_path: Path,
    common: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    rows_key: str = "rows",
) -> None:
    write_json(json_path, {**dict(common), rows_key: list(rows)})
    write_csv_rows(csv_path, rows)


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def load_mapping(path: Path, *, missing_ok: bool = False) -> dict[str, Any]:
    if missing_ok and not path.exists():
        return {}
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def records(value: object) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def strings(value: object) -> list[str]:
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    if value is None:
        return []
    return [str(value)] if str(value) else []


def to_float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def round_float(value: object, digits: int = 6) -> float:
    return round(to_float(value), digits)


def rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round_float(numerator / denominator)


def clean_for_yaml(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): clean_for_yaml(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean_for_yaml(item) for item in value]
    if isinstance(value, tuple):
        return [clean_for_yaml(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return clean_for_yaml(value.item())
        except (TypeError, ValueError):
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value


def _empty_outcomes() -> dict[str, float | None]:
    return {
        "future_1d_return": None,
        "future_5d_return": None,
        "future_10d_return": None,
        "future_20d_return": None,
        "future_max_drawdown": None,
    }
