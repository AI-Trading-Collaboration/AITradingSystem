from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import pandas as pd

from ai_trading_system.config import FundamentalFeaturesConfig, SecCompaniesConfig
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    PeriodType,
    SecFundamentalMetricsCsvValidationReport,
)
from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SEC_PIT_CURRENT_HISTORY_GRADE,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

SEC_PIT_INTERVAL_COLUMNS = (
    "ticker",
    "metric_id",
    "period_type",
    "period_end",
    "value",
    "unit",
    "source_accession_number",
    "available_from_signal_date",
    "available_until_signal_date",
    "available_time_utc",
    "superseded_by_accession_number",
    "pit_data_grade",
    "confidence_level",
)

SEC_PIT_DAILY_PANEL_COLUMNS = (
    "decision_date",
    "ticker",
    "metric_id",
    "period_type",
    "period_end",
    "value",
    "unit",
    "source_accession_number",
    "available_time_utc",
    "pit_data_grade",
    "confidence_level",
)

SEC_PIT_FEATURE_PANEL_COLUMNS = (
    "decision_date",
    "ticker",
    "feature_id",
    "feature_value",
    "feature_unit",
    "input_metric_ids",
    "input_accession_numbers",
    "input_available_times_utc",
    "max_input_available_time_utc",
    "pit_data_grade",
    "confidence_level",
    "confidence_reason",
    "period_type",
    "period_end",
    "input_metric_units",
)


@dataclass(frozen=True)
class SecPitFeaturePanelLoadResult:
    decision_date: date
    tickers: tuple[str, ...]
    frame: pd.DataFrame


def build_fundamental_pit_intervals(mapped_metrics: pd.DataFrame) -> pd.DataFrame:
    if mapped_metrics.empty:
        return pd.DataFrame(columns=list(SEC_PIT_INTERVAL_COLUMNS))
    records: list[dict[str, object]] = []
    key_columns = ["ticker", "metric_id", "period_type", "period_end"]
    for _key, group in mapped_metrics.groupby(key_columns, sort=False):
        ordered = group.sort_values(["available_for_signal_date", "available_time_utc"])
        rows = list(ordered.to_dict(orient="records"))
        for index, row in enumerate(rows):
            start = _date_or_none(row.get("available_for_signal_date"))
            if start is None:
                continue
            next_start = (
                _date_or_none(rows[index + 1].get("available_for_signal_date"))
                if index + 1 < len(rows)
                else None
            )
            until = next_start - timedelta(days=1) if next_start is not None else None
            records.append(
                {
                    "ticker": row.get("ticker", ""),
                    "metric_id": row.get("metric_id", ""),
                    "period_type": row.get("period_type", ""),
                    "period_end": row.get("period_end", ""),
                    "value": row.get("value", ""),
                    "unit": row.get("unit", ""),
                    "source_accession_number": row.get("source_accession_number", ""),
                    "available_from_signal_date": start.isoformat(),
                    "available_until_signal_date": until.isoformat() if until else "",
                    "available_time_utc": row.get("available_time_utc", ""),
                    "superseded_by_accession_number": (
                        rows[index + 1].get("source_accession_number", "")
                        if index + 1 < len(rows)
                        else ""
                    ),
                    "pit_data_grade": row.get("pit_data_grade", ""),
                    "confidence_level": row.get("confidence_level", ""),
                }
            )
    frame = pd.DataFrame(records, columns=list(SEC_PIT_INTERVAL_COLUMNS))
    if not frame.empty:
        frame = frame.sort_values(
            ["ticker", "metric_id", "period_type", "period_end", "available_from_signal_date"]
        ).reset_index(drop=True)
    return frame


def build_fundamental_pit_intervals_csv(
    *,
    mapped_metrics_path: Path,
    output_path: Path,
) -> Path:
    mapped = (
        pd.read_csv(mapped_metrics_path, dtype=str).fillna("")
        if mapped_metrics_path.exists()
        else pd.DataFrame()
    )
    frame = build_fundamental_pit_intervals(mapped)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def build_fundamental_pit_daily_panel(
    *,
    intervals: pd.DataFrame,
    start: date,
    end: date,
) -> pd.DataFrame:
    if start > end:
        raise ValueError("start must be on or before end")
    if intervals.empty:
        return pd.DataFrame(columns=list(SEC_PIT_DAILY_PANEL_COLUMNS))
    records: list[dict[str, object]] = []
    for decision_date in _trading_days(start, end):
        active = _active_intervals(intervals, decision_date)
        for row in active.to_dict(orient="records"):
            records.append(
                {
                    "decision_date": decision_date.isoformat(),
                    "ticker": row.get("ticker", ""),
                    "metric_id": row.get("metric_id", ""),
                    "period_type": row.get("period_type", ""),
                    "period_end": row.get("period_end", ""),
                    "value": row.get("value", ""),
                    "unit": row.get("unit", ""),
                    "source_accession_number": row.get("source_accession_number", ""),
                    "available_time_utc": row.get("available_time_utc", ""),
                    "pit_data_grade": row.get("pit_data_grade", ""),
                    "confidence_level": row.get("confidence_level", ""),
                }
            )
    return pd.DataFrame(records, columns=list(SEC_PIT_DAILY_PANEL_COLUMNS))


def build_fundamental_pit_daily_panel_csv(
    *,
    intervals_path: Path,
    start: date,
    end: date,
    output_path: Path,
) -> Path:
    intervals = (
        pd.read_csv(intervals_path, dtype=str).fillna("")
        if intervals_path.exists()
        else pd.DataFrame()
    )
    frame = build_fundamental_pit_daily_panel(intervals=intervals, start=start, end=end)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def build_sec_pit_feature_panel(
    *,
    intervals: pd.DataFrame,
    features: FundamentalFeaturesConfig,
    sec_companies: SecCompaniesConfig,
    start: date,
    end: date,
) -> pd.DataFrame:
    if start > end:
        raise ValueError("start must be on or before end")
    if intervals.empty:
        return pd.DataFrame(columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS))
    active_tickers = [company.ticker for company in sec_companies.companies if company.active]
    records: list[dict[str, object]] = []
    for decision_date in _trading_days(start, end):
        active = _active_intervals(intervals, decision_date)
        for ticker in active_tickers:
            ticker_frame = active.loc[active["ticker"].astype(str).str.upper() == ticker]
            if ticker_frame.empty:
                continue
            for feature in features.features:
                for period_type in feature.preferred_periods:
                    row = _feature_row_for(
                        ticker_frame=ticker_frame,
                        decision_date=decision_date,
                        feature_id=feature.feature_id,
                        numerator_metric_id=feature.numerator_metric_id,
                        denominator_metric_id=feature.denominator_metric_id,
                        period_type=period_type,
                    )
                    if row is not None:
                        records.append(row)
    frame = pd.DataFrame(records, columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS))
    if not frame.empty:
        frame = frame.sort_values(
            ["decision_date", "ticker", "feature_id", "period_type"]
        ).reset_index(drop=True)
    return frame


def build_sec_pit_feature_panel_csv(
    *,
    intervals_path: Path,
    features: FundamentalFeaturesConfig,
    sec_companies: SecCompaniesConfig,
    start: date,
    end: date,
    output_path: Path,
) -> Path:
    intervals = (
        pd.read_csv(intervals_path, dtype=str).fillna("")
        if intervals_path.exists()
        else pd.DataFrame()
    )
    frame = build_sec_pit_feature_panel(
        intervals=intervals,
        features=features,
        sec_companies=sec_companies,
        start=start,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def load_sec_pit_feature_panel(
    path: Path,
    decision_date: date,
    tickers: Iterable[str],
) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS))
    frame = pd.read_csv(path, dtype=str).fillna("")
    missing = sorted(set(SEC_PIT_FEATURE_PANEL_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"SEC PIT feature panel missing columns: {', '.join(missing)}")
    requested = {ticker.upper() for ticker in tickers}
    selected = frame.loc[
        (frame["decision_date"].astype(str) == decision_date.isoformat())
        & (frame["ticker"].astype(str).str.upper().isin(requested))
    ].copy()
    future_rows = selected.loc[
        pd.to_datetime(
            selected["max_input_available_time_utc"], errors="coerce", utc=True
        ).dt.date.fillna(date.min)
        > decision_date
    ]
    if not future_rows.empty:
        raise ValueError(
            "SEC PIT feature panel contains future available_time for "
            f"decision_date={decision_date.isoformat()}"
        )
    return selected.reset_index(drop=True)


def sec_pit_feature_panel_to_feature_reports(
    path: Path,
    decision_dates: Iterable[date],
    tickers: Iterable[str],
) -> dict[date, SecFundamentalFeaturesReport]:
    reports: dict[date, SecFundamentalFeaturesReport] = {}
    for decision_date in decision_dates:
        frame = load_sec_pit_feature_panel(path, decision_date, tickers)
        rows = tuple(
            _panel_record_to_feature_row(record, path) for record in frame.to_dict("records")
        )
        reports[decision_date] = SecFundamentalFeaturesReport(
            as_of=decision_date,
            input_path=path,
            validation_report=SecFundamentalMetricsCsvValidationReport(
                as_of=decision_date,
                input_path=path,
                row_count=len(frame),
                as_of_row_count=len(frame),
                expected_observation_count=len(frame),
                observed_observation_count=len(frame),
            ),
            rows=rows,
        )
    return reports


def _feature_row_for(
    *,
    ticker_frame: pd.DataFrame,
    decision_date: date,
    feature_id: str,
    numerator_metric_id: str,
    denominator_metric_id: str,
    period_type: str,
) -> dict[str, object] | None:
    numerator_rows = _metric_rows(ticker_frame, numerator_metric_id, period_type)
    denominator_rows = _metric_rows(ticker_frame, denominator_metric_id, period_type)
    if numerator_rows.empty or denominator_rows.empty:
        return None
    common_periods = sorted(
        set(numerator_rows["period_end"].astype(str))
        & set(denominator_rows["period_end"].astype(str))
    )
    if not common_periods:
        return None
    period_end = common_periods[-1]
    numerator = numerator_rows.loc[numerator_rows["period_end"].astype(str) == period_end].iloc[-1]
    denominator = denominator_rows.loc[
        denominator_rows["period_end"].astype(str) == period_end
    ].iloc[-1]
    if str(numerator.get("unit") or "") != str(denominator.get("unit") or ""):
        return None
    denominator_value = _float_or_none(denominator.get("value"))
    numerator_value = _float_or_none(numerator.get("value"))
    if numerator_value is None or denominator_value is None or denominator_value <= 0:
        return None
    input_available_times = (
        str(numerator.get("available_time_utc") or ""),
        str(denominator.get("available_time_utc") or ""),
    )
    grades = {
        str(numerator.get("pit_data_grade") or ""),
        str(denominator.get("pit_data_grade") or ""),
    }
    confidence = _combined_confidence(
        str(numerator.get("confidence_level") or ""),
        str(denominator.get("confidence_level") or ""),
    )
    return {
        "decision_date": decision_date.isoformat(),
        "ticker": str(numerator.get("ticker") or ""),
        "feature_id": feature_id,
        "feature_value": numerator_value / denominator_value,
        "feature_unit": "ratio",
        "input_metric_ids": f"{numerator_metric_id},{denominator_metric_id}",
        "input_accession_numbers": (
            f"{numerator.get('source_accession_number')},"
            f"{denominator.get('source_accession_number')}"
        ),
        "input_available_times_utc": ",".join(input_available_times),
        "max_input_available_time_utc": max(input_available_times),
        "pit_data_grade": (
            SEC_PIT_CURRENT_HISTORY_GRADE
            if SEC_PIT_CURRENT_HISTORY_GRADE in grades
            else SEC_PIT_BACKTEST_DATA_GRADE
        ),
        "confidence_level": confidence,
        "confidence_reason": (
            "all required ratio inputs were available by decision_date; "
            "cross-currency inputs are blocked"
        ),
        "period_type": period_type,
        "period_end": period_end,
        "input_metric_units": f"{numerator.get('unit')},{denominator.get('unit')}",
    }


def _metric_rows(frame: pd.DataFrame, metric_id: str, period_type: str) -> pd.DataFrame:
    rows = frame.loc[
        (frame["metric_id"].astype(str) == metric_id)
        & (frame["period_type"].astype(str) == period_type)
    ].copy()
    if rows.empty:
        return rows
    return rows.sort_values(["period_end", "available_from_signal_date", "available_time_utc"])


def _active_intervals(intervals: pd.DataFrame, decision_date: date) -> pd.DataFrame:
    if intervals.empty:
        return intervals
    decision_timestamp = pd.Timestamp(decision_date)
    starts = pd.to_datetime(intervals["available_from_signal_date"], errors="coerce")
    until = pd.to_datetime(intervals["available_until_signal_date"], errors="coerce")
    mask = (starts <= decision_timestamp) & (until.isna() | (until >= decision_timestamp))
    return intervals.loc[mask].copy()


def _trading_days(start: date, end: date) -> tuple[date, ...]:
    current = start
    values: list[date] = []
    while current <= end:
        if is_us_equity_trading_day(current):
            values.append(current)
        current += timedelta(days=1)
    return tuple(values)


def _panel_record_to_feature_row(record: dict[str, object], path: Path) -> SecFundamentalFeatureRow:
    metric_ids = str(record.get("input_metric_ids") or ",").split(",")
    values = str(record.get("input_accession_numbers") or "")
    return SecFundamentalFeatureRow(
        as_of=_required_date(record.get("decision_date")),
        ticker=str(record.get("ticker") or "").upper(),
        period_type=cast(PeriodType, str(record.get("period_type") or "quarterly")),
        fiscal_year=None,
        fiscal_period="",
        end_date=_date_or_none(record.get("period_end")),
        filed_date=_required_date(record.get("decision_date")),
        feature_id=str(record.get("feature_id") or ""),
        feature_name=str(record.get("feature_id") or "").replace("_", " ").title(),
        value=_float_or_none(record.get("feature_value")) or 0.0,
        unit=str(record.get("feature_unit") or "ratio"),
        numerator_metric_id=metric_ids[0] if metric_ids else "",
        denominator_metric_id=metric_ids[1] if len(metric_ids) > 1 else "",
        numerator_value=0.0,
        denominator_value=0.0,
        source_metric_accessions=values,
        source_path=path,
    )


def _combined_confidence(*levels: str) -> str:
    normalized = {level.lower() for level in levels}
    if "low" in normalized:
        return "low"
    if "medium" in normalized:
        return "medium"
    return "high"


def _required_date(value: object) -> date:
    parsed = _date_or_none(value)
    if parsed is None:
        raise ValueError(f"missing required date value: {value}")
    return parsed


def _date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
