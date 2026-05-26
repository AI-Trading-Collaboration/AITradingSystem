from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from numbers import Real
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import FundamentalMetricConfig, FundamentalMetricsConfig
from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SecPitBackfillConfig,
)

SEC_PIT_MAPPED_METRIC_COLUMNS = (
    "ticker",
    "metric_id",
    "metric_name",
    "period_type",
    "fiscal_year",
    "fiscal_period",
    "period_start",
    "period_end",
    "value",
    "unit",
    "source_taxonomy",
    "source_concept",
    "source_accession_number",
    "source_form",
    "filing_acceptance_datetime_utc",
    "available_time_utc",
    "available_for_signal_date",
    "selection_rank",
    "selection_reason",
    "is_restated_fact",
    "supersedes_accession_number",
    "pit_data_grade",
    "confidence_level",
    "confidence_reason",
)


@dataclass(frozen=True)
class _MetricCandidate:
    record: dict[str, object]
    metric: FundamentalMetricConfig
    concept_priority: int


def build_mapped_metrics(
    *,
    facts: pd.DataFrame,
    metrics: FundamentalMetricsConfig,
    policy: SecPitBackfillConfig,
    end: date,
) -> pd.DataFrame:
    if facts.empty:
        return pd.DataFrame(columns=list(SEC_PIT_MAPPED_METRIC_COLUMNS))
    records: list[dict[str, object]] = []
    metric_rows_by_id = _selected_source_metric_rows(
        facts=facts,
        source_metrics=tuple([*metrics.metrics, *metrics.supporting_metrics]),
        policy=policy,
        end=end,
    )
    records.extend(
        _mapped_record(candidate, rank)
        for rank, candidate in enumerate(_ordered_candidates(metric_rows_by_id), start=1)
        if candidate.metric in metrics.metrics
    )
    records.extend(
        _derived_metric_records(
            metric_rows_by_id=metric_rows_by_id,
            output_metrics=tuple(metrics.metrics),
            derived_metrics=metrics.derived_metrics,
        )
    )
    frame = pd.DataFrame(records, columns=list(SEC_PIT_MAPPED_METRIC_COLUMNS))
    if frame.empty:
        return frame
    frame = _attach_restatement_lineage(frame)
    return frame.sort_values(
        ["ticker", "metric_id", "period_type", "period_end", "available_time_utc"]
    ).reset_index(drop=True)


def build_mapped_metrics_csv(
    *,
    facts_path: Path,
    metrics: FundamentalMetricsConfig,
    policy: SecPitBackfillConfig,
    end: date,
    output_path: Path,
) -> Path:
    facts = pd.read_csv(facts_path, dtype=str).fillna("") if facts_path.exists() else pd.DataFrame()
    frame = build_mapped_metrics(
        facts=facts,
        metrics=metrics,
        policy=policy,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def _selected_source_metric_rows(
    *,
    facts: pd.DataFrame,
    source_metrics: tuple[FundamentalMetricConfig, ...],
    policy: SecPitBackfillConfig,
    end: date,
) -> dict[str, list[_MetricCandidate]]:
    records = [dict(record) for record in facts.to_dict(orient="records")]
    selected_by_metric: dict[str, list[_MetricCandidate]] = {}
    allowed_forms = set(policy.metric_panel_forms)
    for metric in source_metrics:
        candidates: list[_MetricCandidate] = []
        for concept_priority, concept in enumerate(metric.concepts):
            for record in records:
                if str(record.get("taxonomy")) != concept.taxonomy:
                    continue
                if str(record.get("concept")) != concept.concept:
                    continue
                if str(record.get("unit")) != concept.unit:
                    continue
                if str(record.get("period_type")) not in set(metric.preferred_periods):
                    continue
                if str(record.get("form")).upper() not in allowed_forms:
                    continue
                available_date = _date_or_none(record.get("available_for_signal_date"))
                if available_date is None or available_date > end:
                    continue
                if _numeric_value(record.get("value")) is None:
                    continue
                candidates.append(
                    _MetricCandidate(
                        record=record,
                        metric=metric,
                        concept_priority=concept_priority,
                    )
                )
        selected_by_metric[metric.metric_id] = _dedupe_candidates(candidates)
    return selected_by_metric


def _dedupe_candidates(candidates: list[_MetricCandidate]) -> list[_MetricCandidate]:
    by_key: dict[tuple[str, str, str, str, str], _MetricCandidate] = {}
    for candidate in candidates:
        record = candidate.record
        key = (
            str(record.get("ticker")),
            str(candidate.metric.metric_id),
            str(record.get("period_type")),
            str(record.get("end_date")),
            str(record.get("accession_number")),
        )
        current = by_key.get(key)
        if current is None or _candidate_sort_key(candidate) > _candidate_sort_key(current):
            by_key[key] = candidate
    return sorted(by_key.values(), key=_candidate_sort_key)


def _ordered_candidates(
    by_metric: dict[str, list[_MetricCandidate]],
) -> tuple[_MetricCandidate, ...]:
    candidates = [candidate for rows in by_metric.values() for candidate in rows]
    return tuple(
        sorted(candidates, key=lambda item: (item.metric.metric_id, _candidate_sort_key(item)))
    )


def _mapped_record(candidate: _MetricCandidate, selection_rank: int) -> dict[str, object]:
    record = candidate.record
    return {
        "ticker": str(record.get("ticker") or "").upper(),
        "metric_id": candidate.metric.metric_id,
        "metric_name": candidate.metric.name,
        "period_type": str(record.get("period_type") or ""),
        "fiscal_year": str(record.get("fy") or ""),
        "fiscal_period": str(record.get("fp") or ""),
        "period_start": str(record.get("start_date") or ""),
        "period_end": str(record.get("end_date") or ""),
        "value": _numeric_value(record.get("value")) or 0.0,
        "unit": str(record.get("unit") or ""),
        "source_taxonomy": str(record.get("taxonomy") or ""),
        "source_concept": str(record.get("concept") or ""),
        "source_accession_number": str(record.get("accession_number") or ""),
        "source_form": str(record.get("form") or ""),
        "filing_acceptance_datetime_utc": str(record.get("filing_acceptance_datetime_utc") or ""),
        "available_time_utc": str(record.get("available_time_utc") or ""),
        "available_for_signal_date": str(record.get("available_for_signal_date") or ""),
        "selection_rank": selection_rank,
        "selection_reason": (
            "matched configured taxonomy/concept/unit; selected by available_time "
            "and concept priority"
        ),
        "is_restated_fact": "false",
        "supersedes_accession_number": "",
        "pit_data_grade": str(record.get("pit_data_grade") or SEC_PIT_BACKTEST_DATA_GRADE),
        "confidence_level": str(record.get("confidence_level") or "medium"),
        "confidence_reason": str(record.get("confidence_reason") or ""),
    }


def _derived_metric_records(
    *,
    metric_rows_by_id: dict[str, list[_MetricCandidate]],
    output_metrics: tuple[FundamentalMetricConfig, ...],
    derived_metrics: list[Any],
) -> list[dict[str, object]]:
    output_by_id = {metric.metric_id: metric for metric in output_metrics}
    derived_records: list[dict[str, object]] = []
    for derived in derived_metrics:
        metric = output_by_id.get(derived.metric_id)
        if metric is None:
            continue
        existing_keys = {
            _derive_key(candidate) for candidate in metric_rows_by_id.get(derived.metric_id, [])
        }
        minuend = metric_rows_by_id.get(derived.minuend_metric_id, [])
        subtrahend = metric_rows_by_id.get(derived.subtrahend_metric_id, [])
        subtrahend_by_key = {_derive_key(candidate): candidate for candidate in subtrahend}
        for candidate in minuend:
            if _derive_key(candidate) in existing_keys:
                continue
            matched = subtrahend_by_key.get(_derive_key(candidate))
            if matched is None:
                continue
            minuend_value = _numeric_value(candidate.record.get("value"))
            subtrahend_value = _numeric_value(matched.record.get("value"))
            if minuend_value is None or subtrahend_value is None:
                continue
            synthetic = _MetricCandidate(
                record={
                    **candidate.record,
                    "value": minuend_value - subtrahend_value,
                    "taxonomy": "derived",
                    "concept": (
                        "derived:" f"{derived.minuend_metric_id}-{derived.subtrahend_metric_id}"
                    ),
                },
                metric=metric,
                concept_priority=0,
            )
            derived_records.append(_mapped_record(synthetic, len(derived_records) + 1))
    return derived_records


def _derive_key(candidate: _MetricCandidate) -> tuple[str, str, str, str, str, str]:
    record = candidate.record
    return (
        str(record.get("ticker")),
        str(record.get("period_type")),
        str(record.get("fy")),
        str(record.get("fp")),
        str(record.get("end_date")),
        str(record.get("accession_number")),
    )


def _attach_restatement_lineage(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["is_restated_fact"] = "false"
    frame["supersedes_accession_number"] = ""
    key_columns = ["ticker", "metric_id", "period_type", "period_end"]
    for _key, group in frame.groupby(key_columns, sort=False):
        ordered = group.sort_values(["available_for_signal_date", "available_time_utc"])
        previous_accession = ""
        for index, row in ordered.iterrows():
            accession = str(row.get("source_accession_number") or "")
            if previous_accession and accession != previous_accession:
                frame.at[index, "is_restated_fact"] = "true"
                frame.at[index, "supersedes_accession_number"] = previous_accession
            if accession:
                previous_accession = accession
    return frame


def _candidate_sort_key(candidate: _MetricCandidate) -> tuple[str, str, str, int]:
    record = candidate.record
    return (
        str(record.get("available_for_signal_date") or ""),
        str(record.get("available_time_utc") or ""),
        str(record.get("filed_date") or ""),
        -candidate.concept_priority,
    )


def _date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _numeric_value(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            return None
        return numeric if math.isfinite(numeric) else None
    return None
