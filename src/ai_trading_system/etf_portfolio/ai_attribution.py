from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START_DATE

DEFAULT_AI_ATTRIBUTION_REPORT_ROOT = PROJECT_ROOT / "reports" / "etf_portfolio" / "ai_attribution"
DEFAULT_AI_ATTRIBUTION_DATASET_DIR = DEFAULT_AI_ATTRIBUTION_REPORT_ROOT / "datasets"
DEFAULT_AI_ATTRIBUTION_REVIEW_DIR = DEFAULT_AI_ATTRIBUTION_REPORT_ROOT / "reports"
DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR = DEFAULT_AI_ATTRIBUTION_REPORT_ROOT / "validation"

AI_ATTRIBUTION_DATASET_SCHEMA_VERSION = "ai_attribution_dataset_v1"
AI_ATTRIBUTION_REPORT_SCHEMA_VERSION = "ai_attribution_report_v1"
AI_ATTRIBUTION_VALIDATION_SCHEMA_VERSION = "ai_attribution_validation_v1"

AI_ATTRIBUTION_MARKET_REGIME = "unified_primary_2021"
AI_ATTRIBUTION_REGIME_START = PRIMARY_RESEARCH_START_DATE
FORWARD_WINDOWS: tuple[int, ...] = (1, 5, 20, 60)
PRICE_TARGETS: tuple[str, ...] = ("QQQ", "SPY", "SMH", "SOXX")

AI_ATTRIBUTION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

SCORE_BUCKETS: tuple[tuple[str, float, float | None], ...] = (
    ("negative", 0.0, 30.0),
    ("weak", 30.0, 45.0),
    ("neutral", 45.0, 65.0),
    ("confirm", 65.0, 80.0),
    ("strong_confirm", 80.0, None),
)
EVENT_RISK_BUCKETS: tuple[tuple[str, float, float | None], ...] = (
    ("low", 0.0, 30.0),
    ("medium", 30.0, 60.0),
    ("high", 60.0, 80.0),
    ("critical", 80.0, None),
)

# TRADING-072 pilot reporting floor only; it lowers confidence and never grants action rights.
MIN_ATTRIBUTION_SAMPLE_COUNT = 5
# TRADING-072 pilot evidence floor for report-level lift summaries only.
MEANINGFUL_LIFT_THRESHOLD = 0.005
# TRADING-072 pilot false-positive/negative severity boundary for event-risk diagnostics only.
EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD = 0.02
# TRADING-072 pilot overlap bands for redundancy warnings only.
REDUNDANCY_MEDIUM_CORRELATION = 0.50
REDUNDANCY_HIGH_CORRELATION = 0.75

FORBIDDEN_OUTPUT_KEYS = {
    "production_weight_update",
    "candidate_auto_promotion",
    "broker_order",
    "baseline_config_mutation",
    "target_weights",
    "production_weights",
}


class AIAttributionError(ValueError):
    """Raised when an AI attribution payload violates its audit contract."""


def load_ai_confirmation_report_payloads(
    report_dir: Path,
    *,
    as_of: date,
    start: date = AI_ATTRIBUTION_REGIME_START,
) -> list[dict[str, Any]]:
    if not report_dir.exists():
        return []
    reports: list[dict[str, Any]] = []
    for path in sorted(report_dir.glob("ai_confirmation_report_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        score_date = _parse_date(payload.get("date"))
        if score_date is None or score_date < start or score_date > as_of:
            continue
        payload = dict(payload)
        payload["source_report_path"] = str(path)
        reports.append(payload)
    return sorted(reports, key=lambda item: str(item.get("date")))


def build_ai_attribution_dataset(
    *,
    ai_confirmation_reports: Iterable[Mapping[str, Any]],
    prices: pd.DataFrame,
    evaluation_as_of_date: date,
    start: date = AI_ATTRIBUTION_REGIME_START,
    forward_windows: Iterable[int] = FORWARD_WINDOWS,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    market_regime: str = AI_ATTRIBUTION_MARKET_REGIME,
    requested_date_range: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    price_frame = _prepare_price_frame(prices, evaluation_as_of_date)
    windows = tuple(int(window) for window in forward_windows)
    records: list[dict[str, Any]] = []
    source_reports: list[dict[str, Any]] = []

    for report in sorted(ai_confirmation_reports, key=lambda item: str(item.get("date"))):
        score_date = _parse_date(report.get("date"))
        if score_date is None or score_date < start or score_date > evaluation_as_of_date:
            continue
        source_path = _text(report.get("source_report_path"))
        source_reports.append(
            {
                "score_date": score_date.isoformat(),
                "source_report_path": source_path,
                "score_band": _text(
                    _mapping(report.get("AIConfirmationScore")).get("score_band"),
                    "unknown",
                ),
            }
        )
        for window in windows:
            row = _dataset_record_for_report(
                report=report,
                price_frame=price_frame,
                score_date=score_date,
                evaluation_as_of_date=evaluation_as_of_date,
                forward_window=window,
                market_regime=market_regime,
                source_report_path=source_path,
            )
            records.append(row)

    payload = {
        "schema_version": AI_ATTRIBUTION_DATASET_SCHEMA_VERSION,
        "report_type": "ai_attribution_dataset",
        "attribution_dataset_id": _stable_id(
            "ai-attribution-dataset",
            evaluation_as_of_date.isoformat(),
            str(len(records)),
        ),
        "task": "TRADING-072A",
        "as_of_date": evaluation_as_of_date.isoformat(),
        "evaluation_as_of_date": evaluation_as_of_date.isoformat(),
        "market_regime": market_regime,
        "requested_date_range": dict(
            requested_date_range
            or {
                "start": start.isoformat(),
                "end": evaluation_as_of_date.isoformat(),
            }
        ),
        "forward_windows": [f"{window}D" for window in windows],
        "data_quality": {
            "status": data_quality_status,
            "report_path": data_quality_report,
        },
        "source_reports": source_reports,
        "record_count": len(records),
        "available_sample_count": len(
            [record for record in records if record.get("sample_available") is True]
        ),
        "records": records,
        "production_weights_mutated": False,
        "evaluation_only": True,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    validate_ai_attribution_dataset(payload)
    return payload


def validate_ai_attribution_dataset(payload: Mapping[str, Any]) -> None:
    issues: list[str] = []
    issues.extend(_safety_issues(payload, owner_id="dataset"))
    if payload.get("evaluation_only") is not True:
        issues.append("dataset:EVALUATION_ONLY_REQUIRED")
    for key in FORBIDDEN_OUTPUT_KEYS:
        if key in payload:
            issues.append(f"dataset:FORBIDDEN_OUTPUT_KEY:{key}")
    records = _records(payload.get("records"))
    for index, record in enumerate(records):
        owner = f"dataset.records[{index}]"
        issues.extend(_safety_issues(record, owner_id=owner))
        if record.get("evaluation_only") is not True:
            issues.append(f"{owner}:EVALUATION_ONLY_REQUIRED")
        for key in FORBIDDEN_OUTPUT_KEYS:
            if key in record:
                issues.append(f"{owner}:FORBIDDEN_OUTPUT_KEY:{key}")
        if not _text(record.get("score_date")):
            issues.append(f"{owner}:SCORE_DATE_REQUIRED")
        if not _text(record.get("forward_window")):
            issues.append(f"{owner}:FORWARD_WINDOW_REQUIRED")
        if not _text(record.get("evaluation_as_of_date")):
            issues.append(f"{owner}:EVALUATION_AS_OF_DATE_REQUIRED")
    if issues:
        raise AIAttributionError(";".join(issues))


def write_ai_attribution_dataset(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
) -> dict[str, Path]:
    validate_ai_attribution_dataset(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"ai_attribution_dataset_{as_of}.json"
    csv_path = output_dir / f"ai_attribution_dataset_{as_of}.csv"
    markdown_path = output_dir / f"ai_attribution_dataset_{as_of}.md"
    _write_json(payload, json_path)
    ai_attribution_dataset_records_to_frame(payload).to_csv(csv_path, index=False)
    markdown_path.write_text(render_ai_attribution_dataset_markdown(payload), encoding="utf-8")
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def ai_attribution_dataset_records_to_frame(payload: Mapping[str, Any]) -> pd.DataFrame:
    records = _records(payload.get("records"))
    if not records:
        return pd.DataFrame()
    normalized: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)
        row["safety_json"] = json.dumps(row.pop("safety", {}), ensure_ascii=False, sort_keys=True)
        normalized.append(row)
    return pd.DataFrame(normalized)


def render_ai_attribution_dataset_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# AI Confirmation Forward Attribution Dataset",
            "",
            f"- Task: {payload.get('task')}",
            f"- As Of: {payload.get('as_of_date')}",
            f"- Market Regime: {payload.get('market_regime')}",
            f"- Requested Date Range: {_mapping(payload.get('requested_date_range'))}",
            f"- Record Count: {payload.get('record_count')}",
            f"- Available Sample Count: {payload.get('available_sample_count')}",
            "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
            "broker_action=none, manual_review_required=true",
            "- Forward Return Usage: evaluation_only=true",
            "",
        ]
    )


def build_ai_score_bucket_forward_return_analysis(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    records = _records(dataset_payload.get("records"))
    rows: list[dict[str, Any]] = []
    for window in _dataset_windows(dataset_payload):
        window_records = [
            record
            for record in records
            if _text(record.get("forward_window")) == window
            and record.get("sample_available") is True
        ]
        for bucket_id, _, _ in SCORE_BUCKETS:
            bucket_records = [
                record
                for record in window_records
                if score_bucket(_float_or_none(record.get("AIConfirmationScore"))) == bucket_id
            ]
            rows.append(
                {
                    "forward_window": window,
                    "score_bucket": bucket_id,
                    **_forward_metric_summary(bucket_records, min_sample_count=min_sample_count),
                }
            )
    payload = {
        "schema_version": "ai_attribution_bucket_analysis_v1",
        "report_type": "ai_attribution_score_bucket_analysis",
        "task": "TRADING-072B",
        "as_of_date": dataset_payload.get("as_of_date"),
        "bucket_policy": {
            "negative": "0-30",
            "weak": "30-45",
            "neutral": "45-65",
            "confirm": "65-80",
            "strong_confirm": "80-100",
            "min_sample_count": min_sample_count,
        },
        "buckets": rows,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="bucket_analysis")
    return payload


def build_component_level_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    records = [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]
    component_fields = {
        "SemiconductorBreadthScore": "SemiconductorBreadthScore",
        "MegaCapAIScore": "MegaCapAIScore",
        "AISemiconductorRelativeStrengthScore": "AISemiconductorRelativeStrengthScore",
        "EventRiskScore": "EventRiskScore",
        "DataCoverageScore": "DataCoverageScore",
    }
    target_fields = {
        "QQQ_minus_SPY": "QQQ_minus_SPY_forward_return",
        "SMH_minus_QQQ": "SMH_minus_QQQ_forward_return",
        "SMH_forward_return": "SMH_forward_return",
        "SOXX_forward_return": "SOXX_forward_return",
        "candidate_overlay_forward_return": "candidate_overlay_forward_return",
    }
    component_results: list[dict[str, Any]] = []
    for component_id, component_field in component_fields.items():
        component_records = [
            record for record in records if _float_or_none(record.get(component_field)) is not None
        ]
        target_results: list[dict[str, Any]] = []
        for target_id, target_field in target_fields.items():
            paired = [
                record
                for record in component_records
                if _float_or_none(record.get(target_field)) is not None
            ]
            target_results.append(
                {
                    "target": target_id,
                    "sample_count": len(paired),
                    "rank_correlation_with_forward_return": _rank_correlation(
                        [_float_or_none(row.get(component_field)) for row in paired],
                        [_float_or_none(row.get(target_field)) for row in paired],
                    ),
                    "directional_hit_rate": _directional_hit_rate(
                        paired,
                        component_field=component_field,
                        target_field=target_field,
                        event_risk_component=component_id == "EventRiskScore",
                    ),
                    "incremental_excess_return_by_bucket": _incremental_lift(
                        paired,
                        score_field=component_field,
                        target_field=target_field,
                    ),
                }
            )
        drawdown_relationship = _rank_correlation(
            [_float_or_none(row.get(component_field)) for row in component_records],
            [_drawdown_severity(row.get("max_drawdown_forward")) for row in component_records],
        )
        component_results.append(
            {
                "component": component_id,
                "sample_count": len(component_records),
                "component_bucket_analysis": _component_bucket_summary(
                    component_records,
                    score_field=component_field,
                    min_sample_count=min_sample_count,
                ),
                "targets": target_results,
                "forward_drawdown_relationship": drawdown_relationship,
                "stability_warning": (
                    "insufficient_sample" if len(component_records) < min_sample_count else "none"
                ),
            }
        )
    payload = {
        "schema_version": "ai_attribution_component_analysis_v1",
        "report_type": "ai_attribution_component_analysis",
        "task": "TRADING-072C",
        "as_of_date": dataset_payload.get("as_of_date"),
        "components": component_results,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="component_attribution")
    return payload


def build_regime_conditional_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    records = [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]
    regimes = sorted({_text(record.get("regime"), "unknown") or "unknown" for record in records})
    rows: list[dict[str, Any]] = []
    for regime in regimes or ["unknown"]:
        regime_records = [
            record
            for record in records
            if (_text(record.get("regime"), "unknown") or "unknown") == regime
        ]
        for bucket_id, _, _ in SCORE_BUCKETS:
            bucket_records = [
                record
                for record in regime_records
                if score_bucket(_float_or_none(record.get("AIConfirmationScore"))) == bucket_id
            ]
            summary = _forward_metric_summary(
                bucket_records,
                min_sample_count=min_sample_count,
            )
            rows.append(
                {
                    "regime": regime,
                    "score_bucket": bucket_id,
                    "sample_count": summary["sample_count"],
                    "mean_forward_return": summary["mean_forward_return"],
                    "mean_excess_vs_baseline": summary["mean_excess_vs_SPY"],
                    "mean_SMH_minus_QQQ": summary["mean_SMH_minus_QQQ"],
                    "mean_QQQ_minus_SPY": summary["mean_QQQ_minus_SPY"],
                    "drawdown": summary["mean_forward_drawdown"],
                    "volatility": summary["mean_forward_volatility"],
                    "hit_rate": summary["hit_rate_positive"],
                    "confidence_warning": summary["confidence_warning"],
                }
            )
    payload = {
        "schema_version": "ai_attribution_regime_analysis_v1",
        "report_type": "ai_attribution_regime_analysis",
        "task": "TRADING-072D",
        "as_of_date": dataset_payload.get("as_of_date"),
        "regime_bucket_metrics": rows,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="regime_attribution")
    return payload


def build_event_risk_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    records = [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]
    rows: list[dict[str, Any]] = []
    for bucket_id, _, _ in EVENT_RISK_BUCKETS:
        bucket_records = [
            record
            for record in records
            if event_risk_bucket(_float_or_none(record.get("EventRiskScore"))) == bucket_id
        ]
        high_risk = bucket_id in {"high", "critical"}
        rows.append(
            {
                "event_risk_bucket": bucket_id,
                "sample_count": len(bucket_records),
                "forward_max_drawdown": _mean(
                    _float_or_none(record.get("max_drawdown_forward")) for record in bucket_records
                ),
                "forward_volatility": _mean(
                    _float_or_none(record.get("realized_vol_forward")) for record in bucket_records
                ),
                "negative_return_hit_rate": _share(
                    (_float_or_none(record.get("QQQ_forward_return")) or 0.0) < 0
                    for record in bucket_records
                ),
                "SMH_minus_QQQ_forward": _mean(
                    _float_or_none(record.get("SMH_minus_QQQ_forward_return"))
                    for record in bucket_records
                ),
                "QQQ_minus_SPY_forward": _mean(
                    _float_or_none(record.get("QQQ_minus_SPY_forward_return"))
                    for record in bucket_records
                ),
                "risk_event_false_positive_rate": (
                    _risk_false_positive_rate(bucket_records) if high_risk else None
                ),
                "risk_event_false_negative_rate": (
                    None if high_risk else _risk_false_negative_rate(bucket_records)
                ),
                "confidence_warning": (
                    "insufficient_sample" if len(bucket_records) < min_sample_count else "none"
                ),
            }
        )
    payload = {
        "schema_version": "ai_attribution_event_risk_analysis_v1",
        "report_type": "ai_attribution_event_risk_analysis",
        "task": "TRADING-072E",
        "as_of_date": dataset_payload.get("as_of_date"),
        "event_risk_bucket_metrics": rows,
        "risk_policy": {
            "drawdown_severity_threshold": EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD,
            "min_sample_count": min_sample_count,
        },
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="event_risk_attribution")
    return payload


def build_redundancy_diagnostics(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    records = [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]
    comparison_fields = {
        "QQQ_momentum": "QQQ_momentum_20d",
        "SMH_momentum": "SMH_momentum_20d",
        "SMH_QQQ_relative_strength": "SMH_QQQ_relative_strength_20d",
        "QQQ_SPY_relative_strength": "QQQ_SPY_relative_strength_20d",
        "ETF_baseline_signal_score": "ETF_baseline_signal_score",
        "Regime_score": "Regime_score",
    }
    rows: list[dict[str, Any]] = []
    max_abs_correlation = None
    for comparison_id, field in comparison_fields.items():
        paired = [
            record
            for record in records
            if _float_or_none(record.get("AIConfirmationScore")) is not None
            and _float_or_none(record.get(field)) is not None
        ]
        corr = _pearson_correlation(
            [_float_or_none(row.get("AIConfirmationScore")) for row in paired],
            [_float_or_none(row.get(field)) for row in paired],
        )
        rank_corr = _rank_correlation(
            [_float_or_none(row.get("AIConfirmationScore")) for row in paired],
            [_float_or_none(row.get(field)) for row in paired],
        )
        if corr is not None:
            max_abs_correlation = max(abs(corr), max_abs_correlation or 0.0)
        rows.append(
            {
                "comparison_signal": comparison_id,
                "sample_count": len(paired),
                "correlation": corr,
                "rank_correlation": rank_corr,
                "overlap_warning": _overlap_warning(corr, len(paired), min_sample_count),
            }
        )
    redundancy_band = _redundancy_band(max_abs_correlation, len(records), min_sample_count)
    incremental_bucket_lift = _incremental_lift(
        records,
        score_field="AIConfirmationScore",
        target_field="QQQ_minus_SPY_forward_return",
    )
    payload = {
        "schema_version": "ai_attribution_redundancy_diagnostics_v1",
        "report_type": "ai_attribution_redundancy_diagnostics",
        "task": "TRADING-072F",
        "as_of_date": dataset_payload.get("as_of_date"),
        "correlation_matrix": rows,
        "rank_correlation": {row["comparison_signal"]: row["rank_correlation"] for row in rows},
        "incremental_bucket_lift": incremental_bucket_lift,
        "residual_signal_summary": _residual_signal_summary(records),
        "redundancy_band": redundancy_band,
        "overlap_warning": (
            "high_overlap_with_existing_signals"
            if redundancy_band == "high"
            else (
                "medium_overlap_with_existing_signals"
                if redundancy_band == "medium"
                else "none" if redundancy_band == "low" else "unknown_insufficient_data"
            )
        ),
        "policy": {
            "medium_correlation": REDUNDANCY_MEDIUM_CORRELATION,
            "high_correlation": REDUNDANCY_HIGH_CORRELATION,
            "min_sample_count": min_sample_count,
        },
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="redundancy_diagnostics")
    return payload


def build_ai_attribution_evidence_scorecard(
    *,
    dataset_payload: Mapping[str, Any],
    bucket_analysis: Mapping[str, Any],
    component_attribution: Mapping[str, Any],
    regime_attribution: Mapping[str, Any],
    event_risk_attribution: Mapping[str, Any],
    redundancy_diagnostics: Mapping[str, Any],
    min_sample_count: int = MIN_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_ai_attribution_dataset(dataset_payload)
    data_quality_status = _text(_mapping(dataset_payload.get("data_quality")).get("status"))
    available_sample_count = _int(dataset_payload.get("available_sample_count"))
    records = [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]
    forward_evidence = _forward_evidence_score(bucket_analysis)
    semiconductor_evidence = _component_target_score(
        component_attribution,
        "SemiconductorBreadthScore",
        "SMH_minus_QQQ",
    )
    mega_cap_evidence = _component_target_score(
        component_attribution,
        "MegaCapAIScore",
        "QQQ_minus_SPY",
    )
    event_risk_evidence = _event_risk_evidence_score(event_risk_attribution)
    regime_stability = _regime_stability_score(regime_attribution)
    redundancy_penalty = _redundancy_penalty_score(redundancy_diagnostics)
    sample_quality = min(100.0, (available_sample_count / max(min_sample_count * 2, 1)) * 100.0)
    data_coverage = _mean(_float_or_none(record.get("DataCoverageScore")) for record in records)
    dimension_scores = {
        "forward_return_evidence": round(forward_evidence, 2),
        "semiconductor_relative_evidence": round(semiconductor_evidence, 2),
        "mega_cap_growth_evidence": round(mega_cap_evidence, 2),
        "event_risk_evidence": round(event_risk_evidence, 2),
        "regime_stability_evidence": round(regime_stability, 2),
        "redundancy_penalty": round(redundancy_penalty, 2),
        "sample_quality": round(sample_quality, 2),
        "data_coverage": round(data_coverage or 0.0, 2),
    }
    overall_status = _scorecard_status(
        data_quality_status=data_quality_status,
        sample_count=available_sample_count,
        dimension_scores=dimension_scores,
        redundancy_band=_text(redundancy_diagnostics.get("redundancy_band"), "unknown"),
        min_sample_count=min_sample_count,
    )
    supporting_evidence = _scorecard_supporting_evidence(
        dimension_scores,
        redundancy_band=_text(redundancy_diagnostics.get("redundancy_band"), "unknown"),
    )
    blocking_evidence = _scorecard_blocking_evidence(
        data_quality_status=data_quality_status,
        sample_count=available_sample_count,
        redundancy_band=_text(redundancy_diagnostics.get("redundancy_band"), "unknown"),
        min_sample_count=min_sample_count,
    )
    payload = {
        "schema_version": "ai_attribution_evidence_scorecard_v1",
        "report_type": "ai_attribution_evidence_scorecard",
        "task": "TRADING-072G",
        "scorecard_id": _stable_id(
            "ai-attribution-scorecard",
            _text(dataset_payload.get("as_of_date")),
            str(available_sample_count),
        ),
        "as_of_date": dataset_payload.get("as_of_date"),
        "overall_status": overall_status,
        "dimension_scores": dimension_scores,
        "supporting_evidence": supporting_evidence,
        "blocking_evidence": blocking_evidence,
        "sample_summary": {
            "record_count": dataset_payload.get("record_count"),
            "available_sample_count": available_sample_count,
            "source_report_count": len(_records(dataset_payload.get("source_reports"))),
            "min_sample_count": min_sample_count,
        },
        "manual_review_recommendation": _manual_review_recommendation(overall_status),
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    _assert_safe_payload(payload, owner_id="evidence_scorecard")
    return payload


def build_ai_attribution_report(
    dataset_payload: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    bucket_analysis = build_ai_score_bucket_forward_return_analysis(dataset_payload)
    component_attribution = build_component_level_attribution(dataset_payload)
    regime_attribution = build_regime_conditional_attribution(dataset_payload)
    event_risk_attribution = build_event_risk_attribution(dataset_payload)
    redundancy_diagnostics = build_redundancy_diagnostics(dataset_payload)
    scorecard = build_ai_attribution_evidence_scorecard(
        dataset_payload=dataset_payload,
        bucket_analysis=bucket_analysis,
        component_attribution=component_attribution,
        regime_attribution=regime_attribution,
        event_risk_attribution=event_risk_attribution,
        redundancy_diagnostics=redundancy_diagnostics,
    )
    payload = {
        "schema_version": AI_ATTRIBUTION_REPORT_SCHEMA_VERSION,
        "report_type": "ai_attribution_report",
        "task": "TRADING-072H",
        "report_id": _stable_id(
            "ai-attribution-report",
            _text(dataset_payload.get("as_of_date")),
            str(dataset_payload.get("record_count")),
        ),
        "as_of_date": dataset_payload.get("as_of_date"),
        "generated_at": generated.isoformat(),
        "status": scorecard["overall_status"],
        "safety_banner": _safety_banner(),
        "review_metadata": {
            "market_regime": dataset_payload.get("market_regime"),
            "requested_date_range": dataset_payload.get("requested_date_range"),
            "evaluation_as_of_date": dataset_payload.get("evaluation_as_of_date"),
            "production_effect": "none",
            "broker_action": "none",
        },
        "dataset_coverage": {
            "record_count": dataset_payload.get("record_count"),
            "available_sample_count": dataset_payload.get("available_sample_count"),
            "forward_windows": dataset_payload.get("forward_windows"),
            "data_quality": dataset_payload.get("data_quality"),
        },
        "score_bucket_analysis": bucket_analysis,
        "component_attribution": component_attribution,
        "regime_conditional_attribution": regime_attribution,
        "event_risk_attribution": event_risk_attribution,
        "redundancy_diagnostics": redundancy_diagnostics,
        "evidence_scorecard": scorecard,
        "manual_review_recommendations": [scorecard["manual_review_recommendation"]],
        "source_report_links": _records(dataset_payload.get("source_reports")),
        "dataset": dataset_payload,
        "production_weights_mutated": False,
        "evaluation_only": True,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    validate_ai_attribution_report(payload)
    return payload


def validate_ai_attribution_report(payload: Mapping[str, Any]) -> None:
    issues: list[str] = []
    issues.extend(_safety_issues(payload, owner_id="report"))
    if payload.get("evaluation_only") is not True:
        issues.append("report:EVALUATION_ONLY_REQUIRED")
    for key in FORBIDDEN_OUTPUT_KEYS:
        if key in payload:
            issues.append(f"report:FORBIDDEN_OUTPUT_KEY:{key}")
    required_sections = (
        "dataset_coverage",
        "score_bucket_analysis",
        "component_attribution",
        "regime_conditional_attribution",
        "event_risk_attribution",
        "redundancy_diagnostics",
        "evidence_scorecard",
        "source_report_links",
    )
    for section in required_sections:
        if section not in payload:
            issues.append(f"report:MISSING_SECTION:{section}")
    dataset = _mapping(payload.get("dataset"))
    if dataset:
        try:
            validate_ai_attribution_dataset(dataset)
        except AIAttributionError as exc:
            issues.append(f"report:DATASET_INVALID:{exc}")
    if issues:
        raise AIAttributionError(";".join(issues))


def write_ai_attribution_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
) -> dict[str, Path]:
    validate_ai_attribution_report(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"ai_attribution_report_{as_of}.json"
    markdown_path = output_dir / f"ai_attribution_report_{as_of}.md"
    _write_json(payload, json_path)
    markdown_path.write_text(render_ai_attribution_report_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_ai_attribution_report_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("review_metadata"))
    coverage = _mapping(payload.get("dataset_coverage"))
    scorecard = _mapping(payload.get("evidence_scorecard"))
    dimensions = _mapping(scorecard.get("dimension_scores"))
    source_links = _records(payload.get("source_report_links"))
    lines = [
        "# AI Confirmation Forward Attribution Review",
        "",
        f"- Status: {payload.get('status')}",
        f"- As Of: {payload.get('as_of_date')}",
        f"- Market Regime: {metadata.get('market_regime')}",
        f"- Requested Date Range: {metadata.get('requested_date_range')}",
        f"- Evaluation As Of Date: {metadata.get('evaluation_as_of_date')}",
        f"- Safety: {payload.get('safety_banner')}",
        "- Forward Return Usage: evaluation_only=true; attribution/reporting only.",
        "",
        "## Dataset Coverage",
        "",
        f"- Record Count: {coverage.get('record_count')}",
        f"- Available Sample Count: {coverage.get('available_sample_count')}",
        f"- Forward Windows: {_join_list(coverage.get('forward_windows'))}",
        f"- Data Quality: {_mapping(coverage.get('data_quality')).get('status')}",
        "",
        "## Evidence Scorecard",
        "",
        f"- Overall Status: {scorecard.get('overall_status')}",
        f"- Manual Review: {scorecard.get('manual_review_recommendation')}",
        "",
        "| Dimension | Score |",
        "|---|---:|",
    ]
    for dimension, score in dimensions.items():
        lines.append(f"| {dimension} | {_fmt_number(score)} |")
    lines.extend(
        [
            "",
            "## Attribution Sections",
            "",
            "- Score bucket analysis: included in JSON `score_bucket_analysis`.",
            "- Component-level attribution: included in JSON `component_attribution`.",
            "- Regime-conditional attribution: included in JSON `regime_conditional_attribution`.",
            "- Event risk attribution: included in JSON `event_risk_attribution`.",
            "- Redundancy diagnostics: included in JSON `redundancy_diagnostics`.",
            "",
            "## Source Reports",
            "",
        ]
    )
    if source_links:
        for source in source_links:
            lines.append(
                f"- {source.get('score_date')}: {source.get('source_report_path') or 'MISSING'}"
            )
    else:
        lines.append("- MISSING")
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- observe_only=true",
            "- candidate_only=true",
            "- production_effect=none",
            "- broker_action=none",
            "- manual_review_required=true",
            "- production weights are not mutated",
            "",
        ]
    )
    return "\n".join(lines)


def build_ai_attribution_validation_report(
    *,
    report_registry: Mapping[str, Any] | None = None,
    reader_brief_available: bool = False,
    report_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    sample_report = dict(report_payload or _validation_sample_report(generated))
    sample_dataset = _mapping(sample_report.get("dataset"))

    _append_check(
        checks,
        "dataset_builder_available",
        callable(build_ai_attribution_dataset),
        "AI attribution dataset builder is available.",
    )
    _append_check(
        checks,
        "score_bucket_analysis_available",
        callable(build_ai_score_bucket_forward_return_analysis),
        "Score bucket forward return analysis is available.",
    )
    _append_check(
        checks,
        "component_attribution_available",
        callable(build_component_level_attribution),
        "Component-level attribution is available.",
    )
    _append_check(
        checks,
        "regime_attribution_available",
        callable(build_regime_conditional_attribution),
        "Regime-conditional attribution is available.",
    )
    _append_check(
        checks,
        "event_risk_attribution_available",
        callable(build_event_risk_attribution),
        "Event risk attribution is available.",
    )
    _append_check(
        checks,
        "redundancy_diagnostics_available",
        callable(build_redundancy_diagnostics),
        "Redundancy diagnostics are available.",
    )
    _append_check(
        checks,
        "evidence_scorecard_available",
        callable(build_ai_attribution_evidence_scorecard),
        "Evidence scorecard is available.",
    )
    _append_check(
        checks,
        "report_generator_available",
        callable(build_ai_attribution_report),
        "AI attribution report generator is available.",
    )
    _append_check(
        checks,
        "reader_brief_integration_available",
        reader_brief_available,
        "Reader Brief AI Attribution Review section is wired.",
        blocker="READER_BRIEF_AI_ATTRIBUTION_SECTION_UNAVAILABLE",
    )
    _append_check(
        checks,
        "report_registry_integration_available",
        _report_registry_has_ai_attribution(report_registry),
        "Report registry exposes AI attribution report and validation artifacts.",
        blocker="REPORT_REGISTRY_MISSING_AI_ATTRIBUTION",
    )
    _append_check(
        checks,
        "forward_returns_evaluation_only",
        sample_dataset.get("evaluation_only") is True
        and all(
            record.get("evaluation_only") is True
            for record in _records(sample_dataset.get("records"))
        ),
        "Forward-return fields are marked evaluation_only=true.",
        blocker="FORWARD_RETURNS_NOT_EVALUATION_ONLY",
    )
    for field, expected in AI_ATTRIBUTION_SAFETY.items():
        _append_check(
            checks,
            f"{field}_safe",
            sample_report.get(field) == expected,
            f"{field}={expected}",
            blocker=f"UNSAFE_{field.upper()}",
        )
    _append_check(
        checks,
        "unsafe_outputs_absent",
        not any(key in sample_report for key in FORBIDDEN_OUTPUT_KEYS),
        "Forbidden production/trading output keys are absent.",
        blocker="FORBIDDEN_OUTPUT_KEY_PRESENT",
    )
    try:
        validate_ai_attribution_report(sample_report)
        report_schema_valid = True
    except AIAttributionError:
        report_schema_valid = False
    _append_check(
        checks,
        "report_payload_schema_valid",
        report_schema_valid,
        "Sample report passes AI attribution schema and safety validation.",
        blocker="REPORT_PAYLOAD_SCHEMA_INVALID",
    )

    failed = [check for check in checks if check["status"] == "FAIL"]
    payload = {
        "schema_version": AI_ATTRIBUTION_VALIDATION_SCHEMA_VERSION,
        "report_type": "ai_attribution_validation",
        "task": "TRADING-072J",
        "as_of_date": generated.date().isoformat(),
        "generated_at": generated.isoformat(),
        "status": "FAIL" if failed else "PASS",
        "failed_check_count": len(failed),
        "checks": checks,
        "sample_report_status": sample_report.get("status"),
        "production_weights_mutated": False,
        "evaluation_only": True,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    validate_ai_attribution_validation_report(payload)
    return payload


def validate_ai_attribution_validation_report(payload: Mapping[str, Any]) -> None:
    issues = _safety_issues(payload, owner_id="validation")
    if payload.get("evaluation_only") is not True:
        issues.append("validation:EVALUATION_ONLY_REQUIRED")
    if _int(payload.get("failed_check_count")) != len(
        [check for check in _records(payload.get("checks")) if check.get("status") == "FAIL"]
    ):
        issues.append("validation:FAILED_CHECK_COUNT_MISMATCH")
    if issues:
        raise AIAttributionError(";".join(issues))


def write_ai_attribution_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
) -> dict[str, Path]:
    validate_ai_attribution_validation_report(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"ai_attribution_validation_{as_of}.json"
    markdown_path = output_dir / f"ai_attribution_validation_{as_of}.md"
    _write_json(payload, json_path)
    markdown_path.write_text(render_ai_attribution_validation_markdown(payload), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_ai_attribution_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# AI Attribution Validation Gate",
        "",
        f"- Task: {payload.get('task')}",
        f"- Status: {payload.get('status')}",
        f"- Failed Checks: {payload.get('failed_check_count')}",
        "- Safety: observe_only=true, candidate_only=true, production_effect=none, "
        "broker_action=none, manual_review_required=true",
        "",
        "## Checks",
        "",
        "| Check | Status | Summary | Blockers |",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "| "
            f"{check.get('check_id')} | "
            f"{check.get('status')} | "
            f"{check.get('summary')} | "
            f"{_join_list(check.get('blockers'))} |"
        )
    return "\n".join(lines) + "\n"


def score_bucket(score: float | None) -> str:
    if score is None:
        return "unknown"
    for bucket_id, lower, upper in SCORE_BUCKETS:
        if score >= lower and (upper is None or score < upper):
            return bucket_id
    return "negative"


def event_risk_bucket(score: float | None) -> str:
    if score is None:
        return "unknown"
    for bucket_id, lower, upper in EVENT_RISK_BUCKETS:
        if score >= lower and (upper is None or score < upper):
            return bucket_id
    return "low"


def _dataset_record_for_report(
    *,
    report: Mapping[str, Any],
    price_frame: pd.DataFrame,
    score_date: date,
    evaluation_as_of_date: date,
    forward_window: int,
    market_regime: str,
    source_report_path: str,
) -> dict[str, Any]:
    score = _mapping(report.get("AIConfirmationScore"))
    component_scores = _mapping(score.get("component_scores")) or _mapping(
        report.get("component_scores")
    )
    event_risk = _mapping(report.get("event_risk_overlay"))
    data_coverage = _mapping(report.get("data_coverage"))
    forward = _forward_window_metrics(
        price_frame,
        score_date=score_date,
        evaluation_as_of_date=evaluation_as_of_date,
        window=forward_window,
    )
    score_value = _float_or_none(score.get("score_value"))
    event_risk_score = _float_or_none(event_risk.get("event_risk_score"))
    data_coverage_score = _float_or_none(component_scores.get("data_coverage"))
    if data_coverage_score is None:
        data_coverage_score = (
            _float_or_none(data_coverage.get("composite_data_coverage_ratio")) or 0.0
        ) * 100.0
    record = {
        "record_id": _stable_id(
            "ai-attribution-record",
            score_date.isoformat(),
            str(forward_window),
            source_report_path,
        ),
        "score_date": score_date.isoformat(),
        "evaluation_as_of_date": evaluation_as_of_date.isoformat(),
        "forward_window": f"{forward_window}D",
        "forward_window_days": forward_window,
        "forward_window_end_date": forward.get("forward_window_end_date"),
        "AIConfirmationScore": score_value,
        "SemiconductorBreadthScore": _float_or_none(component_scores.get("semiconductor_breadth")),
        "MegaCapAIScore": _float_or_none(component_scores.get("mega_cap_ai")),
        "AISemiconductorRelativeStrengthScore": _float_or_none(
            component_scores.get("ai_relative_strength")
        ),
        "EventRiskScore": event_risk_score,
        "DataCoverageScore": data_coverage_score,
        "score_band": _text(score.get("score_band"), score_bucket(score_value)),
        "event_risk_band": _text(event_risk.get("risk_band"), event_risk_bucket(event_risk_score)),
        "regime": _text(report.get("market_regime"), market_regime) or "unknown",
        "QQQ_forward_return": forward.get("QQQ_forward_return"),
        "SPY_forward_return": forward.get("SPY_forward_return"),
        "SMH_forward_return": forward.get("SMH_forward_return"),
        "SOXX_forward_return": forward.get("SOXX_forward_return"),
        "SMH_minus_QQQ_forward_return": forward.get("SMH_minus_QQQ_forward_return"),
        "QQQ_minus_SPY_forward_return": forward.get("QQQ_minus_SPY_forward_return"),
        "max_drawdown_forward": forward.get("max_drawdown_forward"),
        "realized_vol_forward": forward.get("realized_vol_forward"),
        "candidate_overlay_forward_return": None,
        "satellite_candidate_forward_return": None,
        "QQQ_momentum_20d": _momentum(price_frame, "QQQ", score_date, 20),
        "SMH_momentum_20d": _momentum(price_frame, "SMH", score_date, 20),
        "SMH_QQQ_relative_strength_20d": _relative_momentum(
            price_frame,
            "SMH",
            "QQQ",
            score_date,
            20,
        ),
        "QQQ_SPY_relative_strength_20d": _relative_momentum(
            price_frame,
            "QQQ",
            "SPY",
            score_date,
            20,
        ),
        "ETF_baseline_signal_score": _baseline_signal_proxy(price_frame, score_date),
        "Regime_score": _regime_score(_text(report.get("market_regime"), market_regime)),
        "sample_available": forward["sample_available"],
        "insufficient_data_reason": forward["insufficient_data_reason"],
        "source_report_path": source_report_path,
        "evaluation_only": True,
        "safety": dict(AI_ATTRIBUTION_SAFETY),
        **AI_ATTRIBUTION_SAFETY,
    }
    return record


def _prepare_price_frame(prices: pd.DataFrame, evaluation_as_of_date: date) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["_date", "symbol", "_adj_close"])
    frame = prices.copy()
    if "symbol" not in frame.columns or "date" not in frame.columns:
        raise AIAttributionError("prices must contain date and symbol columns")
    price_column = "adj_close" if "adj_close" in frame.columns else "close"
    if price_column not in frame.columns:
        raise AIAttributionError("prices must contain adj_close or close column")
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame[price_column], errors="coerce")
    frame = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] <= pd.Timestamp(evaluation_as_of_date))
        & frame["_adj_close"].notna()
        & (frame["_adj_close"] > 0)
    ].copy()
    return frame.sort_values(["symbol", "_date"]).reset_index(drop=True)


def _forward_window_metrics(
    price_frame: pd.DataFrame,
    *,
    score_date: date,
    evaluation_as_of_date: date,
    window: int,
) -> dict[str, Any]:
    symbol_returns: dict[str, float | None] = {}
    end_dates: list[date] = []
    missing: list[str] = []
    paths: list[pd.Series] = []
    for symbol in PRICE_TARGETS:
        result = _symbol_forward_path(
            price_frame,
            symbol,
            score_date=score_date,
            evaluation_as_of_date=evaluation_as_of_date,
            window=window,
        )
        if result["return"] is None:
            missing.append(f"{symbol}:{result['reason']}")
        else:
            end_dates.append(result["end_date"])
            paths.append(result["prices"])
        symbol_returns[f"{symbol}_forward_return"] = result["return"]
    sample_available = not missing
    qqq = symbol_returns.get("QQQ_forward_return")
    spy = symbol_returns.get("SPY_forward_return")
    smh = symbol_returns.get("SMH_forward_return")
    return {
        **symbol_returns,
        "SMH_minus_QQQ_forward_return": (None if smh is None or qqq is None else float(smh - qqq)),
        "QQQ_minus_SPY_forward_return": (None if qqq is None or spy is None else float(qqq - spy)),
        "max_drawdown_forward": _worst_drawdown(paths) if sample_available else None,
        "realized_vol_forward": _mean_realized_vol(paths) if sample_available else None,
        "forward_window_end_date": max(end_dates).isoformat() if end_dates else "",
        "sample_available": sample_available,
        "insufficient_data_reason": "none" if sample_available else ";".join(missing),
    }


def _symbol_forward_path(
    price_frame: pd.DataFrame,
    symbol: str,
    *,
    score_date: date,
    evaluation_as_of_date: date,
    window: int,
) -> dict[str, Any]:
    history = (
        price_frame.loc[price_frame["symbol"] == symbol].sort_values("_date").reset_index(drop=True)
    )
    if history.empty:
        return _missing_forward_result("missing_symbol")
    start_candidates = history.index[history["_date"] <= pd.Timestamp(score_date)].tolist()
    if not start_candidates:
        return _missing_forward_result("missing_score_date_price")
    start_idx = start_candidates[-1]
    end_idx = start_idx + int(window)
    if end_idx >= len(history):
        return _missing_forward_result("insufficient_forward_window")
    end_date = pd.Timestamp(history.loc[end_idx, "_date"]).date()
    if end_date > evaluation_as_of_date:
        return _missing_forward_result("forward_window_after_evaluation_as_of")
    prices = history.loc[start_idx:end_idx, "_adj_close"].astype(float).reset_index(drop=True)
    start_price = float(prices.iloc[0])
    end_price = float(prices.iloc[-1])
    return {
        "return": float(end_price / start_price - 1.0),
        "reason": "none",
        "end_date": end_date,
        "prices": prices,
    }


def _missing_forward_result(reason: str) -> dict[str, Any]:
    return {
        "return": None,
        "reason": reason,
        "end_date": None,
        "prices": pd.Series(dtype=float),
    }


def _momentum(
    price_frame: pd.DataFrame,
    symbol: str,
    score_date: date,
    window: int,
) -> float | None:
    history = (
        price_frame.loc[
            (price_frame["symbol"] == symbol) & (price_frame["_date"] <= pd.Timestamp(score_date))
        ]
        .sort_values("_date")
        .reset_index(drop=True)
    )
    if len(history) <= window:
        return None
    prices = history["_adj_close"].astype(float)
    return float(prices.iloc[-1] / prices.iloc[-window - 1] - 1.0)


def _relative_momentum(
    price_frame: pd.DataFrame,
    numerator: str,
    denominator: str,
    score_date: date,
    window: int,
) -> float | None:
    first = _momentum(price_frame, numerator, score_date, window)
    second = _momentum(price_frame, denominator, score_date, window)
    if first is None or second is None:
        return None
    return float(first - second)


def _baseline_signal_proxy(price_frame: pd.DataFrame, score_date: date) -> float | None:
    values = [
        _momentum(price_frame, "QQQ", score_date, 20),
        _momentum(price_frame, "SMH", score_date, 20),
        _relative_momentum(price_frame, "QQQ", "SPY", score_date, 20),
        _relative_momentum(price_frame, "SMH", "QQQ", score_date, 20),
    ]
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(np.mean(clean))


def _forward_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    return {
        "sample_count": len(records),
        "mean_forward_return": _mean(
            _float_or_none(row.get("QQQ_forward_return")) for row in records
        ),
        "median_forward_return": _median(
            _float_or_none(row.get("QQQ_forward_return")) for row in records
        ),
        "hit_rate_positive": _share(
            (_float_or_none(row.get("QQQ_forward_return")) or 0.0) > 0 for row in records
        ),
        "mean_excess_vs_SPY": _mean(
            _float_or_none(row.get("QQQ_minus_SPY_forward_return")) for row in records
        ),
        "mean_excess_vs_QQQ": _mean(
            _float_or_none(row.get("SMH_minus_QQQ_forward_return")) for row in records
        ),
        "mean_SMH_minus_QQQ": _mean(
            _float_or_none(row.get("SMH_minus_QQQ_forward_return")) for row in records
        ),
        "mean_QQQ_minus_SPY": _mean(
            _float_or_none(row.get("QQQ_minus_SPY_forward_return")) for row in records
        ),
        "mean_forward_drawdown": _mean(
            _float_or_none(row.get("max_drawdown_forward")) for row in records
        ),
        "mean_forward_volatility": _mean(
            _float_or_none(row.get("realized_vol_forward")) for row in records
        ),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _component_bucket_summary(
    records: list[dict[str, Any]],
    *,
    score_field: str,
    min_sample_count: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket_id, _, _ in SCORE_BUCKETS:
        bucket_records = [
            record
            for record in records
            if score_bucket(_float_or_none(record.get(score_field))) == bucket_id
        ]
        rows.append(
            {
                "score_bucket": bucket_id,
                **_forward_metric_summary(bucket_records, min_sample_count=min_sample_count),
            }
        )
    return rows


def _directional_hit_rate(
    records: list[dict[str, Any]],
    *,
    component_field: str,
    target_field: str,
    event_risk_component: bool,
) -> float | None:
    hits = []
    for record in records:
        score = _float_or_none(record.get(component_field))
        target = _float_or_none(record.get(target_field))
        if score is None or target is None:
            continue
        if event_risk_component:
            if score >= 60:
                hits.append(target <= 0)
            elif score < 30:
                hits.append(target >= 0)
        else:
            if score >= 65:
                hits.append(target > 0)
            elif score < 45:
                hits.append(target <= 0)
    return _share(hits)


def _incremental_lift(
    records: list[dict[str, Any]],
    *,
    score_field: str,
    target_field: str,
) -> float | None:
    high = [
        _float_or_none(row.get(target_field))
        for row in records
        if (_float_or_none(row.get(score_field)) or -1.0) >= 65.0
    ]
    low = [
        _float_or_none(row.get(target_field))
        for row in records
        if (_float_or_none(row.get(score_field)) or 101.0) < 45.0
    ]
    high_mean = _mean(high)
    low_mean = _mean(low)
    if high_mean is None or low_mean is None:
        return None
    return float(high_mean - low_mean)


def _risk_false_positive_rate(records: list[dict[str, Any]]) -> float | None:
    flags = []
    for record in records:
        qqq_return = _float_or_none(record.get("QQQ_forward_return"))
        drawdown = _drawdown_severity(record.get("max_drawdown_forward"))
        if qqq_return is None or drawdown is None:
            continue
        flags.append(qqq_return >= 0 and drawdown < EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD)
    return _share(flags)


def _risk_false_negative_rate(records: list[dict[str, Any]]) -> float | None:
    flags = []
    for record in records:
        qqq_return = _float_or_none(record.get("QQQ_forward_return"))
        drawdown = _drawdown_severity(record.get("max_drawdown_forward"))
        if qqq_return is None or drawdown is None:
            continue
        flags.append(qqq_return < 0 or drawdown >= EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD)
    return _share(flags)


def _residual_signal_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    ai_scores = [_float_or_none(record.get("AIConfirmationScore")) for record in records]
    baseline = [_float_or_none(record.get("ETF_baseline_signal_score")) for record in records]
    target = [_float_or_none(record.get("QQQ_minus_SPY_forward_return")) for record in records]
    paired = [
        (ai, base, future)
        for ai, base, future in zip(ai_scores, baseline, target, strict=False)
        if ai is not None and base is not None and future is not None
    ]
    if len(paired) < 3:
        return {
            "status": "unknown_insufficient_data",
            "sample_count": len(paired),
            "residual_rank_correlation": None,
        }
    ai_values = np.array([item[0] for item in paired], dtype=float)
    base_values = np.array([item[1] for item in paired], dtype=float)
    target_values = np.array([item[2] for item in paired], dtype=float)
    if float(np.var(base_values)) == 0.0:
        residual = ai_values - float(np.mean(ai_values))
    else:
        beta = float(np.cov(ai_values, base_values)[0, 1] / np.var(base_values))
        residual = ai_values - beta * base_values
    return {
        "status": "available",
        "sample_count": len(paired),
        "residual_rank_correlation": _rank_correlation(list(residual), list(target_values)),
    }


def _forward_evidence_score(bucket_analysis: Mapping[str, Any]) -> float:
    buckets = _records(bucket_analysis.get("buckets"))
    strong = [
        row
        for row in buckets
        if row.get("score_bucket") in {"confirm", "strong_confirm"}
        and _float_or_none(row.get("mean_QQQ_minus_SPY")) is not None
    ]
    weak = [
        row
        for row in buckets
        if row.get("score_bucket") in {"negative", "weak"}
        and _float_or_none(row.get("mean_QQQ_minus_SPY")) is not None
    ]
    lift = (_mean(row.get("mean_QQQ_minus_SPY") for row in strong) or 0.0) - (
        _mean(row.get("mean_QQQ_minus_SPY") for row in weak) or 0.0
    )
    if lift >= MEANINGFUL_LIFT_THRESHOLD * 2:
        return 80.0
    if lift >= MEANINGFUL_LIFT_THRESHOLD:
        return 65.0
    if lift > 0:
        return 55.0
    return 40.0


def _component_target_score(
    component_attribution: Mapping[str, Any],
    component_id: str,
    target_id: str,
) -> float:
    for component in _records(component_attribution.get("components")):
        if component.get("component") != component_id:
            continue
        for target in _records(component.get("targets")):
            if target.get("target") != target_id:
                continue
            lift = _float_or_none(target.get("incremental_excess_return_by_bucket"))
            corr = _float_or_none(target.get("rank_correlation_with_forward_return"))
            if lift is not None and lift >= MEANINGFUL_LIFT_THRESHOLD and (corr or 0.0) > 0:
                return 75.0
            if lift is not None and lift > 0:
                return 60.0
            return 45.0
    return 0.0


def _event_risk_evidence_score(event_risk_attribution: Mapping[str, Any]) -> float:
    rows = _records(event_risk_attribution.get("event_risk_bucket_metrics"))
    high = [row for row in rows if row.get("event_risk_bucket") in {"high", "critical"}]
    low = [row for row in rows if row.get("event_risk_bucket") in {"low", "medium"}]
    high_drawdown = _mean(_drawdown_severity(row.get("forward_max_drawdown")) for row in high)
    low_drawdown = _mean(_drawdown_severity(row.get("forward_max_drawdown")) for row in low)
    if high_drawdown is None or low_drawdown is None:
        return 40.0
    if high_drawdown - low_drawdown >= EVENT_RISK_DRAWDOWN_SEVERITY_THRESHOLD:
        return 75.0
    if high_drawdown > low_drawdown:
        return 60.0
    return 45.0


def _regime_stability_score(regime_attribution: Mapping[str, Any]) -> float:
    rows = [
        row
        for row in _records(regime_attribution.get("regime_bucket_metrics"))
        if row.get("score_bucket") in {"confirm", "strong_confirm"}
        and _int(row.get("sample_count")) > 0
    ]
    if not rows:
        return 40.0
    positive = [
        row for row in rows if (_float_or_none(row.get("mean_excess_vs_baseline")) or 0.0) > 0
    ]
    return float(len(positive) / len(rows) * 100.0)


def _redundancy_penalty_score(redundancy_diagnostics: Mapping[str, Any]) -> float:
    band = _text(redundancy_diagnostics.get("redundancy_band"), "unknown")
    return {
        "low": 100.0,
        "medium": 65.0,
        "high": 25.0,
        "unknown_insufficient_data": 45.0,
    }.get(band, 45.0)


def _scorecard_status(
    *,
    data_quality_status: str,
    sample_count: int,
    dimension_scores: Mapping[str, Any],
    redundancy_band: str,
    min_sample_count: int,
) -> str:
    if data_quality_status.upper().startswith("FAIL"):
        return "blocked_by_data_quality"
    if sample_count < min_sample_count:
        return "needs_more_data"
    forward = _float_or_none(dimension_scores.get("forward_return_evidence")) or 0.0
    semiconductor = _float_or_none(dimension_scores.get("semiconductor_relative_evidence")) or 0.0
    event = _float_or_none(dimension_scores.get("event_risk_evidence")) or 0.0
    redundancy_score = _float_or_none(dimension_scores.get("redundancy_penalty")) or 0.0
    if redundancy_band == "high" and forward < 70.0:
        return "noisy_or_redundant"
    if forward >= 70.0 and max(semiconductor, event) >= 70.0 and redundancy_score >= 65.0:
        return "useful_candidate_overlay_factor"
    if forward < 50.0 and redundancy_score < 65.0:
        return "noisy_or_redundant"
    return "reporting_only"


def _scorecard_supporting_evidence(
    dimension_scores: Mapping[str, Any],
    *,
    redundancy_band: str,
) -> list[str]:
    evidence: list[str] = []
    if (_float_or_none(dimension_scores.get("forward_return_evidence")) or 0.0) >= 65.0:
        evidence.append("forward_return_bucket_lift_positive")
    if (_float_or_none(dimension_scores.get("semiconductor_relative_evidence")) or 0.0) >= 65.0:
        evidence.append("semiconductor_relative_evidence_positive")
    if (_float_or_none(dimension_scores.get("event_risk_evidence")) or 0.0) >= 65.0:
        evidence.append("event_risk_forward_risk_relationship_positive")
    if redundancy_band == "low":
        evidence.append("low_overlap_with_existing_momentum_signals")
    return evidence or ["insufficient_positive_evidence"]


def _scorecard_blocking_evidence(
    *,
    data_quality_status: str,
    sample_count: int,
    redundancy_band: str,
    min_sample_count: int,
) -> list[str]:
    blockers: list[str] = []
    if data_quality_status.upper().startswith("FAIL"):
        blockers.append("DATA_QUALITY_BLOCKED")
    if sample_count < min_sample_count:
        blockers.append("INSUFFICIENT_FORWARD_ATTRIBUTION_SAMPLE")
    if redundancy_band == "high":
        blockers.append("HIGH_REDUNDANCY_WITH_EXISTING_SIGNALS")
    return blockers


def _manual_review_recommendation(status: str) -> str:
    return {
        "useful_candidate_overlay_factor": (
            "继续人工复核 evidence；不得自动提高 AI overlay 权重。"
        ),
        "reporting_only": "保持 reporting-only，等待更多 forward attribution 样本。",
        "needs_more_data": "继续 observe-only 积累样本；不得把当前结果写成交易结论。",
        "noisy_or_redundant": (
            "保持 reporting-only，并检查是否只是 momentum/relative strength proxy。"
        ),
        "blocked_by_data_quality": "先修复数据质量门禁，再解释 attribution 结果。",
    }.get(status, "保持人工复核。")


def _validation_sample_report(generated_at: datetime) -> dict[str, Any]:
    as_of = date(2026, 6, 30)
    prices = _validation_prices()
    reports = _validation_ai_reports()
    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=as_of,
        data_quality_status="PASS",
        data_quality_report="validation_sample_data_quality.md",
        requested_date_range={"start": "2022-12-01", "end": as_of.isoformat()},
    )
    return build_ai_attribution_report(dataset, generated_at=generated_at)


def _validation_prices() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    dates = pd.bdate_range("2026-01-02", periods=140)
    slopes = {"SPY": 0.10, "QQQ": 0.14, "SMH": 0.20, "SOXX": 0.19}
    for index, current_date in enumerate(dates):
        for symbol, slope in slopes.items():
            price = 100.0 + index * slope
            if symbol in {"SMH", "SOXX"} and index > 45:
                price += 2.0
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "validation_fixture",
                    "created_at": "2026-06-30T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _validation_ai_reports() -> list[dict[str, Any]]:
    score_dates = pd.bdate_range("2026-03-02", periods=8, freq="10B")
    scores = [35.0, 42.0, 52.0, 66.0, 72.0, 81.0, 86.0, 90.0]
    reports: list[dict[str, Any]] = []
    for index, (score_date, score) in enumerate(zip(score_dates, scores, strict=True), start=1):
        event_risk = 20.0 if score < 80 else 65.0
        reports.append(
            {
                "date": score_date.date().isoformat(),
                "market_regime": "growth_leadership" if score >= 65 else "neutral",
                "source_report_path": f"validation_ai_confirmation_{index}.json",
                "AIConfirmationScore": {
                    "score_value": score,
                    "score_band": score_bucket(score),
                    "action_hint": "supports_ai_overweight_candidate",
                    "component_scores": {
                        "semiconductor_breadth": score,
                        "mega_cap_ai": min(100.0, score + 3.0),
                        "ai_relative_strength": min(100.0, score + 2.0),
                        "event_risk_adjustment": 100.0 - event_risk,
                        "data_coverage": 95.0,
                    },
                    **AI_ATTRIBUTION_SAFETY,
                },
                "component_scores": {
                    "semiconductor_breadth": score,
                    "mega_cap_ai": min(100.0, score + 3.0),
                    "ai_relative_strength": min(100.0, score + 2.0),
                    "event_risk_adjustment": 100.0 - event_risk,
                    "data_coverage": 95.0,
                },
                "event_risk_overlay": {
                    "event_risk_score": event_risk,
                    "risk_band": event_risk_bucket(event_risk),
                    **AI_ATTRIBUTION_SAFETY,
                },
                "data_coverage": {
                    "composite_data_coverage_ratio": 0.95,
                },
                **AI_ATTRIBUTION_SAFETY,
            }
        )
    return reports


def _append_check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    summary: str,
    *,
    blocker: str | None = None,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "summary": summary,
            "blockers": [] if passed or blocker is None else [blocker],
            "production_effect": "none",
        }
    )


def _report_registry_has_ai_attribution(report_registry: Mapping[str, Any] | None) -> bool:
    reports = _records(_mapping(report_registry).get("reports"))
    by_id = {_text(report.get("report_id")): report for report in reports}
    required = ("etf_ai_attribution_report", "etf_ai_attribution_validation")
    for report_id in required:
        report = by_id.get(report_id)
        if not report:
            return False
        if report.get("include_in_reader_brief") is not True:
            return False
    return True


def _dataset_windows(dataset_payload: Mapping[str, Any]) -> list[str]:
    windows = [str(window) for window in dataset_payload.get("forward_windows") or []]
    if windows:
        return windows
    return sorted(
        {_text(record.get("forward_window")) for record in _records(dataset_payload.get("records"))}
    )


def _worst_drawdown(paths: list[pd.Series]) -> float | None:
    drawdowns = [_path_drawdown(path) for path in paths if len(path) > 1]
    return _min(drawdowns)


def _path_drawdown(path: pd.Series) -> float | None:
    if path.empty:
        return None
    values = path.astype(float).reset_index(drop=True)
    rolling_peak = values.cummax()
    drawdowns = values / rolling_peak - 1.0
    return float(drawdowns.min())


def _mean_realized_vol(paths: list[pd.Series]) -> float | None:
    vols = []
    for path in paths:
        if len(path) <= 1:
            continue
        returns = path.astype(float).pct_change().dropna()
        if returns.empty:
            continue
        vols.append(float(returns.std(ddof=0) * np.sqrt(252)))
    return _mean(vols)


def _regime_score(regime: str) -> float | None:
    return {
        "risk_off": 20.0,
        "shock_recovery": 45.0,
        "neutral": 50.0,
        "risk_on": 70.0,
        "growth_leadership": 80.0,
        "overheated": 65.0,
        AI_ATTRIBUTION_MARKET_REGIME: 60.0,
    }.get(regime or "unknown")


def _overlap_warning(
    corr: float | None,
    sample_count: int,
    min_sample_count: int,
) -> str:
    if sample_count < min_sample_count or corr is None:
        return "unknown_insufficient_data"
    absolute = abs(corr)
    if absolute >= REDUNDANCY_HIGH_CORRELATION:
        return "high_overlap"
    if absolute >= REDUNDANCY_MEDIUM_CORRELATION:
        return "medium_overlap"
    return "low_overlap"


def _redundancy_band(
    max_abs_correlation: float | None,
    sample_count: int,
    min_sample_count: int,
) -> str:
    if sample_count < min_sample_count or max_abs_correlation is None:
        return "unknown_insufficient_data"
    if max_abs_correlation >= REDUNDANCY_HIGH_CORRELATION:
        return "high"
    if max_abs_correlation >= REDUNDANCY_MEDIUM_CORRELATION:
        return "medium"
    return "low"


def _rank_correlation(
    left: Iterable[float | None],
    right: Iterable[float | None],
) -> float | None:
    pairs = [(x, y) for x, y in zip(left, right, strict=False) if x is not None and y is not None]
    if len(pairs) < 2:
        return None
    frame = pd.DataFrame(pairs, columns=["left", "right"])
    left_rank = frame["left"].rank(method="average").astype(float).tolist()
    right_rank = frame["right"].rank(method="average").astype(float).tolist()
    return _pearson_correlation(left_rank, right_rank)


def _pearson_correlation(
    left: Iterable[float | None],
    right: Iterable[float | None],
) -> float | None:
    pairs = [(x, y) for x, y in zip(left, right, strict=False) if x is not None and y is not None]
    if len(pairs) < 2:
        return None
    frame = pd.DataFrame(pairs, columns=["left", "right"])
    if frame["left"].nunique(dropna=True) < 2 or frame["right"].nunique(dropna=True) < 2:
        return None
    corr = frame["left"].corr(frame["right"])
    return None if pd.isna(corr) else float(corr)


def _drawdown_severity(value: object) -> float | None:
    parsed = _float_or_none(value)
    if parsed is None:
        return None
    return abs(min(parsed, 0.0))


def _mean(values: Iterable[Any]) -> float | None:
    clean = [float(value) for value in values if _float_or_none(value) is not None]
    if not clean:
        return None
    return float(np.mean(clean))


def _median(values: Iterable[Any]) -> float | None:
    clean = [float(value) for value in values if _float_or_none(value) is not None]
    if not clean:
        return None
    return float(np.median(clean))


def _min(values: Iterable[Any]) -> float | None:
    clean = [float(value) for value in values if _float_or_none(value) is not None]
    if not clean:
        return None
    return float(min(clean))


def _share(values: Iterable[bool]) -> float | None:
    clean = [bool(value) for value in values]
    if not clean:
        return None
    return float(sum(1 for value in clean if value) / len(clean))


def _safety_issues(payload: Mapping[str, Any], *, owner_id: str) -> list[str]:
    issues: list[str] = []
    safety = _mapping(payload.get("safety"))
    for field, expected in AI_ATTRIBUTION_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(f"{owner_id}:UNSAFE_{field.upper()}")
        if safety and safety.get(field) != expected:
            issues.append(f"{owner_id}.safety:UNSAFE_{field.upper()}")
    return issues


def _assert_safe_payload(payload: Mapping[str, Any], *, owner_id: str) -> None:
    issues = _safety_issues(payload, owner_id=owner_id)
    for key in FORBIDDEN_OUTPUT_KEYS:
        if key in payload:
            issues.append(f"{owner_id}:FORBIDDEN_OUTPUT_KEY:{key}")
    if issues:
        raise AIAttributionError(";".join(issues))


def _safety_banner() -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
    )


def _write_json(payload: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _stable_id(*parts: str) -> str:
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"{parts[0]}-{digest}"


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _join_list(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, list | tuple | set):
        values = [str(item) for item in value if str(item)]
    else:
        values = [str(value)] if str(value) else []
    return ", ".join(values) if values else "none"


def _fmt_number(value: object) -> str:
    parsed = _float_or_none(value)
    return "-" if parsed is None else f"{parsed:.2f}"
