from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.satellite import (
    SatelliteUniverseConfig,
    satellite_benchmark_mappings,
)

DEFAULT_SATELLITE_ATTRIBUTION_REPORT_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "satellite_attribution"
)
DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR = DEFAULT_SATELLITE_ATTRIBUTION_REPORT_ROOT / "datasets"
DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR = DEFAULT_SATELLITE_ATTRIBUTION_REPORT_ROOT / "reports"
DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR = (
    DEFAULT_SATELLITE_ATTRIBUTION_REPORT_ROOT / "validation"
)

SATELLITE_ATTRIBUTION_DATASET_SCHEMA_VERSION = "satellite_attribution_dataset_v1"
SATELLITE_ATTRIBUTION_REPORT_SCHEMA_VERSION = "satellite_attribution_report_v1"
SATELLITE_ATTRIBUTION_VALIDATION_SCHEMA_VERSION = "satellite_attribution_validation_v1"

SATELLITE_ATTRIBUTION_MARKET_REGIME = "ai_after_chatgpt"
SATELLITE_ATTRIBUTION_REGIME_START = date(2022, 12, 1)
FORWARD_WINDOWS: tuple[int, ...] = (1, 5, 20, 60)

SATELLITE_ATTRIBUTION_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

ELIGIBILITY_BUCKETS: tuple[str, ...] = (
    "eligible",
    "watch",
    "fallback_to_etf",
    "blocked",
    "insufficient_data",
)
SCORE_BUCKETS: tuple[tuple[str, float, float | None], ...] = (
    ("reject", 0.0, 30.0),
    ("weak", 30.0, 45.0),
    ("neutral", 45.0, 65.0),
    ("candidate", 65.0, 80.0),
    ("strong_candidate", 80.0, None),
)

# TRADING-073 pilot reporting floor only; it lowers confidence and never grants action rights.
MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT = 5
# TRADING-073 pilot report-level stock-vs-ETF alpha floor only.
SATELLITE_MEANINGFUL_ALPHA_THRESHOLD = 0.005
# TRADING-073 pilot volatility-delta warning boundary only.
SATELLITE_HIGH_RISK_VOLATILITY_DELTA = 0.05
# TRADING-073 pilot drawdown severity boundary only.
SATELLITE_DRAWDOWN_SEVERITY_THRESHOLD = 0.02
# TRADING-073 pilot AI high/low interaction buckets for reporting only.
SATELLITE_AI_HIGH_THRESHOLD = 65.0
SATELLITE_AI_LOW_THRESHOLD = 45.0

FORBIDDEN_OUTPUT_KEYS = {
    "production_weight_update",
    "candidate_auto_promotion",
    "broker_order",
    "baseline_config_mutation",
    "live_satellite_allocation",
    "target_weights",
    "production_weights",
}


class SatelliteAttributionError(ValueError):
    """Raised when a satellite attribution payload violates its audit contract."""


def load_satellite_replacement_report_payloads(
    report_dir: Path,
    *,
    as_of: date,
    start: date = SATELLITE_ATTRIBUTION_REGIME_START,
) -> list[dict[str, Any]]:
    if not report_dir.exists():
        return []
    reports: list[dict[str, Any]] = []
    for path in sorted(report_dir.glob("satellite_replacement_report_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        decision_date = _parse_date(payload.get("date"))
        if decision_date is None or decision_date < start or decision_date > as_of:
            continue
        payload = dict(payload)
        payload["source_report_path"] = str(path)
        reports.append(payload)
    return sorted(reports, key=lambda item: str(item.get("date")))


def load_ai_confirmation_report_payloads_for_satellite(
    report_dir: Path,
    *,
    as_of: date,
    start: date = SATELLITE_ATTRIBUTION_REGIME_START,
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


def build_satellite_attribution_dataset(
    *,
    satellite_reports: Iterable[Mapping[str, Any]],
    prices: pd.DataFrame,
    evaluation_as_of_date: date,
    universe_config: SatelliteUniverseConfig | None = None,
    ai_confirmation_reports: Iterable[Mapping[str, Any]] | None = None,
    start: date = SATELLITE_ATTRIBUTION_REGIME_START,
    forward_windows: Iterable[int] = FORWARD_WINDOWS,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    market_regime: str = SATELLITE_ATTRIBUTION_MARKET_REGIME,
    requested_date_range: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    price_frame = _prepare_price_frame(prices, evaluation_as_of_date)
    mappings = _universe_mapping(universe_config)
    ai_reports = list(ai_confirmation_reports or [])
    windows = tuple(int(window) for window in forward_windows)
    records: list[dict[str, Any]] = []
    source_reports: list[dict[str, Any]] = []

    for report in sorted(satellite_reports, key=lambda item: str(item.get("date"))):
        decision_date = _parse_date(report.get("date"))
        if decision_date is None or decision_date < start or decision_date > evaluation_as_of_date:
            continue
        source_path = _text(report.get("source_report_path"))
        source_reports.append(
            {
                "decision_date": decision_date.isoformat(),
                "source_report_path": source_path,
                "eligible_count": len(_records(report.get("eligible_stocks"))),
                "fallback_count": len(_records(report.get("fallback_to_etf_stocks"))),
            }
        )
        for eligibility in _records(report.get("replacement_eligibility")):
            ticker = _text(eligibility.get("ticker")).upper()
            if not ticker:
                continue
            for window in windows:
                records.append(
                    _dataset_record_for_satellite(
                        report=report,
                        eligibility=eligibility,
                        price_frame=price_frame,
                        decision_date=decision_date,
                        evaluation_as_of_date=evaluation_as_of_date,
                        forward_window=window,
                        market_regime=market_regime,
                        source_report_path=source_path,
                        mappings=mappings,
                        ai_context=_ai_context_for_date(
                            report=report,
                            ai_reports=ai_reports,
                            decision_date=decision_date,
                        ),
                    )
                )

    payload = {
        "schema_version": SATELLITE_ATTRIBUTION_DATASET_SCHEMA_VERSION,
        "report_type": "satellite_attribution_dataset",
        "attribution_dataset_id": _stable_id(
            "satellite-attribution-dataset",
            evaluation_as_of_date.isoformat(),
            str(len(records)),
        ),
        "task": "TRADING-073A",
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
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }
    validate_satellite_attribution_dataset(payload)
    return payload


def validate_satellite_attribution_dataset(payload: Mapping[str, Any]) -> None:
    issues: list[str] = []
    issues.extend(_safety_issues(payload, owner_id="dataset"))
    if payload.get("evaluation_only") is not True:
        issues.append("dataset:EVALUATION_ONLY_REQUIRED")
    _forbidden_key_issues(payload, "dataset", issues)
    records = _records(payload.get("records"))
    for index, record in enumerate(records):
        owner = f"dataset.records[{index}]"
        issues.extend(_safety_issues(record, owner_id=owner))
        _forbidden_key_issues(record, owner, issues)
        if record.get("evaluation_only") is not True:
            issues.append(f"{owner}:EVALUATION_ONLY_REQUIRED")
        for field in (
            "decision_date",
            "eligibility_date",
            "replacement_plan_date",
            "forward_window",
            "evaluation_as_of_date",
            "ticker",
            "benchmark_etf",
        ):
            if not _text(record.get(field)):
                issues.append(f"{owner}:{field.upper()}_REQUIRED")
    if issues:
        raise SatelliteAttributionError(";".join(issues))


def write_satellite_attribution_dataset(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
) -> dict[str, Path]:
    validate_satellite_attribution_dataset(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"satellite_attribution_dataset_{as_of}.json"
    csv_path = output_dir / f"satellite_attribution_dataset_{as_of}.csv"
    markdown_path = output_dir / f"satellite_attribution_dataset_{as_of}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    satellite_attribution_dataset_records_to_frame(payload).to_csv(csv_path, index=False)
    markdown_path.write_text(
        render_satellite_attribution_dataset_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}


def satellite_attribution_dataset_records_to_frame(
    payload: Mapping[str, Any],
) -> pd.DataFrame:
    return pd.DataFrame(_records(payload.get("records")))


def render_satellite_attribution_dataset_markdown(payload: Mapping[str, Any]) -> str:
    records = _records(payload.get("records"))
    lines = [
        "# Satellite Attribution Dataset",
        "",
        f"- 状态：dataset records={len(records)}",
        f"- as_of：{_text(payload.get('as_of_date'))}",
        f"- market_regime：`{_text(payload.get('market_regime'))}`",
        f"- data_quality：{_text(_mapping(payload.get('data_quality')).get('status'))}",
        "- safety：observe_only=true, candidate_only=true, "
        "production_effect=none, broker_action=none, manual_review_required=true",
        "",
        "| Decision Date | Ticker | Benchmark | Window | Status | Fallback | "
        "Stock-Benchmark | Replacement-ETF | Sample |",
        "|---|---|---|---|---|---|---:|---:|---|",
    ]
    for record in records[:80]:
        lines.append(
            "| "
            f"{record.get('decision_date')} | "
            f"{record.get('ticker')} | "
            f"{record.get('benchmark_etf')} | "
            f"{record.get('forward_window')} | "
            f"{record.get('eligibility_status')} | "
            f"{str(record.get('fallback_to_etf')).lower()} | "
            f"{_fmt_number(record.get('stock_minus_benchmark_forward_return'))} | "
            f"{_fmt_number(record.get('replacement_minus_ETF_forward_return'))} | "
            f"{record.get('sample_available')} |"
        )
    if len(records) > 80:
        lines.append(f"| ... | ... | ... | ... | ... | ... | ... | ... | +{len(records)-80} |")
    return "\n".join(lines) + "\n"


def build_eligibility_bucket_forward_return_analysis(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    rows: list[dict[str, Any]] = []
    for window in _forward_windows_from_dataset(dataset_payload):
        window_records = [record for record in records if record.get("forward_window") == window]
        for bucket in ELIGIBILITY_BUCKETS:
            bucket_records = [
                record for record in window_records if _eligibility_bucket(record) == bucket
            ]
            rows.append(
                {
                    "forward_window": window,
                    "eligibility_bucket": bucket,
                    **_satellite_forward_metric_summary(
                        bucket_records,
                        min_sample_count=min_sample_count,
                    ),
                }
            )
    return {
        "schema_version": "satellite_eligibility_bucket_analysis_v1",
        "report_type": "satellite_eligibility_bucket_analysis",
        "task": "TRADING-073B",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "buckets": rows,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_stock_vs_benchmark_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    rows: list[dict[str, Any]] = []
    for (ticker, benchmark), stock_records in sorted(
        _group_records(records, ("ticker", "benchmark_etf")).items()
    ):
        window_metrics = []
        for window in _forward_windows_from_dataset(dataset_payload):
            window_records = [
                record for record in stock_records if record.get("forward_window") == window
            ]
            window_metrics.append(
                {
                    "forward_window": window,
                    **_stock_metric_summary(
                        window_records,
                        min_sample_count=min_sample_count,
                    ),
                }
            )
        rows.append(
            {
                "ticker": ticker,
                "benchmark_etf": benchmark,
                "role": _most_common_text(stock_records, "role"),
                "group": _most_common_text(stock_records, "group"),
                **_stock_metric_summary(stock_records, min_sample_count=min_sample_count),
                "best_forward_window": _best_window(window_metrics),
                "worst_forward_window": _worst_window(window_metrics),
                "window_metrics": window_metrics,
            }
        )
    return {
        "schema_version": "satellite_stock_vs_benchmark_attribution_v1",
        "report_type": "satellite_stock_vs_benchmark_attribution",
        "task": "TRADING-073C",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "stocks": rows,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_fallback_to_etf_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = [
        record
        for record in _available_records(dataset_payload)
        if record.get("fallback_to_etf") is True
    ]
    by_window: list[dict[str, Any]] = []
    for window in _forward_windows_from_dataset(dataset_payload):
        window_records = [record for record in records if record.get("forward_window") == window]
        by_window.append(
            {
                "forward_window": window,
                **_fallback_metric_summary(
                    window_records,
                    min_sample_count=min_sample_count,
                ),
            }
        )
    return {
        "schema_version": "satellite_fallback_attribution_v1",
        "report_type": "satellite_fallback_attribution",
        "task": "TRADING-073D",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "fallback_sample_count": len(records),
        **_fallback_metric_summary(records, min_sample_count=min_sample_count),
        "by_window": by_window,
        "fallback_reason_breakdown": _reason_breakdown(records),
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_satellite_score_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    rows: list[dict[str, Any]] = []
    for window in _forward_windows_from_dataset(dataset_payload):
        window_records = [record for record in records if record.get("forward_window") == window]
        for bucket_id, _, _ in SCORE_BUCKETS:
            bucket_records = [
                record
                for record in window_records
                if satellite_score_bucket(_float_or_none(record.get("SatelliteCandidateScore")))
                == bucket_id
            ]
            rows.append(
                {
                    "forward_window": window,
                    "score_bucket": bucket_id,
                    **_score_bucket_metric_summary(
                        bucket_records,
                        min_sample_count=min_sample_count,
                    ),
                }
            )
    return {
        "schema_version": "satellite_score_attribution_v1",
        "report_type": "satellite_score_attribution",
        "task": "TRADING-073E",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "score_buckets": rows,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_satellite_risk_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    by_window = []
    for window in _forward_windows_from_dataset(dataset_payload):
        window_records = [record for record in records if record.get("forward_window") == window]
        by_window.append(
            {
                "forward_window": window,
                **_risk_metric_summary(
                    window_records,
                    min_sample_count=min_sample_count,
                ),
            }
        )
    overall = _risk_metric_summary(records, min_sample_count=min_sample_count)
    return {
        "schema_version": "satellite_risk_attribution_v1",
        "report_type": "satellite_risk_attribution",
        "task": "TRADING-073F",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        **overall,
        "by_window": by_window,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_role_group_level_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    role_metrics = _role_or_group_metrics(
        records,
        field="role",
        min_sample_count=min_sample_count,
    )
    group_metrics = _role_or_group_metrics(
        records,
        field="group",
        min_sample_count=min_sample_count,
    )
    return {
        "schema_version": "satellite_role_group_attribution_v1",
        "report_type": "satellite_role_group_attribution",
        "task": "TRADING-073G",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "role_metrics": role_metrics,
        "group_metrics": group_metrics,
        "best_role": _best_entity(role_metrics),
        "worst_role": _worst_entity(role_metrics),
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_ai_confirmation_interaction_attribution(
    dataset_payload: Mapping[str, Any],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    records = _available_records(dataset_payload)
    dimensions = {
        "AIConfirmationScore": "AIConfirmationScore",
        "SemiconductorBreadthScore": "SemiconductorBreadthScore",
        "MegaCapAIScore": "MegaCapAIScore",
        "EventRiskScore": "EventRiskScore",
    }
    interactions: list[dict[str, Any]] = []
    for dimension, field in dimensions.items():
        for bucket in ("low", "neutral", "high", "missing"):
            bucket_records = [
                record
                for record in records
                if _ai_interaction_bucket(_float_or_none(record.get(field))) == bucket
            ]
            interactions.append(
                {
                    "dimension": dimension,
                    "ai_bucket": bucket,
                    **_interaction_metric_summary(
                        bucket_records,
                        min_sample_count=min_sample_count,
                    ),
                }
            )
    return {
        "schema_version": "satellite_ai_interaction_attribution_v1",
        "report_type": "satellite_ai_interaction_attribution",
        "task": "TRADING-073H",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "interactions": interactions,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def build_satellite_attribution_evidence_scorecard(
    *,
    dataset_payload: Mapping[str, Any],
    eligibility_bucket_analysis: Mapping[str, Any] | None = None,
    stock_vs_benchmark_attribution: Mapping[str, Any] | None = None,
    fallback_attribution: Mapping[str, Any] | None = None,
    score_attribution: Mapping[str, Any] | None = None,
    risk_attribution: Mapping[str, Any] | None = None,
    role_group_attribution: Mapping[str, Any] | None = None,
    ai_interaction_attribution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    eligibility_bucket_analysis = eligibility_bucket_analysis or (
        build_eligibility_bucket_forward_return_analysis(dataset_payload)
    )
    stock_vs_benchmark_attribution = stock_vs_benchmark_attribution or (
        build_stock_vs_benchmark_attribution(dataset_payload)
    )
    fallback_attribution = fallback_attribution or build_fallback_to_etf_attribution(
        dataset_payload
    )
    score_attribution = score_attribution or build_satellite_score_attribution(dataset_payload)
    risk_attribution = risk_attribution or build_satellite_risk_attribution(dataset_payload)
    role_group_attribution = role_group_attribution or build_role_group_level_attribution(
        dataset_payload
    )
    ai_interaction_attribution = ai_interaction_attribution or (
        build_ai_confirmation_interaction_attribution(dataset_payload)
    )
    records = _available_records(dataset_payload)
    eligible_records = [record for record in records if _eligibility_bucket(record) == "eligible"]
    fallback_records = [record for record in records if record.get("fallback_to_etf") is True]
    data_quality = _text(_mapping(dataset_payload.get("data_quality")).get("status"), "UNKNOWN")
    eligible_alpha = _mean(
        _float_or_none(record.get("stock_minus_benchmark_forward_return"))
        for record in eligible_records
    )
    fallback_saved_loss_rate = _fallback_metric_summary(fallback_records)[
        "fallback_saved_loss_rate"
    ]
    risk_adjusted_alpha = _float_or_none(risk_attribution.get("risk_adjusted_alpha"))
    score_lift = _score_bucket_lift(score_attribution)
    risk_status = _risk_status(risk_attribution)
    dimension_scores = {
        "eligible_outperformance_evidence": _evidence_status(eligible_alpha),
        "fallback_protection_evidence": _rate_status(fallback_saved_loss_rate),
        "score_ranking_evidence": _evidence_status(score_lift),
        "risk_adjusted_evidence": _evidence_status(risk_adjusted_alpha),
        "role_group_evidence": (
            "mixed"
            if role_group_attribution.get("best_role") != role_group_attribution.get("worst_role")
            else "weak"
        ),
        "AI_interaction_evidence": _ai_interaction_status(ai_interaction_attribution),
        "sample_quality": (
            "sufficient"
            if len(records) >= MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT
            else "insufficient"
        ),
        "data_coverage": data_quality,
    }
    overall_status = _overall_scorecard_status(
        data_quality=data_quality,
        sample_count=len(records),
        eligible_alpha=eligible_alpha,
        fallback_saved_loss_rate=fallback_saved_loss_rate,
        risk_status=risk_status,
        risk_adjusted_alpha=risk_adjusted_alpha,
    )
    payload = {
        "schema_version": "satellite_attribution_evidence_scorecard_v1",
        "report_type": "satellite_attribution_evidence_scorecard",
        "scorecard_id": _stable_id(
            "satellite-attribution-scorecard",
            _text(dataset_payload.get("as_of_date")),
            str(len(records)),
        ),
        "task": "TRADING-073I",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "overall_status": overall_status,
        "dimension_scores": dimension_scores,
        "supporting_evidence": _supporting_evidence(
            eligible_alpha=eligible_alpha,
            fallback_saved_loss_rate=fallback_saved_loss_rate,
            score_lift=score_lift,
            risk_adjusted_alpha=risk_adjusted_alpha,
        ),
        "blocking_evidence": _blocking_evidence(
            data_quality=data_quality,
            sample_count=len(records),
            risk_status=risk_status,
        ),
        "sample_summary": {
            "record_count": len(_records(dataset_payload.get("records"))),
            "available_sample_count": len(records),
            "eligible_sample_count": len(eligible_records),
            "fallback_sample_count": len(fallback_records),
        },
        "manual_review_recommendation": _manual_review_recommendation(overall_status),
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }
    _raise_if_unsafe_payload(payload, "scorecard")
    return payload


def build_satellite_attribution_report(
    dataset_payload: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_satellite_attribution_dataset(dataset_payload)
    generated_at = generated_at or datetime.now(tz=UTC)
    eligibility = build_eligibility_bucket_forward_return_analysis(dataset_payload)
    stock = build_stock_vs_benchmark_attribution(dataset_payload)
    fallback = build_fallback_to_etf_attribution(dataset_payload)
    score = build_satellite_score_attribution(dataset_payload)
    risk = build_satellite_risk_attribution(dataset_payload)
    role = build_role_group_level_attribution(dataset_payload)
    ai = build_ai_confirmation_interaction_attribution(dataset_payload)
    scorecard = build_satellite_attribution_evidence_scorecard(
        dataset_payload=dataset_payload,
        eligibility_bucket_analysis=eligibility,
        stock_vs_benchmark_attribution=stock,
        fallback_attribution=fallback,
        score_attribution=score,
        risk_attribution=risk,
        role_group_attribution=role,
        ai_interaction_attribution=ai,
    )
    payload = {
        "schema_version": SATELLITE_ATTRIBUTION_REPORT_SCHEMA_VERSION,
        "report_type": "satellite_attribution_report",
        "report_id": _stable_id(
            "satellite-attribution-report",
            _text(dataset_payload.get("as_of_date")),
            str(dataset_payload.get("record_count", 0)),
        ),
        "task": "TRADING-073J",
        "as_of_date": _text(dataset_payload.get("as_of_date")),
        "generated_at": generated_at.isoformat(),
        "market_regime": _text(dataset_payload.get("market_regime")),
        "requested_date_range": dict(_mapping(dataset_payload.get("requested_date_range"))),
        "safety_banner": _safety_banner(),
        "data_quality": dict(_mapping(dataset_payload.get("data_quality"))),
        "dataset_coverage": {
            "record_count": dataset_payload.get("record_count", 0),
            "available_sample_count": dataset_payload.get("available_sample_count", 0),
            "forward_windows": list(dataset_payload.get("forward_windows", [])),
            "source_report_count": len(_records(dataset_payload.get("source_reports"))),
        },
        "dataset": dict(dataset_payload),
        "eligibility_bucket_analysis": eligibility,
        "stock_vs_benchmark_attribution": stock,
        "fallback_attribution": fallback,
        "score_attribution": score,
        "risk_attribution": risk,
        "role_group_attribution": role,
        "AI_interaction_attribution": ai,
        "evidence_scorecard": scorecard,
        "source_links": _source_links(dataset_payload),
        "manual_review_recommendation": scorecard.get("manual_review_recommendation"),
        "production_weights_mutated": False,
        "evaluation_only": True,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }
    validate_satellite_attribution_report(payload)
    return payload


def validate_satellite_attribution_report(payload: Mapping[str, Any]) -> None:
    issues: list[str] = []
    issues.extend(_safety_issues(payload, owner_id="report"))
    _forbidden_key_issues(payload, "report", issues)
    if payload.get("evaluation_only") is not True:
        issues.append("report:EVALUATION_ONLY_REQUIRED")
    if payload.get("production_weights_mutated") is not False:
        issues.append("report:PRODUCTION_WEIGHTS_MUTATED_MUST_BE_FALSE")
    dataset = _mapping(payload.get("dataset"))
    if dataset:
        try:
            validate_satellite_attribution_dataset(dataset)
        except SatelliteAttributionError as exc:
            issues.append(f"report:DATASET_INVALID:{exc}")
    else:
        issues.append("report:DATASET_REQUIRED")
    for section in (
        "eligibility_bucket_analysis",
        "stock_vs_benchmark_attribution",
        "fallback_attribution",
        "score_attribution",
        "risk_attribution",
        "role_group_attribution",
        "AI_interaction_attribution",
        "evidence_scorecard",
    ):
        if not _mapping(payload.get(section)):
            issues.append(f"report:{section.upper()}_REQUIRED")
    if issues:
        raise SatelliteAttributionError(";".join(issues))


def write_satellite_attribution_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR,
) -> dict[str, Path]:
    validate_satellite_attribution_report(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"satellite_attribution_report_{as_of}.json"
    markdown_path = output_dir / f"satellite_attribution_report_{as_of}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_satellite_attribution_report_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_satellite_attribution_report_markdown(payload: Mapping[str, Any]) -> str:
    scorecard = _mapping(payload.get("evidence_scorecard"))
    coverage = _mapping(payload.get("dataset_coverage"))
    fallback = _mapping(payload.get("fallback_attribution"))
    risk = _mapping(payload.get("risk_attribution"))
    role = _mapping(payload.get("role_group_attribution"))
    stock_rows = _records(_mapping(payload.get("stock_vs_benchmark_attribution")).get("stocks"))
    lines = [
        "# Satellite Replacement Forward Attribution Review",
        "",
        f"- 状态：{scorecard.get('overall_status')}",
        f"- as_of：{payload.get('as_of_date')}",
        f"- market_regime：`{payload.get('market_regime')}`",
        f"- record_count：{coverage.get('record_count', 0)}",
        f"- available_sample_count：{coverage.get('available_sample_count', 0)}",
        "- safety：observe_only=true, candidate_only=true, "
        "production_effect=none, broker_action=none, manual_review_required=true",
        "- forward returns：evaluation_only=true，仅用于 attribution/evaluation",
        "",
        "## Evidence Scorecard",
        "",
        f"- overall_status：`{scorecard.get('overall_status')}`",
        f"- manual_review_recommendation：{scorecard.get('manual_review_recommendation')}",
        f"- supporting_evidence：{_join_list(scorecard.get('supporting_evidence'))}",
        f"- blocking_evidence：{_join_list(scorecard.get('blocking_evidence'))}",
        "",
        "## Fallback-To-ETF",
        "",
        f"- fallback_sample_count：{fallback.get('fallback_sample_count', 0)}",
        f"- fallback_saved_loss_rate：{_fmt_number(fallback.get('fallback_saved_loss_rate'))}",
        f"- fallback_missed_gain_rate：{_fmt_number(fallback.get('fallback_missed_gain_rate'))}",
        f"- mean_saved_drawdown：{_fmt_number(fallback.get('mean_saved_drawdown'))}",
        f"- mean_missed_upside：{_fmt_number(fallback.get('mean_missed_upside'))}",
        "",
        "## Risk Attribution",
        "",
        f"- risk_adjusted_alpha：{_fmt_number(risk.get('risk_adjusted_alpha'))}",
        f"- high_volatility_failure_rate："
        f"{_fmt_number(risk.get('high_volatility_failure_rate'))}",
        f"- drawdown_saved_by_fallback：" f"{_fmt_number(risk.get('drawdown_saved_by_fallback'))}",
        f"- drawdown_added_by_eligible_replacement："
        f"{_fmt_number(risk.get('drawdown_added_by_eligible_replacement'))}",
        "",
        "## Role / Group Evidence",
        "",
        f"- best_role：{role.get('best_role')}",
        f"- worst_role：{role.get('worst_role')}",
        "",
        "## Stock Vs Benchmark ETF",
        "",
        "| Ticker | Benchmark | Role | Group | Sample | Mean Alpha | Hit Rate | Fallback Miss |",
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for row in stock_rows[:25]:
        lines.append(
            "| "
            f"{row.get('ticker')} | "
            f"{row.get('benchmark_etf')} | "
            f"{row.get('role')} | "
            f"{row.get('group')} | "
            f"{row.get('sample_count')} | "
            f"{_fmt_number(row.get('mean_stock_minus_benchmark'))} | "
            f"{_fmt_number(row.get('hit_rate_outperformance'))} | "
            f"{_fmt_number(row.get('fallback_miss_rate'))} |"
        )
    lines.extend(
        [
            "",
            "## Source Links",
            "",
        ]
    )
    for link in _records(payload.get("source_links"))[:50]:
        lines.append(f"- `{link.get('source_report_path')}`")
    return "\n".join(lines) + "\n"


def build_satellite_attribution_validation_report(
    *,
    report_registry: Mapping[str, Any] | None = None,
    reader_brief_available: bool = False,
    report_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(tz=UTC)
    checks: list[dict[str, Any]] = []
    checks.append(
        _validation_check(
            "dataset_builder_available",
            callable(build_satellite_attribution_dataset),
        )
    )
    checks.append(
        _validation_check(
            "eligibility_bucket_analysis_available",
            callable(build_eligibility_bucket_forward_return_analysis),
        )
    )
    checks.append(
        _validation_check(
            "stock_vs_benchmark_attribution_available",
            callable(build_stock_vs_benchmark_attribution),
        )
    )
    checks.append(
        _validation_check(
            "fallback_attribution_available",
            callable(build_fallback_to_etf_attribution),
        )
    )
    checks.append(
        _validation_check(
            "score_attribution_available",
            callable(build_satellite_score_attribution),
        )
    )
    checks.append(
        _validation_check("risk_attribution_available", callable(build_satellite_risk_attribution))
    )
    checks.append(
        _validation_check(
            "role_group_attribution_available",
            callable(build_role_group_level_attribution),
        )
    )
    checks.append(
        _validation_check(
            "AI_interaction_attribution_available",
            callable(build_ai_confirmation_interaction_attribution),
        )
    )
    checks.append(
        _validation_check(
            "evidence_scorecard_available",
            callable(build_satellite_attribution_evidence_scorecard),
        )
    )
    checks.append(
        _validation_check(
            "report_generator_available",
            callable(build_satellite_attribution_report),
        )
    )
    checks.append(_validation_check("reader_brief_integration_available", reader_brief_available))
    checks.append(
        _validation_check(
            "report_registry_integration_available",
            _report_registry_has_satellite_attribution(report_registry),
        )
    )

    sample_dataset = _sample_dataset()
    sample_report = build_satellite_attribution_report(
        sample_dataset,
        generated_at=generated_at,
    )
    checks.append(
        _validation_check(
            "forward_returns_evaluation_only",
            sample_dataset.get("evaluation_only") is True
            and all(row.get("evaluation_only") is True for row in sample_dataset["records"]),
        )
    )
    checks.append(
        _validation_check(
            "production_effect_none",
            sample_report.get("production_effect") == "none"
            and sample_report.get("production_weights_mutated") is False,
        )
    )
    checks.append(
        _validation_check("broker_action_none", sample_report.get("broker_action") == "none")
    )
    checks.append(
        _validation_check(
            "manual_review_required",
            sample_report.get("manual_review_required") is True,
        )
    )
    checks.append(
        _validation_check(
            "forbidden_output_keys_absent",
            not _contains_forbidden_output_key(sample_report),
        )
    )
    if report_payload:
        try:
            validate_satellite_attribution_report(report_payload)
            checks.append(_validation_check("provided_report_valid", True))
        except SatelliteAttributionError as exc:
            checks.append(_validation_check("provided_report_valid", False, [str(exc)]))
    failed = [check for check in checks if check.get("status") != "PASS"]
    payload = {
        "schema_version": SATELLITE_ATTRIBUTION_VALIDATION_SCHEMA_VERSION,
        "report_type": "satellite_attribution_validation",
        "task": "TRADING-073L",
        "generated_at": generated_at.isoformat(),
        "as_of_date": generated_at.date().isoformat(),
        "status": "FAIL" if failed else "PASS",
        "failed_check_count": len(failed),
        "checks": checks,
        "production_weights_mutated": False,
        "evaluation_only": True,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }
    validate_satellite_attribution_validation_report(payload)
    return payload


def validate_satellite_attribution_validation_report(payload: Mapping[str, Any]) -> None:
    issues = _safety_issues(payload, owner_id="validation")
    if payload.get("evaluation_only") is not True:
        issues.append("validation:EVALUATION_ONLY_REQUIRED")
    if payload.get("production_weights_mutated") is not False:
        issues.append("validation:PRODUCTION_WEIGHTS_MUTATED_MUST_BE_FALSE")
    _forbidden_key_issues(payload, "validation", issues)
    if issues:
        raise SatelliteAttributionError(";".join(issues))


def write_satellite_attribution_validation_report(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR,
) -> dict[str, Path]:
    validate_satellite_attribution_validation_report(payload)
    as_of = _text(payload.get("as_of_date"), date.today().isoformat())
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"satellite_attribution_validation_{as_of}.json"
    markdown_path = output_dir / f"satellite_attribution_validation_{as_of}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_satellite_attribution_validation_markdown(payload),
        encoding="utf-8",
    )
    return {"json": json_path, "markdown": markdown_path}


def render_satellite_attribution_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Satellite Attribution Validation Gate",
        "",
        f"- status：{payload.get('status')}",
        f"- failed_check_count：{payload.get('failed_check_count')}",
        "- safety：observe_only=true, candidate_only=true, "
        "production_effect=none, broker_action=none, manual_review_required=true",
        "",
        "| Check | Status | Blockers |",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "| "
            f"{check.get('check_id')} | "
            f"{check.get('status')} | "
            f"{_join_list(check.get('blockers'))} |"
        )
    return "\n".join(lines) + "\n"


def satellite_score_bucket(score: float | None) -> str:
    if score is None:
        return "unknown"
    for bucket_id, lower, upper in SCORE_BUCKETS:
        if score >= lower and (upper is None or score < upper):
            return bucket_id
    return "reject"


def build_satellite_attribution_report_from_sources(
    *,
    satellite_reports: Iterable[Mapping[str, Any]],
    prices: pd.DataFrame,
    evaluation_as_of_date: date,
    universe_config: SatelliteUniverseConfig | None = None,
    ai_confirmation_reports: Iterable[Mapping[str, Any]] | None = None,
    data_quality_status: str = "UNKNOWN",
    data_quality_report: str = "",
    market_regime: str = SATELLITE_ATTRIBUTION_MARKET_REGIME,
) -> dict[str, Any]:
    dataset = build_satellite_attribution_dataset(
        satellite_reports=satellite_reports,
        prices=prices,
        evaluation_as_of_date=evaluation_as_of_date,
        universe_config=universe_config,
        ai_confirmation_reports=ai_confirmation_reports,
        data_quality_status=data_quality_status,
        data_quality_report=data_quality_report,
        market_regime=market_regime,
    )
    return build_satellite_attribution_report(dataset)


def _dataset_record_for_satellite(
    *,
    report: Mapping[str, Any],
    eligibility: Mapping[str, Any],
    price_frame: pd.DataFrame,
    decision_date: date,
    evaluation_as_of_date: date,
    forward_window: int,
    market_regime: str,
    source_report_path: str,
    mappings: Mapping[str, Mapping[str, Any]],
    ai_context: Mapping[str, Any],
) -> dict[str, Any]:
    ticker = _text(eligibility.get("ticker")).upper()
    score = _by_ticker(_records(report.get("satellite_candidate_scores")), ticker)
    feature = _by_ticker(_records(report.get("stock_vs_etf_features")), ticker)
    plan = _mapping(report.get("replacement_plan"))
    allocation = _by_ticker(_records(plan.get("satellite_allocations")), ticker)
    fallback = _by_ticker(_records(plan.get("fallback_positions")), ticker)
    mapping = _mapping(mappings.get(ticker))
    benchmark = _text(
        eligibility.get("benchmark_etf"),
        _text(score.get("benchmark_etf"), _text(feature.get("benchmark_etf"))),
    ).upper()
    stock_path = _symbol_forward_path(
        price_frame,
        ticker,
        score_date=decision_date,
        evaluation_as_of_date=evaluation_as_of_date,
        window=forward_window,
    )
    benchmark_path = _symbol_forward_path(
        price_frame,
        benchmark,
        score_date=decision_date,
        evaluation_as_of_date=evaluation_as_of_date,
        window=forward_window,
    )
    stock_return = stock_path["return"]
    benchmark_return = benchmark_path["return"]
    stock_minus_benchmark = (
        None
        if stock_return is None or benchmark_return is None
        else stock_return - benchmark_return
    )
    replacement_weight = _float_or_none(allocation.get("allocation")) or 0.0
    fallback_to_etf = (
        eligibility.get("fallback_to_etf") is True
        or bool(fallback)
        or _text(eligibility.get("status")) != "eligible"
        or replacement_weight <= 0.0
    )
    replacement_plan_return = benchmark_return if fallback_to_etf else stock_return
    etf_only_return = benchmark_return
    replacement_minus_etf = (
        None
        if stock_minus_benchmark is None
        else (0.0 if fallback_to_etf else stock_minus_benchmark * replacement_weight)
    )
    stock_drawdown = _path_drawdown(stock_path["prices"])
    benchmark_drawdown = _path_drawdown(benchmark_path["prices"])
    stock_vol = _path_volatility(stock_path["prices"])
    benchmark_vol = _path_volatility(benchmark_path["prices"])
    component_scores = _mapping(score.get("component_scores"))
    event_risk_score = _float_or_none(score.get("event_risk_score"))
    if event_risk_score is None:
        event_risk_score = _float_or_none(ai_context.get("EventRiskScore"))
    missing = []
    if stock_return is None:
        missing.append(f"{ticker}:{stock_path['reason']}")
    if benchmark_return is None:
        missing.append(f"{benchmark}:{benchmark_path['reason']}")
    sample_available = not missing
    blockers = _strings(eligibility.get("blockers"))
    reason_codes = sorted(
        set(
            blockers
            + _strings(eligibility.get("reason_codes"))
            + _strings(fallback.get("reason_codes"))
        )
    )
    return {
        "record_id": _stable_id(
            "satellite-attribution-record",
            decision_date.isoformat(),
            ticker,
            str(forward_window),
            source_report_path,
        ),
        "decision_date": decision_date.isoformat(),
        "eligibility_date": _text(eligibility.get("date"), decision_date.isoformat()),
        "replacement_plan_date": _text(plan.get("date"), decision_date.isoformat()),
        "evaluation_as_of_date": evaluation_as_of_date.isoformat(),
        "forward_window": f"{forward_window}D",
        "forward_window_days": forward_window,
        "forward_window_end_date": _latest_end_date(stock_path, benchmark_path),
        "ticker": ticker,
        "benchmark_etf": benchmark,
        "sleeve": _text(eligibility.get("sleeve"), _text(mapping.get("sleeve"), "unknown")),
        "role": _text(eligibility.get("role"), _text(mapping.get("role"), "unknown")),
        "group": _text(mapping.get("group"), _text(score.get("group"), "unknown")),
        "eligibility_status": _text(eligibility.get("status"), "unknown"),
        "SatelliteCandidateScore": _float_or_none(
            score.get("score_value", eligibility.get("score_value"))
        ),
        "score_band": _text(score.get("score_band"), _text(eligibility.get("score_band"))),
        "relative_strength_score": _float_or_none(component_scores.get("relative_strength_score")),
        "trend_score": _float_or_none(component_scores.get("trend_score")),
        "drawdown_risk_score": _float_or_none(component_scores.get("drawdown_risk_score")),
        "event_risk_penalty": _event_risk_penalty(component_scores, event_risk_score),
        "ai_confirmation_support_score": _float_or_none(
            component_scores.get("ai_confirmation_support_score")
        ),
        "AIConfirmationScore": _float_or_none(ai_context.get("AIConfirmationScore")),
        "SemiconductorBreadthScore": _float_or_none(ai_context.get("SemiconductorBreadthScore")),
        "MegaCapAIScore": _float_or_none(ai_context.get("MegaCapAIScore")),
        "EventRiskScore": event_risk_score,
        "replacement_weight": replacement_weight,
        "fallback_to_etf": fallback_to_etf,
        "blockers": blockers,
        "reason_codes": reason_codes or ["none"],
        "stock_forward_return": stock_return,
        "benchmark_forward_return": benchmark_return,
        "stock_minus_benchmark_forward_return": stock_minus_benchmark,
        "replacement_plan_forward_return": replacement_plan_return,
        "ETF_only_forward_return": etf_only_return,
        "replacement_minus_ETF_forward_return": replacement_minus_etf,
        "forward_drawdown": stock_drawdown,
        "benchmark_forward_drawdown": benchmark_drawdown,
        "drawdown_delta_vs_benchmark": (
            None
            if stock_drawdown is None or benchmark_drawdown is None
            else stock_drawdown - benchmark_drawdown
        ),
        "forward_volatility": stock_vol,
        "benchmark_forward_volatility": benchmark_vol,
        "volatility_delta_vs_benchmark": (
            None if stock_vol is None or benchmark_vol is None else stock_vol - benchmark_vol
        ),
        "replacement_drawdown_delta_vs_ETF": (
            None
            if stock_drawdown is None or benchmark_drawdown is None
            else (0.0 if fallback_to_etf else stock_drawdown - benchmark_drawdown)
        ),
        "replacement_volatility_delta_vs_ETF": (
            None
            if stock_vol is None or benchmark_vol is None
            else (0.0 if fallback_to_etf else stock_vol - benchmark_vol)
        ),
        "regime": _text(report.get("market_regime"), market_regime) or "unknown",
        "event_window_flag": _event_window_flag(event_risk_score, reason_codes),
        "sample_available": sample_available,
        "insufficient_data_reason": "none" if sample_available else ";".join(missing),
        "source_report_path": source_report_path,
        "evaluation_only": True,
        "safety": dict(SATELLITE_ATTRIBUTION_SAFETY),
        **SATELLITE_ATTRIBUTION_SAFETY,
    }


def _prepare_price_frame(prices: pd.DataFrame, evaluation_as_of_date: date) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["_date", "symbol", "_adj_close"])
    frame = prices.copy()
    if "symbol" not in frame.columns or "date" not in frame.columns:
        raise SatelliteAttributionError("prices must contain date and symbol columns")
    price_column = "adj_close" if "adj_close" in frame.columns else "close"
    if price_column not in frame.columns:
        raise SatelliteAttributionError("prices must contain adj_close or close column")
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
        return _missing_forward_result("missing_decision_date_price")
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


def _path_drawdown(prices: pd.Series) -> float | None:
    clean = pd.to_numeric(prices, errors="coerce").dropna().astype(float)
    if len(clean) < 2:
        return None
    running_max = clean.cummax()
    drawdowns = clean / running_max - 1.0
    return float(drawdowns.min())


def _path_volatility(prices: pd.Series) -> float | None:
    clean = pd.to_numeric(prices, errors="coerce").dropna().astype(float)
    if len(clean) < 3:
        return None
    returns = clean.pct_change().dropna()
    if returns.empty:
        return None
    return float(returns.std(ddof=0) * np.sqrt(252.0))


def _satellite_forward_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    return {
        "sample_count": len(records),
        "mean_stock_forward_return": _mean(
            _float_or_none(row.get("stock_forward_return")) for row in records
        ),
        "median_stock_forward_return": _median(
            _float_or_none(row.get("stock_forward_return")) for row in records
        ),
        "mean_benchmark_forward_return": _mean(
            _float_or_none(row.get("benchmark_forward_return")) for row in records
        ),
        "mean_stock_minus_benchmark": _mean(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in records
        ),
        "hit_rate_stock_outperforms_benchmark": _share(
            (_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0
            for row in records
        ),
        "mean_replacement_minus_ETF": _mean(
            _float_or_none(row.get("replacement_minus_ETF_forward_return")) for row in records
        ),
        "mean_forward_drawdown": _mean(
            _float_or_none(row.get("forward_drawdown")) for row in records
        ),
        "mean_forward_volatility": _mean(
            _float_or_none(row.get("forward_volatility")) for row in records
        ),
        "event_window_ratio": _share(row.get("event_window_flag") is True for row in records),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _stock_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    return {
        "sample_count": len(records),
        "mean_stock_minus_benchmark": _mean(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in records
        ),
        "median_stock_minus_benchmark": _median(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in records
        ),
        "hit_rate_outperformance": _share(
            (_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0
            for row in records
        ),
        "mean_drawdown_delta_vs_benchmark": _mean(
            _float_or_none(row.get("drawdown_delta_vs_benchmark")) for row in records
        ),
        "mean_volatility_delta_vs_benchmark": _mean(
            _float_or_none(row.get("volatility_delta_vs_benchmark")) for row in records
        ),
        "eligibility_success_rate": _eligibility_success_rate(records),
        "fallback_miss_rate": _fallback_miss_rate(records),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _fallback_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int = MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT,
) -> dict[str, Any]:
    saved_loss = [
        record
        for record in records
        if (_float_or_none(record.get("stock_minus_benchmark_forward_return")) or 0.0) < 0.0
    ]
    missed_gain = [
        record
        for record in records
        if (_float_or_none(record.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0
    ]
    return {
        "fallback_sample_count": len(records),
        "fallback_stock_minus_benchmark_forward": _mean(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in records
        ),
        "fallback_saved_loss_rate": _share(record in saved_loss for record in records),
        "fallback_missed_gain_rate": _share(record in missed_gain for record in records),
        "mean_saved_drawdown": _mean(_saved_drawdown(row) for row in saved_loss),
        "mean_missed_upside": _mean(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in missed_gain
        ),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _score_bucket_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    return {
        "sample_count": len(records),
        "mean_stock_minus_benchmark": _mean(
            _float_or_none(row.get("stock_minus_benchmark_forward_return")) for row in records
        ),
        "hit_rate_outperformance": _share(
            (_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0
            for row in records
        ),
        "mean_replacement_minus_ETF": _mean(
            _float_or_none(row.get("replacement_minus_ETF_forward_return")) for row in records
        ),
        "mean_drawdown_delta": _mean(
            _float_or_none(row.get("drawdown_delta_vs_benchmark")) for row in records
        ),
        "mean_volatility_delta": _mean(
            _float_or_none(row.get("volatility_delta_vs_benchmark")) for row in records
        ),
        "event_window_ratio": _share(row.get("event_window_flag") is True for row in records),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _risk_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    mean_alpha = _mean(
        _float_or_none(row.get("replacement_minus_ETF_forward_return")) for row in records
    )
    mean_vol_delta = _mean(
        _float_or_none(row.get("replacement_volatility_delta_vs_ETF")) for row in records
    )
    positive_vol_penalty = max(0.0, mean_vol_delta or 0.0)
    return {
        "sample_count": len(records),
        "replacement_drawdown_delta_vs_ETF": _mean(
            _float_or_none(row.get("replacement_drawdown_delta_vs_ETF")) for row in records
        ),
        "replacement_volatility_delta_vs_ETF": mean_vol_delta,
        "single_name_risk_contribution": _mean(
            abs(_float_or_none(row.get("replacement_weight")) or 0.0)
            * max(0.0, _float_or_none(row.get("volatility_delta_vs_benchmark")) or 0.0)
            for row in records
        ),
        "event_window_drawdown": _mean(
            _float_or_none(row.get("forward_drawdown"))
            for row in records
            if row.get("event_window_flag") is True
        ),
        "high_volatility_failure_rate": _share(
            (
                (_float_or_none(row.get("volatility_delta_vs_benchmark")) or 0.0)
                > SATELLITE_HIGH_RISK_VOLATILITY_DELTA
            )
            and ((_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) < 0.0)
            for row in records
        ),
        "risk_adjusted_alpha": (
            None if mean_alpha is None else float(mean_alpha - positive_vol_penalty)
        ),
        "drawdown_saved_by_fallback": _mean(
            _saved_drawdown(row) for row in records if row.get("fallback_to_etf") is True
        ),
        "drawdown_added_by_eligible_replacement": _mean(
            _added_drawdown(row) for row in records if _eligibility_bucket(row) == "eligible"
        ),
        "confidence_warning": "insufficient_sample" if len(records) < min_sample_count else "none",
    }


def _role_or_group_metrics(
    records: list[dict[str, Any]],
    *,
    field: str,
    min_sample_count: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (name,), grouped in sorted(_group_records(records, (field,)).items()):
        summary = _stock_metric_summary(grouped, min_sample_count=min_sample_count)
        rows.append(
            {
                field: name or "unknown",
                **summary,
                "fallback_saved_loss_rate": _fallback_metric_summary(
                    [row for row in grouped if row.get("fallback_to_etf") is True],
                    min_sample_count=min_sample_count,
                )["fallback_saved_loss_rate"],
                "fallback_missed_gain_rate": _fallback_miss_rate(grouped),
            }
        )
    return rows


def _interaction_metric_summary(
    records: list[dict[str, Any]],
    *,
    min_sample_count: int,
) -> dict[str, Any]:
    fallback_records = [row for row in records if row.get("fallback_to_etf") is True]
    return {
        "sample_count": len(records),
        "eligible_stock_outperformance_by_ai_bucket": _share(
            (_eligibility_bucket(row) == "eligible")
            and ((_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0)
            for row in records
        ),
        "fallback_saved_loss_by_ai_bucket": _fallback_metric_summary(
            fallback_records,
            min_sample_count=min_sample_count,
        )["fallback_saved_loss_rate"],
        "replacement_minus_ETF_by_ai_bucket": _mean(
            _float_or_none(row.get("replacement_minus_ETF_forward_return")) for row in records
        ),
        "drawdown_delta_by_ai_bucket": _mean(
            _float_or_none(row.get("drawdown_delta_vs_benchmark")) for row in records
        ),
        "interaction_confidence_warning": (
            "insufficient_sample" if len(records) < min_sample_count else "none"
        ),
    }


def _universe_mapping(
    universe_config: SatelliteUniverseConfig | None,
) -> dict[str, dict[str, Any]]:
    if universe_config is None:
        return {}
    return {
        str(item.get("stock_ticker", "")).upper(): item
        for item in satellite_benchmark_mappings(universe_config)
    }


def _ai_context_for_date(
    *,
    report: Mapping[str, Any],
    ai_reports: Sequence[Mapping[str, Any]],
    decision_date: date,
) -> dict[str, Any]:
    direct = _mapping(report.get("ai_confirmation_context"))
    selected = _latest_report_on_or_before(ai_reports, decision_date)
    score = _mapping(selected.get("AIConfirmationScore")) if selected else {}
    components = _mapping(score.get("component_scores"))
    event = _mapping(selected.get("event_risk_overlay")) if selected else {}
    return {
        "AIConfirmationScore": _float_or_none(score.get("score_value", direct.get("score_value"))),
        "SemiconductorBreadthScore": _float_or_none(components.get("semiconductor_breadth")),
        "MegaCapAIScore": _float_or_none(components.get("mega_cap_ai")),
        "EventRiskScore": _float_or_none(
            event.get("event_risk_score", direct.get("event_risk_score"))
        ),
        "source_report_path": _text(selected.get("source_report_path")) if selected else "",
    }


def _latest_report_on_or_before(
    reports: Sequence[Mapping[str, Any]],
    target_date: date,
) -> Mapping[str, Any] | None:
    candidates = [
        report
        for report in reports
        if (_parse_date(report.get("date")) is not None)
        and (_parse_date(report.get("date")) <= target_date)
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: str(item.get("date")))[-1]


def _available_records(dataset_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        record
        for record in _records(dataset_payload.get("records"))
        if record.get("sample_available") is True
    ]


def _forward_windows_from_dataset(dataset_payload: Mapping[str, Any]) -> list[str]:
    windows = [str(window) for window in dataset_payload.get("forward_windows", [])]
    if windows:
        return windows
    return sorted(
        {str(row.get("forward_window")) for row in _records(dataset_payload.get("records"))}
    )


def _eligibility_bucket(record: Mapping[str, Any]) -> str:
    status = _text(record.get("eligibility_status"), "unknown")
    if record.get("fallback_to_etf") is True and status not in {"blocked", "insufficient_data"}:
        return "fallback_to_etf" if status != "watch" else "watch"
    if status in ELIGIBILITY_BUCKETS:
        return status
    return "fallback_to_etf" if record.get("fallback_to_etf") is True else "blocked"


def _event_risk_penalty(
    component_scores: Mapping[str, Any],
    event_risk_score: float | None,
) -> float | None:
    adjusted = _float_or_none(component_scores.get("event_risk_adjusted_score"))
    if adjusted is not None:
        return float(max(0.0, 100.0 - adjusted))
    return event_risk_score


def _event_window_flag(event_risk_score: float | None, reason_codes: Sequence[str]) -> bool:
    if "HIGH_EVENT_RISK" in set(reason_codes):
        return True
    return bool(event_risk_score is not None and event_risk_score >= 60.0)


def _latest_end_date(*paths: Mapping[str, Any]) -> str:
    dates = [item.get("end_date") for item in paths if item.get("end_date") is not None]
    if not dates:
        return ""
    return max(dates).isoformat()


def _eligibility_success_rate(records: Sequence[Mapping[str, Any]]) -> float | None:
    return _share(
        (_eligibility_bucket(row) == "eligible")
        and ((_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0)
        for row in records
    )


def _fallback_miss_rate(records: Sequence[Mapping[str, Any]]) -> float | None:
    fallback = [row for row in records if row.get("fallback_to_etf") is True]
    return _share(
        (_float_or_none(row.get("stock_minus_benchmark_forward_return")) or 0.0) > 0.0
        for row in fallback
    )


def _saved_drawdown(record: Mapping[str, Any]) -> float | None:
    delta = _float_or_none(record.get("drawdown_delta_vs_benchmark"))
    if delta is None or delta >= 0.0:
        return None
    return float(abs(delta))


def _added_drawdown(record: Mapping[str, Any]) -> float | None:
    delta = _float_or_none(record.get("drawdown_delta_vs_benchmark"))
    if delta is None or delta >= 0.0:
        return 0.0
    return float(abs(delta))


def _reason_breakdown(records: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        for reason in _strings(record.get("reason_codes")) or ["unknown"]:
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _group_records(
    records: Sequence[dict[str, Any]],
    fields: Sequence[str],
) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for record in records:
        key = tuple(_text(record.get(field), "unknown") for field in fields)
        grouped.setdefault(key, []).append(record)
    return grouped


def _most_common_text(records: Sequence[Mapping[str, Any]], field: str) -> str:
    values = [_text(record.get(field), "unknown") for record in records]
    if not values:
        return "unknown"
    return sorted(set(values), key=lambda item: (-values.count(item), item))[0]


def _best_window(window_metrics: Sequence[Mapping[str, Any]]) -> str:
    rows = [
        row
        for row in window_metrics
        if _float_or_none(row.get("mean_stock_minus_benchmark")) is not None
    ]
    if not rows:
        return "unknown"
    best = max(
        rows,
        key=lambda item: _float_or_none(item.get("mean_stock_minus_benchmark")) or 0.0,
    )
    return _text(best.get("forward_window"))


def _worst_window(window_metrics: Sequence[Mapping[str, Any]]) -> str:
    rows = [
        row
        for row in window_metrics
        if _float_or_none(row.get("mean_stock_minus_benchmark")) is not None
    ]
    if not rows:
        return "unknown"
    worst = min(
        rows,
        key=lambda item: _float_or_none(item.get("mean_stock_minus_benchmark")) or 0.0,
    )
    return _text(worst.get("forward_window"))


def _best_entity(rows: Sequence[Mapping[str, Any]]) -> str:
    valid = [
        row for row in rows if _float_or_none(row.get("mean_stock_minus_benchmark")) is not None
    ]
    if not valid:
        return "unknown"
    best = max(
        valid,
        key=lambda item: _float_or_none(item.get("mean_stock_minus_benchmark")) or 0.0,
    )
    return _text(best.get("role"), _text(best.get("group"), "unknown"))


def _worst_entity(rows: Sequence[Mapping[str, Any]]) -> str:
    valid = [
        row for row in rows if _float_or_none(row.get("mean_stock_minus_benchmark")) is not None
    ]
    if not valid:
        return "unknown"
    worst = min(
        valid,
        key=lambda item: _float_or_none(item.get("mean_stock_minus_benchmark")) or 0.0,
    )
    return _text(worst.get("role"), _text(worst.get("group"), "unknown"))


def _ai_interaction_bucket(value: float | None) -> str:
    if value is None:
        return "missing"
    if value >= SATELLITE_AI_HIGH_THRESHOLD:
        return "high"
    if value < SATELLITE_AI_LOW_THRESHOLD:
        return "low"
    return "neutral"


def _score_bucket_lift(score_attribution: Mapping[str, Any]) -> float | None:
    rows = _records(score_attribution.get("score_buckets"))
    high = [
        _float_or_none(row.get("mean_stock_minus_benchmark"))
        for row in rows
        if row.get("score_bucket") in {"candidate", "strong_candidate"}
    ]
    low = [
        _float_or_none(row.get("mean_stock_minus_benchmark"))
        for row in rows
        if row.get("score_bucket") in {"reject", "weak"}
    ]
    high_mean = _mean(high)
    low_mean = _mean(low)
    if high_mean is None or low_mean is None:
        return None
    return float(high_mean - low_mean)


def _risk_status(risk_attribution: Mapping[str, Any]) -> str:
    vol = _float_or_none(risk_attribution.get("replacement_volatility_delta_vs_ETF")) or 0.0
    added_drawdown = (
        _float_or_none(risk_attribution.get("drawdown_added_by_eligible_replacement")) or 0.0
    )
    if vol > SATELLITE_HIGH_RISK_VOLATILITY_DELTA or (
        added_drawdown > SATELLITE_DRAWDOWN_SEVERITY_THRESHOLD
    ):
        return "high"
    return "acceptable"


def _evidence_status(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value > SATELLITE_MEANINGFUL_ALPHA_THRESHOLD:
        return "positive"
    if value < -SATELLITE_MEANINGFUL_ALPHA_THRESHOLD:
        return "negative"
    return "mixed"


def _rate_status(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value >= 0.6:
        return "positive"
    if value <= 0.4:
        return "weak"
    return "mixed"


def _ai_interaction_status(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("interactions"))
    high = [
        _float_or_none(row.get("replacement_minus_ETF_by_ai_bucket"))
        for row in rows
        if row.get("dimension") == "AIConfirmationScore" and row.get("ai_bucket") == "high"
    ]
    low = [
        _float_or_none(row.get("replacement_minus_ETF_by_ai_bucket"))
        for row in rows
        if row.get("dimension") == "AIConfirmationScore" and row.get("ai_bucket") == "low"
    ]
    high_mean = _mean(high)
    low_mean = _mean(low)
    if high_mean is None or low_mean is None:
        return "unknown"
    return _evidence_status(high_mean - low_mean)


def _overall_scorecard_status(
    *,
    data_quality: str,
    sample_count: int,
    eligible_alpha: float | None,
    fallback_saved_loss_rate: float | None,
    risk_status: str,
    risk_adjusted_alpha: float | None,
) -> str:
    if data_quality == "FAIL":
        return "blocked_by_data_quality"
    if sample_count < MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT:
        return "needs_more_data"
    if risk_status == "high":
        return "tighten_constraints_recommended"
    if (eligible_alpha or 0.0) > SATELLITE_MEANINGFUL_ALPHA_THRESHOLD and (
        risk_adjusted_alpha or 0.0
    ) > 0.0:
        return "useful_candidate_overlay_policy"
    if (fallback_saved_loss_rate or 0.0) >= 0.6:
        return "ETF_first_fallback_validated"
    return "too_risky_or_noisy"


def _supporting_evidence(
    *,
    eligible_alpha: float | None,
    fallback_saved_loss_rate: float | None,
    score_lift: float | None,
    risk_adjusted_alpha: float | None,
) -> list[str]:
    evidence: list[str] = []
    if (eligible_alpha or 0.0) > SATELLITE_MEANINGFUL_ALPHA_THRESHOLD:
        evidence.append("eligible_stocks_outperformed_benchmark")
    if (fallback_saved_loss_rate or 0.0) >= 0.6:
        evidence.append("fallback_to_etf_saved_losses")
    if (score_lift or 0.0) > SATELLITE_MEANINGFUL_ALPHA_THRESHOLD:
        evidence.append("SatelliteCandidateScore_ranking_lift_positive")
    if (risk_adjusted_alpha or 0.0) > 0.0:
        evidence.append("risk_adjusted_alpha_positive")
    return evidence or ["no_strong_supporting_evidence_yet"]


def _blocking_evidence(*, data_quality: str, sample_count: int, risk_status: str) -> list[str]:
    blockers: list[str] = []
    if data_quality == "FAIL":
        blockers.append("data_quality_failed")
    if sample_count < MIN_SATELLITE_ATTRIBUTION_SAMPLE_COUNT:
        blockers.append("insufficient_forward_samples")
    if risk_status == "high":
        blockers.append("satellite_replacement_risk_too_high")
    return blockers


def _manual_review_recommendation(status: str) -> str:
    mapping = {
        "useful_candidate_overlay_policy": (
            "Review whether satellite replacement policy should continue observing with "
            "current constraints; do not promote without owner approval."
        ),
        "ETF_first_fallback_validated": (
            "Fallback appears protective; continue ETF-first fallback and review missed "
            "upside before relaxing gates."
        ),
        "needs_more_data": "Continue collecting forward samples before changing satellite policy.",
        "too_risky_or_noisy": (
            "Do not expand satellite replacement influence; inspect weak tickers and blockers."
        ),
        "tighten_constraints_recommended": (
            "Review tighter volatility/drawdown/event-risk constraints before further use."
        ),
        "blocked_by_data_quality": "Fix data quality before interpreting satellite attribution.",
    }
    return mapping.get(status, "Manual review required before any policy interpretation.")


def _source_links(dataset_payload: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "decision_date": _text(item.get("decision_date")),
            "source_report_path": _text(item.get("source_report_path")),
        }
        for item in _records(dataset_payload.get("source_reports"))
    ]


def _sample_dataset() -> dict[str, Any]:
    prices = []
    for idx in range(90):
        day = date(2026, 1, 1) + pd.Timedelta(days=idx)
        for symbol, base, drift in (
            ("NVDA", 100.0, 0.004),
            ("AMD", 90.0, -0.001),
            ("SMH", 80.0, 0.002),
            ("QQQ", 70.0, 0.001),
        ):
            price = base * (1.0 + drift) ** idx
            prices.append({"date": day.isoformat(), "symbol": symbol, "adj_close": price})
    satellite_report = {
        "date": "2026-01-20",
        "market_regime": SATELLITE_ATTRIBUTION_MARKET_REGIME,
        "replacement_eligibility": [
            {
                "date": "2026-01-20",
                "ticker": "NVDA",
                "benchmark_etf": "SMH",
                "sleeve": "semiconductor",
                "role": "ai_accelerator",
                "status": "eligible",
                "score_value": 82.0,
                "score_band": "strong_candidate",
                "fallback_to_etf": False,
                "blockers": [],
                "reason_codes": ["SATELLITE_REPLACEMENT_ELIGIBLE"],
                **SATELLITE_ATTRIBUTION_SAFETY,
            },
            {
                "date": "2026-01-20",
                "ticker": "AMD",
                "benchmark_etf": "SMH",
                "sleeve": "semiconductor",
                "role": "ai_accelerator",
                "status": "fallback_to_etf",
                "score_value": 42.0,
                "score_band": "weak",
                "fallback_to_etf": True,
                "blockers": ["LOW_RELATIVE_STRENGTH"],
                "reason_codes": ["LOW_RELATIVE_STRENGTH"],
                **SATELLITE_ATTRIBUTION_SAFETY,
            },
        ],
        "satellite_candidate_scores": [
            {
                "ticker": "NVDA",
                "benchmark_etf": "SMH",
                "score_value": 82.0,
                "score_band": "strong_candidate",
                "component_scores": {
                    "relative_strength_score": 90.0,
                    "trend_score": 85.0,
                    "drawdown_risk_score": 80.0,
                    "event_risk_adjusted_score": 90.0,
                    "ai_confirmation_support_score": 75.0,
                },
                "event_risk_score": 10.0,
            },
            {
                "ticker": "AMD",
                "benchmark_etf": "SMH",
                "score_value": 42.0,
                "score_band": "weak",
                "component_scores": {
                    "relative_strength_score": 30.0,
                    "trend_score": 35.0,
                    "drawdown_risk_score": 60.0,
                    "event_risk_adjusted_score": 80.0,
                    "ai_confirmation_support_score": 55.0,
                },
                "event_risk_score": 20.0,
            },
        ],
        "replacement_plan": {
            "date": "2026-01-20",
            "satellite_allocations": [
                {"ticker": "NVDA", "benchmark_etf": "SMH", "allocation": 0.03}
            ],
            "fallback_positions": [
                {
                    "ticker": "AMD",
                    "benchmark_etf": "SMH",
                    "reason_codes": ["LOW_RELATIVE_STRENGTH"],
                }
            ],
        },
        "ai_confirmation_context": {"score_value": 75.0, "event_risk_score": 10.0},
        "source_report_path": "sample_satellite_report.json",
    }
    ai_report = {
        "date": "2026-01-20",
        "AIConfirmationScore": {
            "score_value": 75.0,
            "component_scores": {
                "semiconductor_breadth": 80.0,
                "mega_cap_ai": 70.0,
            },
        },
        "event_risk_overlay": {"event_risk_score": 10.0},
    }
    return build_satellite_attribution_dataset(
        satellite_reports=[satellite_report],
        prices=pd.DataFrame(prices),
        evaluation_as_of_date=date(2026, 3, 20),
        ai_confirmation_reports=[ai_report],
        data_quality_status="PASS",
    )


def _validation_check(
    check_id: str,
    passed: bool,
    blockers: Sequence[str] | None = None,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "blockers": list(blockers or ([] if passed else [check_id])),
    }


def _report_registry_has_satellite_attribution(
    report_registry: Mapping[str, Any] | None,
) -> bool:
    reports = _records(_mapping(report_registry).get("reports"))
    ids = {str(item.get("report_id")) for item in reports}
    required = ("etf_satellite_attribution_report", "etf_satellite_attribution_validation")
    return all(report_id in ids for report_id in required)


def _raise_if_unsafe_payload(payload: Mapping[str, Any], owner_id: str) -> None:
    issues = _safety_issues(payload, owner_id=owner_id)
    _forbidden_key_issues(payload, owner_id, issues)
    if issues:
        raise SatelliteAttributionError(";".join(issues))


def _safety_issues(payload: Mapping[str, Any], *, owner_id: str) -> list[str]:
    issues: list[str] = []
    for key, expected in SATELLITE_ATTRIBUTION_SAFETY.items():
        if payload.get(key) != expected:
            if key == "production_effect":
                issues.append(f"{owner_id}:UNSAFE_PRODUCTION_EFFECT")
            elif key == "broker_action":
                issues.append(f"{owner_id}:BROKER_ACTION_NOT_NONE")
            else:
                issues.append(f"{owner_id}:UNSAFE_{key.upper()}")
    return issues


def _forbidden_key_issues(payload: Mapping[str, Any], owner_id: str, issues: list[str]) -> None:
    for key in FORBIDDEN_OUTPUT_KEYS:
        if key in payload:
            issues.append(f"{owner_id}:FORBIDDEN_OUTPUT_KEY:{key}")


def _contains_forbidden_output_key(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            key in FORBIDDEN_OUTPUT_KEYS or _contains_forbidden_output_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_output_key(item) for item in value)
    return False


def _by_ticker(records: Sequence[Mapping[str, Any]], ticker: str) -> dict[str, Any]:
    normalized = ticker.strip().upper()
    for record in records:
        if _text(record.get("ticker")).upper() == normalized:
            return dict(record)
    return {}


def _records(value: object) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item not in {"", None}]
    if value in {"", None}:
        return []
    return [str(value)]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _float_or_none(value: object) -> float | None:
    if value in {"", None}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def _mean(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and np.isfinite(value)]
    if not clean:
        return None
    return float(np.mean(clean))


def _median(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None and np.isfinite(value)]
    if not clean:
        return None
    return float(np.median(clean))


def _share(values: Iterable[bool]) -> float | None:
    clean = [bool(value) for value in values]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value in {"", None}:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _stable_id(*parts: str) -> str:
    digest = sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{parts[0]}-{digest}"


def _safety_banner() -> str:
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; forward returns are "
        "evaluation-only attribution evidence"
    )


def _join_list(value: object) -> str:
    items = _strings(value)
    return ", ".join(items) if items else "none"


def _fmt_number(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "n/a"
    return f"{number:.4f}"
