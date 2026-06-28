from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
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
    load_adjusted_price_matrix,
    load_mapping,
    mapping,
    max_price_date,
    round_float,
    strings,
    to_float,
    validate_cached_market_data,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_current_state_policy.yaml"
)
DEFAULT_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "models"
    / "first_layer_composer_v2_predictions.csv"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_current_state"
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"

FAILURE_TYPES = (
    "false_risk_on",
    "false_risk_off",
    "late_risk_off",
    "late_risk_on",
)
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


def run_first_layer_current_state_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    predictions_path: Path = DEFAULT_PREDICTIONS_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    required_benchmarks = strings(mapping(policy.get("benchmarks")).get("required"))
    all_benchmarks = _all_benchmarks(policy)

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=as_of_date,
        expected_price_tickers=required_benchmarks,
        expected_rate_series=(),
    )
    resolved_as_of = (
        _parse_date(data_quality.get("as_of")) or as_of_date or max_price_date(prices_path)
    )

    predictions = _load_predictions(predictions_path, policy, resolved_as_of)
    price_matrix = load_adjusted_price_matrix(prices_path, all_benchmarks)
    benchmark_rows = _benchmark_availability(price_matrix, policy)
    analysis_rows = _build_analysis_rows(
        predictions=predictions,
        price_matrix=price_matrix,
        policy=policy,
        benchmark_rows=benchmark_rows,
    )
    signal_summary = _signal_summary(predictions, policy)
    taxonomy_rows = _failure_taxonomy_rows(analysis_rows, policy)
    consistency = _benchmark_consistency(analysis_rows, benchmark_rows, policy)
    regime_rows = _regime_slice_rows(predictions, analysis_rows, policy, resolved_as_of)
    summary = _summary(
        predictions=predictions,
        benchmark_rows=benchmark_rows,
        taxonomy_rows=taxonomy_rows,
        consistency=consistency,
        regime_rows=regime_rows,
        data_quality=data_quality,
        policy=policy,
        resolved_as_of=resolved_as_of,
    )

    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    taxonomy_payload = _payload(
        report_type="first_layer_failure_taxonomy",
        title="First-Layer Failure Taxonomy",
        status="FIRST_LAYER_FAILURE_TAXONOMY_READY_PROMOTION_BLOCKED",
        summary=summary,
        policy=policy,
        data_quality=data_quality,
        predictions=predictions,
        as_of_date=resolved_as_of,
    )
    taxonomy_payload.update(
        {
            "failure_taxonomy": taxonomy_rows,
            "signal_summary": signal_summary,
            "regime_slices": regime_rows,
            "source_artifacts": {
                "policy_path": str(policy_path),
                "predictions_path": str(predictions_path),
                "prices_path": str(prices_path),
            },
        }
    )

    benchmark_payload = _payload(
        report_type="benchmark_consistency_report",
        title="Benchmark Consistency Report",
        status="BENCHMARK_CONSISTENCY_REPORT_READY_PROMOTION_BLOCKED",
        summary={**summary, **consistency["summary"]},
        policy=policy,
        data_quality=data_quality,
        predictions=predictions,
        as_of_date=resolved_as_of,
    )
    benchmark_payload.update(
        {
            "benchmarks": benchmark_rows,
            "consistency_by_failure_type": consistency["by_failure_type"],
            "analysis_scope": consistency["analysis_scope"],
        }
    )

    final_matrix = _payload(
        report_type="first_layer_current_state_summary",
        title="First-Layer Current State Summary",
        status="FIRST_LAYER_CURRENT_STATE_READY_PROMOTION_BLOCKED",
        summary=summary,
        policy=policy,
        data_quality=data_quality,
        predictions=predictions,
        as_of_date=resolved_as_of,
    )
    final_matrix.update(
        {
            "failure_taxonomy": taxonomy_rows,
            "benchmarks": benchmark_rows,
            "regime_slices": regime_rows,
            "benchmark_consistency": consistency,
        }
    )

    taxonomy_path = output_root / "first_layer_failure_taxonomy.json"
    benchmark_path = output_root / "benchmark_consistency_report.json"
    summary_path = inputs_root / "first_layer_current_state_summary.yaml"
    current_state_doc_path = docs_root / "first_layer_current_state_report.md"
    regime_doc_path = docs_root / "regime_slice_summary.md"

    write_json(taxonomy_path, taxonomy_payload)
    write_json(benchmark_path, benchmark_payload)
    write_yaml(summary_path, final_matrix)
    write_markdown(
        current_state_doc_path,
        _render_current_state_report(final_matrix, taxonomy_rows, benchmark_rows),
    )
    write_markdown(regime_doc_path, _render_regime_slice_summary(final_matrix, regime_rows))

    final_matrix["artifact_paths"] = {
        "first_layer_failure_taxonomy": str(taxonomy_path),
        "benchmark_consistency_report": str(benchmark_path),
        "first_layer_current_state_summary": str(summary_path),
        "first_layer_current_state_report": str(current_state_doc_path),
        "regime_slice_summary": str(regime_doc_path),
    }
    write_yaml(summary_path, final_matrix)
    return clean_for_yaml(final_matrix)


def _load_predictions(path: Path, policy: Mapping[str, Any], as_of_date: date) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"first-layer predictions not found: {path}")
    frame = pd.read_csv(path, parse_dates=["date"])
    missing = {"date", "trend_state"} - set(frame.columns)
    if missing:
        raise ValueError(f"first-layer predictions missing columns: {sorted(missing)}")
    requested = mapping(policy.get("requested_window"))
    requested_start = pd.Timestamp(str(requested.get("start", DEFAULT_BACKTEST_START)))
    frame = frame.loc[
        (frame["date"] >= requested_start) & (frame["date"] <= pd.Timestamp(as_of_date))
    ].copy()
    raw_filtered_rows = int(frame.shape[0])
    frame = (
        frame.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    frame.attrs["raw_filtered_signal_rows"] = raw_filtered_rows
    frame.attrs["deduplicated_signal_rows"] = int(frame.shape[0])
    frame["signal_class"] = [
        _signal_class(state, policy) for state in frame["trend_state"].astype(str)
    ]
    return frame


def _all_benchmarks(policy: Mapping[str, Any]) -> list[str]:
    benchmark_policy = mapping(policy.get("benchmarks"))
    tickers = [
        *strings(benchmark_policy.get("required")),
        *strings(benchmark_policy.get("optional")),
    ]
    return list(dict.fromkeys(tickers))


def _benchmark_availability(
    price_matrix: pd.DataFrame,
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    benchmark_policy = mapping(policy.get("benchmarks"))
    required = set(strings(benchmark_policy.get("required")))
    rows: list[dict[str, Any]] = []
    for ticker in _all_benchmarks(policy):
        series = price_matrix[ticker].dropna() if ticker in price_matrix.columns else pd.Series()
        available = not series.empty
        rows.append(
            {
                "ticker": ticker,
                "required": ticker in required,
                "data_available": available,
                "history_start_date": series.index.min().date().isoformat() if available else "",
                "history_end_date": series.index.max().date().isoformat() if available else "",
                "row_count": int(series.shape[0]) if available else 0,
            }
        )
    return rows


def _build_analysis_rows(
    *,
    predictions: pd.DataFrame,
    price_matrix: pd.DataFrame,
    policy: Mapping[str, Any],
    benchmark_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    thresholds = mapping(policy.get("failure_thresholds"))
    horizon = int(thresholds.get("forward_horizon_days", 20))
    risk_off_lead_days = int(thresholds.get("late_risk_off_required_lead_days", 5))
    risk_on_delay_days = int(thresholds.get("late_risk_on_max_delay_days", 5))
    classes = list(predictions["signal_class"].astype(str))
    rows: list[dict[str, Any]] = []
    available = [str(row["ticker"]) for row in benchmark_rows if row.get("data_available")]
    for ticker in available:
        series = price_matrix[ticker].dropna()
        for idx, record in predictions.iterrows():
            ts = pd.Timestamp(record["date"])
            outcome = _future_outcome(series, ts, horizon)
            if outcome is None:
                continue
            signal_class = str(record["signal_class"])
            recent_states = classes[max(0, idx - risk_off_lead_days) : idx + 1]
            next_states = classes[idx : min(len(classes), idx + risk_on_delay_days + 1)]
            false_risk_on = signal_class == "risk_on" and (
                outcome["forward_return"]
                <= to_float(thresholds.get("false_risk_on_forward_return_threshold"), -0.02)
                or outcome["future_max_drawdown"]
                <= to_float(thresholds.get("false_risk_on_max_drawdown_threshold"), -0.05)
            )
            false_risk_off = signal_class == "risk_off" and (
                outcome["forward_return"]
                >= to_float(thresholds.get("false_risk_off_forward_return_threshold"), 0.03)
            )
            late_risk_off = (
                outcome["future_max_drawdown"]
                <= to_float(thresholds.get("late_risk_off_drawdown_threshold"), -0.05)
                and "risk_off" not in recent_states
            )
            late_risk_on = (
                outcome["forward_return"]
                >= to_float(thresholds.get("late_risk_on_recovery_return_threshold"), 0.05)
                and "risk_on" not in next_states
            )
            rows.append(
                {
                    "date": ts.date().isoformat(),
                    "ticker": ticker,
                    "trend_state": str(record["trend_state"]),
                    "signal_class": signal_class,
                    "forward_horizon_days": horizon,
                    "forward_return": round_float(outcome["forward_return"]),
                    "future_max_drawdown": round_float(outcome["future_max_drawdown"]),
                    "false_risk_on": false_risk_on,
                    "false_risk_off": false_risk_off,
                    "late_risk_off": late_risk_off,
                    "late_risk_on": late_risk_on,
                }
            )
    return rows


def _future_outcome(
    series: pd.Series,
    ts: pd.Timestamp,
    horizon: int,
) -> dict[str, float] | None:
    if ts not in series.index:
        return None
    loc = series.index.get_loc(ts)
    if not isinstance(loc, int) or loc + horizon >= len(series):
        return None
    start_price = to_float(series.iloc[loc])
    end_price = to_float(series.iloc[loc + horizon])
    if start_price <= 0.0 or end_price <= 0.0:
        return None
    window = series.iloc[loc : loc + horizon + 1].dropna().astype(float)
    if len(window) < 2:
        return None
    return {
        "forward_return": end_price / start_price - 1.0,
        "future_max_drawdown": float((window / start_price - 1.0).min()),
    }


def _failure_taxonomy_rows(
    analysis_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for failure_type in FAILURE_TYPES:
        events = [row for row in analysis_rows if row.get(failure_type)]
        by_benchmark = Counter(str(row.get("ticker")) for row in events)
        examples = [
            {
                "date": row.get("date"),
                "ticker": row.get("ticker"),
                "trend_state": row.get("trend_state"),
                "forward_return": row.get("forward_return"),
                "future_max_drawdown": row.get("future_max_drawdown"),
            }
            for row in events[:5]
        ]
        rows.append(
            {
                "failure_type": failure_type,
                "definition": _failure_definition(failure_type, policy),
                "event_count": len(events),
                "benchmark_event_counts": dict(sorted(by_benchmark.items())),
                "representative_examples": examples,
                "promotion_interpretation": "diagnostic_only_not_gate_evidence",
            }
        )
    return rows


def _benchmark_consistency(
    analysis_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    core = [
        str(row["ticker"])
        for row in benchmark_rows
        if row.get("required") and row.get("data_available")
    ]
    by_failure: list[dict[str, Any]] = []
    scores: list[float] = []
    for failure_type in FAILURE_TYPES:
        counts = {
            ticker: sum(
                1
                for row in analysis_rows
                if row.get("ticker") == ticker and bool(row.get(failure_type))
            )
            for ticker in core
        }
        max_count = max(counts.values(), default=0)
        min_count = min(counts.values(), default=0)
        score = 1.0 if max_count == 0 else 1.0 - ((max_count - min_count) / max_count)
        scores.append(score)
        by_failure.append(
            {
                "failure_type": failure_type,
                "core_benchmark_event_counts": counts,
                "core_benchmark_count": len(core),
                "consistency_score": round_float(score),
                "interpretation": "higher_is_more_consistent_across_core_benchmarks",
            }
        )
    optional_missing = [
        str(row["ticker"])
        for row in benchmark_rows
        if not row.get("required") and not row.get("data_available")
    ]
    total_score = sum(scores) / len(scores) if scores else 0.0
    minimum_core = int(
        mapping(policy.get("benchmark_consistency")).get("minimum_core_benchmark_count", 3)
    )
    status = (
        "CORE_BENCHMARKS_AVAILABLE"
        if len(core) >= minimum_core
        else "CORE_BENCHMARK_COVERAGE_INCOMPLETE"
    )
    return {
        "summary": {
            "benchmark_consistency_status": status,
            "benchmark_consistency_score": round_float(total_score),
            "core_benchmarks_available": core,
            "optional_benchmarks_missing": optional_missing,
        },
        "by_failure_type": by_failure,
        "analysis_scope": {
            "score_uses_core_benchmarks_only": True,
            "optional_missing_not_imputed": True,
        },
    }


def _regime_slice_rows(
    predictions: pd.DataFrame,
    analysis_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    as_of_date: date,
) -> list[dict[str, Any]]:
    thresholds = mapping(policy.get("failure_thresholds"))
    minimum_observations = int(thresholds.get("minimum_slice_observations", 60))
    rows: list[dict[str, Any]] = []
    analysis_frame = pd.DataFrame(list(analysis_rows))
    if not analysis_frame.empty:
        analysis_frame["date"] = pd.to_datetime(analysis_frame["date"])
    for item in policy.get("regime_slices", []):
        if not isinstance(item, Mapping):
            continue
        slice_id = str(item.get("id"))
        start = pd.Timestamp(str(item.get("start")))
        end_raw = str(item.get("end", "latest"))
        end = pd.Timestamp(as_of_date if end_raw == "latest" else end_raw)
        signal_slice = predictions.loc[
            (predictions["date"] >= start) & (predictions["date"] <= end)
        ]
        if analysis_frame.empty:
            slice_events = pd.DataFrame()
        else:
            slice_events = analysis_frame.loc[
                (analysis_frame["date"] >= start) & (analysis_frame["date"] <= end)
            ]
        failure_counts = {
            failure_type: int(slice_events[failure_type].sum())
            if failure_type in slice_events
            else 0
            for failure_type in FAILURE_TYPES
        }
        observations = int(signal_slice.shape[0])
        if observations == 0:
            coverage_status = "NO_SIGNAL_COVERAGE"
        elif observations < minimum_observations:
            coverage_status = "LOW_SIGNAL_COVERAGE"
        else:
            coverage_status = "SIGNAL_COVERED"
        rows.append(
            {
                "slice_id": slice_id,
                "label": str(item.get("label", slice_id)),
                "role": str(item.get("role", "")),
                "start": start.date().isoformat(),
                "end": end.date().isoformat(),
                "signal_observation_count": observations,
                "coverage_status": coverage_status,
                "signal_distribution": dict(Counter(signal_slice["trend_state"].astype(str))),
                "failure_event_counts": failure_counts,
                "primary_ai_conclusion_window": slice_id != "2022_bear_rate_shock",
            }
        )
    return rows


def _signal_summary(predictions: pd.DataFrame, policy: Mapping[str, Any]) -> dict[str, Any]:
    classes = list(predictions["signal_class"].astype(str))
    flips = sum(1 for prev, cur in zip(classes, classes[1:], strict=False) if prev != cur)
    window = int(
        mapping(policy.get("failure_thresholds")).get(
            "regime_flip_normalized_window_days", 20
        )
    )
    normalized = flips / max(1, len(classes)) * window
    return {
        "raw_filtered_signal_rows": int(
            predictions.attrs.get("raw_filtered_signal_rows", predictions.shape[0])
        ),
        "observation_count": int(predictions.shape[0]),
        "duplicate_signal_rows_removed": int(
            predictions.attrs.get("raw_filtered_signal_rows", predictions.shape[0])
        )
        - int(predictions.shape[0]),
        "trend_state_distribution": dict(Counter(predictions["trend_state"].astype(str))),
        "signal_class_distribution": dict(Counter(classes)),
        "regime_flip_count": flips,
        "regime_flip_rate_per_20_observations": round_float(normalized),
    }


def _summary(
    *,
    predictions: pd.DataFrame,
    benchmark_rows: Sequence[Mapping[str, Any]],
    taxonomy_rows: Sequence[Mapping[str, Any]],
    consistency: Mapping[str, Any],
    regime_rows: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
    policy: Mapping[str, Any],
    resolved_as_of: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    total_failures = sum(int(row.get("event_count", 0)) for row in taxonomy_rows)
    actual_signal_start = (
        predictions["date"].min().date().isoformat() if not predictions.empty else ""
    )
    actual_signal_end = (
        predictions["date"].max().date().isoformat() if not predictions.empty else ""
    )
    requested_start = str(requested.get("start", DEFAULT_BACKTEST_START))
    coverage_gap = bool(actual_signal_start and actual_signal_start > requested_start)
    no_2022_signal = any(
        row.get("slice_id") == "2022_bear_rate_shock"
        and row.get("coverage_status") == "NO_SIGNAL_COVERAGE"
        for row in regime_rows
    )
    return {
        "current_state_conclusion": "REDESIGN_REQUIRED_DIAGNOSTIC_ONLY",
        "market_regime": MARKET_REGIME,
        "requested_start": requested_start,
        "requested_end": str(requested.get("end", "latest")),
        "as_of": resolved_as_of.isoformat(),
        "actual_signal_start": actual_signal_start,
        "actual_signal_end": actual_signal_end,
        "signal_coverage_gap_from_requested_start": coverage_gap,
        "2022_stress_slice_signal_coverage": "missing" if no_2022_signal else "available",
        "data_quality_status": data_quality.get("status"),
        "data_quality_passed": data_quality.get("passed"),
        "failure_event_count": total_failures,
        "benchmark_consistency_score": mapping(consistency.get("summary")).get(
            "benchmark_consistency_score"
        ),
        "benchmark_availability": {
            str(row["ticker"]): bool(row.get("data_available")) for row in benchmark_rows
        },
        "next_required_tasks": ["TRADING-2271", "TRADING-2272", "TRADING-2273"],
    }


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    policy: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    predictions: pd.DataFrame,
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    actual_signal_start = (
        predictions["date"].min().date().isoformat() if not predictions.empty else ""
    )
    actual_signal_end = (
        predictions["date"].max().date().isoformat() if not predictions.empty else ""
    )
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "requested_start": str(requested.get("start", DEFAULT_BACKTEST_START)),
        "requested_end": str(requested.get("end", "latest")),
        "as_of": as_of_date.isoformat(),
        "actual_signal_start": actual_signal_start,
        "actual_signal_end": actual_signal_end,
        "data_quality_status": data_quality.get("status"),
        "data_quality": data_quality,
        "policy_id": str(policy.get("policy_id", "")),
        "policy_hash": _policy_hash(policy),
        "summary": clean_for_yaml(dict(summary)),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "first_layer_current_state",
            "model_version": "first_layer_composer_v2_current_state",
            "threshold_policy": str(policy.get("policy_id", "")),
            "candidate_count": 0,
            "boundary_contract_version": "first_layer_current_state_research_only_v1",
        },
        **SAFETY_BOUNDARY,
        **BLOCKED_STATE,
    }


def _failure_definition(failure_type: str, policy: Mapping[str, Any]) -> str:
    thresholds = mapping(policy.get("failure_thresholds"))
    definitions = {
        "false_risk_on": (
            "Signal is risk_on/constructive but the benchmark has weak forward return "
            f"<= {thresholds.get('false_risk_on_forward_return_threshold')} or max drawdown "
            f"<= {thresholds.get('false_risk_on_max_drawdown_threshold')} over the horizon."
        ),
        "false_risk_off": (
            "Signal is risk_off/defensive but the benchmark forward return is "
            f">= {thresholds.get('false_risk_off_forward_return_threshold')}."
        ),
        "late_risk_off": (
            "Benchmark drawdown breaches the pilot threshold and no risk_off signal appeared "
            f"in the prior {thresholds.get('late_risk_off_required_lead_days')} signal days."
        ),
        "late_risk_on": (
            "Benchmark recovery exceeds the pilot threshold and no risk_on signal appears within "
            f"{thresholds.get('late_risk_on_max_delay_days')} signal days."
        ),
    }
    return definitions[failure_type]


def _signal_class(trend_state: str, policy: Mapping[str, Any]) -> str:
    signal_policy = mapping(policy.get("signal_source"))
    state = trend_state.lower()
    if state in {item.lower() for item in strings(signal_policy.get("risk_on_states"))}:
        return "risk_on"
    if state in {item.lower() for item in strings(signal_policy.get("risk_off_states"))}:
        return "risk_off"
    return "neutral"


def _policy_hash(policy: Mapping[str, Any]) -> str:
    encoded = json.dumps(clean_for_yaml(dict(policy)), sort_keys=True, ensure_ascii=False).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _render_current_state_report(
    payload: Mapping[str, Any],
    taxonomy_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# First-layer current state report",
        "",
        f"- status: `{payload.get('status')}`",
        f"- market_regime: `{payload.get('market_regime')}`",
        (
            f"- requested_date_range: `{payload.get('requested_start')}` "
            f"to `{payload.get('requested_end')}`"
        ),
        (
            f"- actual_signal_range: `{payload.get('actual_signal_start')}` "
            f"to `{payload.get('actual_signal_end')}`"
        ),
        f"- data_quality_status: `{payload.get('data_quality_status')}`",
        "- safety: `promotion_allowed=false`, `paper_shadow_allowed=false`, "
        "`production_allowed=false`, `broker_action=none`",
        "",
        "## 结论",
        "",
        "当前 first-layer composer v2 只能作为失败归因基线。它没有覆盖 2022 stress slice，"
        "并且仍出现 false risk-on/off、late risk-on/off 与 regime flip 诊断事件；这些结果支持"
        "继续做 TRADING-2271～2273，而不是恢复 reopen gate 或 paper-shadow。",
        "",
        "## Failure taxonomy",
        "",
        "|failure_type|event_count|promotion interpretation|",
        "|---|---:|---|",
    ]
    for row in taxonomy_rows:
        lines.append(
            f"|`{row.get('failure_type')}`|{row.get('event_count')}|"
            f"{row.get('promotion_interpretation')}|"
        )
    lines.extend(
        [
            "",
            "## Benchmark coverage",
            "",
            "|ticker|required|data_available|history_start|history_end|rows|",
            "|---|---:|---:|---|---|---:|",
        ]
    )
    for row in benchmark_rows:
        lines.append(
            f"|`{row.get('ticker')}`|{row.get('required')}|{row.get('data_available')}|"
            f"{row.get('history_start_date')}|{row.get('history_end_date')}|{row.get('row_count')}|"
        )
    lines.extend(
        [
            "",
            "## Audit notes",
            "",
            f"- benchmark_consistency_score: `{summary.get('benchmark_consistency_score')}`",
            (
                "- 2022_stress_slice_signal_coverage: "
                f"`{summary.get('2022_stress_slice_signal_coverage')}`"
            ),
            (
                "- IWM / RSP 缺失时不做 proxy 填补，也不把 QQQ/SPY/SMH consistency "
                "泛化成 true breadth。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_regime_slice_summary(
    payload: Mapping[str, Any],
    regime_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Regime slice summary",
        "",
        f"- status: `{payload.get('status')}`",
        f"- market_regime: `{payload.get('market_regime')}`",
        f"- data_quality_status: `{payload.get('data_quality_status')}`",
        "",
        "|slice|role|date_range|signal_obs|coverage_status|false_risk_on|false_risk_off|late_risk_off|late_risk_on|",
        "|---|---|---|---:|---|---:|---:|---:|---:|",
    ]
    for row in regime_rows:
        counts = mapping(row.get("failure_event_counts"))
        lines.append(
            f"|`{row.get('slice_id')}`|{row.get('role')}|{row.get('start')} to {row.get('end')}|"
            f"{row.get('signal_observation_count')}|`{row.get('coverage_status')}`|"
            f"{counts.get('false_risk_on', 0)}|{counts.get('false_risk_off', 0)}|"
            f"{counts.get('late_risk_off', 0)}|{counts.get('late_risk_on', 0)}|"
        )
    lines.extend(
        [
            "",
            "2022 slice 是 stress comparison，不是默认 AI-cycle conclusion window。当前 baseline "
            "signal 从 2023-02-22 才开始，因此不能宣称已经验证 2022 bear/rate-shock behavior。",
        ]
    )
    return "\n".join(lines) + "\n"
