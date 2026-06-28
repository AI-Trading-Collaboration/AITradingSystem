from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.vendor_adapters import norgate_partial_effectiveness as partial
from ai_trading_system.vendor_adapters.norgate_connector import (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    DEFAULT_RESEARCH_DOCS_ROOT,
    DEFAULT_RESEARCH_INPUTS_ROOT,
    NorgateConnector,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "norgate_2y_partial_evidence_review_policy.yaml"
)
DEFAULT_2267_COVERAGE_PATH = (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT / "norgate_trial_partial_effectiveness_coverage_report.json"
)
DEFAULT_2267_FEATURE_CSV_PATH = (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT / "norgate_trial_breadth_feature_report_2y.csv"
)
DEFAULT_2267_LOCAL_SIGNAL_PATH = (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT / "norgate_trial_local_signal_report_2y.json"
)
DEFAULT_2267_CONCLUSION_PATH = (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT
    / "norgate_trial_partial_effectiveness_conclusion_matrix.json"
)

PARTIAL_EVIDENCE_SAFETY_BOUNDARY: dict[str, Any] = {
    "research_only": True,
    "primary_window_validated": False,
    "model_ready_for_2021_primary_window": False,
    "reopen_gate_allowed": False,
    "first_layer_reopen_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "candidate_count": 0,
    "dynamic_promotion_status": "BLOCKED",
    "purchase_allowed_without_owner_approval": False,
}


def run_norgate_2y_partial_evidence_review(
    *,
    output_root: Path = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    policy_path: Path = DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
    coverage_path: Path = DEFAULT_2267_COVERAGE_PATH,
    feature_csv_path: Path = DEFAULT_2267_FEATURE_CSV_PATH,
    local_signal_path: Path = DEFAULT_2267_LOCAL_SIGNAL_PATH,
    prior_conclusion_path: Path = DEFAULT_2267_CONCLUSION_PATH,
    benchmark_price_frames: Mapping[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """Review 2267 2Y partial evidence and write owner decision memo artifacts."""

    partial._ensure_roots(output_root, docs_root, inputs_root)
    policy = _load_policy(policy_path)
    missing = [
        str(path)
        for path in (coverage_path, feature_csv_path, local_signal_path, prior_conclusion_path)
        if not path.exists()
    ]
    if missing:
        return _write_blocked_review_pack(
            output_root=output_root,
            docs_root=docs_root,
            inputs_root=inputs_root,
            policy=policy,
            blocked_reason="missing_2267_artifacts",
            missing_artifacts=missing,
        )

    coverage = _read_json(coverage_path)
    feature_frame = _read_feature_frame(feature_csv_path)
    local_signal = _read_json(local_signal_path)
    prior_conclusion = _read_json(prior_conclusion_path)
    if feature_frame.empty:
        return _write_blocked_review_pack(
            output_root=output_root,
            docs_root=docs_root,
            inputs_root=inputs_root,
            policy=policy,
            blocked_reason="empty_feature_artifact",
            missing_artifacts=[],
        )

    start_ts = pd.Timestamp(feature_frame["date"].min()).normalize()
    end_ts = pd.Timestamp(feature_frame["date"].max()).normalize()
    benchmark_frames, benchmark_environment_status = _resolve_benchmark_price_frames(
        policy=policy,
        start_ts=start_ts,
        end_ts=end_ts,
        benchmark_price_frames=benchmark_price_frames,
    )

    feature_variation = _build_feature_variation_review(feature_frame, policy)
    bucket_balance = _build_bucket_balance_review(local_signal, policy)
    benchmark_rows = _build_benchmark_consistency_rows(
        feature_frame=feature_frame,
        benchmark_frames=benchmark_frames,
        policy=policy,
    )
    outcome_dominance = _build_outcome_dominance_review(benchmark_rows, policy)
    event_counts = _build_event_count_review(feature_frame, local_signal, policy)
    baseline_increment = _build_baseline_increment_review(local_signal, policy)
    benchmark_consistency = _build_benchmark_consistency_review(benchmark_rows, policy)
    stress_window = _build_stress_window_review(coverage, prior_conclusion, policy)
    conclusion = _build_conclusion_matrix(
        coverage=coverage,
        prior_conclusion=prior_conclusion,
        feature_variation=feature_variation,
        bucket_balance=bucket_balance,
        outcome_dominance=outcome_dominance,
        event_counts=event_counts,
        baseline_increment=baseline_increment,
        benchmark_consistency=benchmark_consistency,
        stress_window=stress_window,
        policy=policy,
        benchmark_environment_status=benchmark_environment_status,
    )
    review = _build_review_payload(
        coverage=coverage,
        prior_conclusion=prior_conclusion,
        feature_variation=feature_variation,
        bucket_balance=bucket_balance,
        outcome_dominance=outcome_dominance,
        event_counts=event_counts,
        baseline_increment=baseline_increment,
        benchmark_consistency=benchmark_consistency,
        stress_window=stress_window,
        benchmark_rows=benchmark_rows,
        conclusion=conclusion,
        policy=policy,
        benchmark_environment_status=benchmark_environment_status,
    )
    memo = _build_decision_memo_payload(review, conclusion, policy)
    _write_review_artifacts(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        review=review,
        memo=memo,
        conclusion=conclusion,
        benchmark_rows=benchmark_rows,
    )
    return conclusion


def _write_blocked_review_pack(
    *,
    output_root: Path,
    docs_root: Path,
    inputs_root: Path,
    policy: Mapping[str, Any],
    blocked_reason: str,
    missing_artifacts: Sequence[str],
) -> dict[str, Any]:
    conclusion = {
        "schema_version": "norgate_2y_partial_evidence_conclusion_matrix.v1",
        "report_type": "norgate_2y_partial_evidence_conclusion_matrix",
        "status": "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_BLOCKED",
        "generated_at": partial._now(),
        "blocked_reason": blocked_reason,
        "missing_artifacts": list(missing_artifacts),
        "local_signal_evidence": "blocked",
        "local_signal_evidence_reason": "inconclusive",
        "trial_2y_feature_value": "weak",
        "full_history_needed_for_final_answer": True,
        "purchase_platinum_recommendation": "defer",
        "purchase_rationale": "weak_evidence",
        "review_policy": str(policy.get("policy_id", "")),
        "research_window_id": "norgate_trial_2y_partial",
        **PARTIAL_EVIDENCE_SAFETY_BOUNDARY,
        "research_audit_metadata": _audit_metadata(policy),
    }
    review = {
        "schema_version": "norgate_2y_partial_evidence_review.v1",
        "report_type": "norgate_2y_partial_evidence_review",
        "status": "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_BLOCKED",
        "generated_at": partial._now(),
        "blocked_reason": blocked_reason,
        "missing_artifacts": list(missing_artifacts),
        "conclusion_matrix": conclusion,
        **PARTIAL_EVIDENCE_SAFETY_BOUNDARY,
    }
    memo = _build_decision_memo_payload(review, conclusion, policy)
    _write_review_artifacts(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        review=review,
        memo=memo,
        conclusion=conclusion,
        benchmark_rows=[],
    )
    return conclusion


def _build_feature_variation_review(
    feature_frame: pd.DataFrame,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    reviewed_features = _policy_list(
        policy,
        ("feature_variation_policy", "reviewed_features"),
        ["pct_above_ma20", "pct_above_ma50", "pct_above_ma200", "breadth_momentum"],
    )
    min_days = _policy_int(policy, ("feature_variation_policy", "min_non_null_days"), 100)
    min_range = _policy_float(
        policy,
        ("feature_variation_policy", "min_range_for_nonflat_breadth_fraction"),
        0.15,
    )
    min_std = _policy_float(
        policy,
        ("feature_variation_policy", "min_std_for_nonflat_breadth_fraction"),
        0.05,
    )
    min_nonflat_count = _policy_int(
        policy,
        ("feature_variation_policy", "min_nonflat_feature_count"),
        2,
    )
    rows: list[dict[str, Any]] = []
    nonflat_count = 0
    for feature in reviewed_features:
        series = pd.to_numeric(feature_frame.get(feature), errors="coerce").dropna()
        sample_count = int(len(series))
        value_range = float(series.max() - series.min()) if sample_count else math.nan
        value_std = float(series.std(ddof=0)) if sample_count else math.nan
        nonflat = (
            sample_count >= min_days
            and partial._finite(value_range)
            and partial._finite(value_std)
            and value_range >= min_range
            and value_std >= min_std
        )
        if nonflat:
            nonflat_count += 1
        rows.append(
            {
                "feature": feature,
                "non_null_day_count": sample_count,
                "min": partial._round_or_none(series.min() if sample_count else math.nan),
                "max": partial._round_or_none(series.max() if sample_count else math.nan),
                "range": partial._round_or_none(value_range),
                "std": partial._round_or_none(value_std),
                "nonflat": nonflat,
            }
        )
    pct_above_ma50_nonflat = any(
        row["feature"] == "pct_above_ma50" and bool(row["nonflat"]) for row in rows
    )
    sufficient = nonflat_count >= min_nonflat_count and pct_above_ma50_nonflat
    return {
        "status": "FEATURE_VARIATION_SUFFICIENT" if sufficient else "FEATURE_VARIATION_WEAK",
        "variation_sufficient": sufficient,
        "nonflat_feature_count": nonflat_count,
        "required_nonflat_feature_count": min_nonflat_count,
        "pct_above_ma50_nonflat": pct_above_ma50_nonflat,
        "feature_rows": rows,
    }


def _build_bucket_balance_review(
    local_signal: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    min_bucket_count = _policy_int(policy, ("bucket_policy", "min_bucket_sample_count"), 50)
    max_imbalance = _policy_float(policy, ("bucket_policy", "max_bucket_imbalance_ratio"), 1.25)
    rows = [
        row
        for row in local_signal.get("breadth_bucket_vs_forward_return", [])
        if isinstance(row, Mapping)
    ]
    counts = {
        str(row.get("breadth_bucket")): int(row.get("sample_count", 0) or 0)
        for row in rows
    }
    bucket_counts = [counts.get(bucket, 0) for bucket in ("low", "mid", "high")]
    min_count = min(bucket_counts) if bucket_counts else 0
    max_count = max(bucket_counts) if bucket_counts else 0
    imbalance_ratio = partial._ratio(max_count, min_count) if min_count else math.inf
    sufficient = min_count >= min_bucket_count and imbalance_ratio <= max_imbalance
    return {
        "status": "BUCKET_SAMPLE_BALANCED" if sufficient else "BUCKET_SAMPLE_WEAK",
        "bucket_sample_sufficient": sufficient,
        "bucket_counts": counts,
        "min_bucket_sample_count": min_count,
        "max_bucket_sample_count": max_count,
        "bucket_imbalance_ratio": partial._round_or_none(imbalance_ratio),
        "policy_min_bucket_sample_count": min_bucket_count,
        "policy_max_bucket_imbalance_ratio": max_imbalance,
    }


def _build_benchmark_consistency_rows(
    *,
    feature_frame: pd.DataFrame,
    benchmark_frames: Mapping[str, pd.DataFrame],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    base = _prepared_signal_frame(feature_frame)
    horizons = _policy_int_list(policy, ("benchmark_consistency_policy", "horizons"), [5, 10, 20])
    rows: list[dict[str, Any]] = []
    for symbol in _benchmark_symbols(policy):
        prices = benchmark_frames.get(symbol, pd.DataFrame())
        if prices.empty:
            for horizon in horizons:
                rows.append(
                    {
                        "benchmark": symbol,
                        "horizon_days": horizon,
                        "status": "BENCHMARK_PRICE_UNAVAILABLE",
                        "sample_count": 0,
                        "supporting_breadth_signal": False,
                    }
                )
            continue
        forward = _benchmark_forward_frame(prices, horizons)
        merged = base.merge(forward, on="date", how="left")
        for horizon in horizons:
            return_column = f"next_{horizon}d_return"
            valid = merged[
                merged["breadth_bucket"].notna() & merged[return_column].notna()
            ].copy()
            bucket_stats = _bucket_return_stats(valid, return_column)
            low_avg = bucket_stats.get("low", {}).get("avg_return")
            high_avg = bucket_stats.get("high", {}).get("avg_return")
            spread = (
                float(high_avg) - float(low_avg)
                if partial._finite(high_avg) and partial._finite(low_avg)
                else math.nan
            )
            dominance = _return_dominance_stats(valid[return_column], policy)
            rows.append(
                {
                    "benchmark": symbol,
                    "horizon_days": horizon,
                    "status": "BENCHMARK_FORWARD_RETURN_READY",
                    "sample_count": int(len(valid)),
                    "low_bucket_count": int(bucket_stats.get("low", {}).get("sample_count", 0)),
                    "mid_bucket_count": int(bucket_stats.get("mid", {}).get("sample_count", 0)),
                    "high_bucket_count": int(
                        bucket_stats.get("high", {}).get("sample_count", 0)
                    ),
                    "low_bucket_avg_return": partial._round_or_none(low_avg),
                    "mid_bucket_avg_return": partial._round_or_none(
                        bucket_stats.get("mid", {}).get("avg_return")
                    ),
                    "high_bucket_avg_return": partial._round_or_none(high_avg),
                    "high_minus_low_return": partial._round_or_none(spread),
                    "supporting_breadth_signal": partial._finite(spread) and spread > 0,
                    **dominance,
                }
            )
    return rows


def _build_outcome_dominance_review(
    benchmark_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    max_top_share = _policy_float(
        policy,
        ("outcome_dominance_policy", "max_top_decile_abs_contribution_share"),
        0.5,
    )
    max_single_share = _policy_float(
        policy,
        ("outcome_dominance_policy", "max_single_day_abs_contribution_share"),
        0.25,
    )
    ready_rows = [row for row in benchmark_rows if row.get("sample_count", 0)]
    dominated_rows = [
        row
        for row in ready_rows
        if bool(row.get("outlier_dominated"))
        or _float(row.get("top_decile_abs_contribution_share")) > max_top_share
        or _float(row.get("max_single_day_abs_contribution_share")) > max_single_share
    ]
    return {
        "status": (
            "OUTCOME_NOT_DOMINATED_BY_FEW_DAYS"
            if ready_rows and not dominated_rows
            else "OUTCOME_DOMINANCE_REVIEW_WEAK_OR_BLOCKED"
        ),
        "outcome_dominated_by_few_days": bool(dominated_rows),
        "reviewed_benchmark_horizon_count": len(ready_rows),
        "dominated_benchmark_horizon_count": len(dominated_rows),
        "max_top_decile_abs_contribution_share": partial._round_or_none(
            max((_float(row.get("top_decile_abs_contribution_share")) for row in ready_rows),
                default=math.nan)
        ),
        "max_single_day_abs_contribution_share": partial._round_or_none(
            max((_float(row.get("max_single_day_abs_contribution_share")) for row in ready_rows),
                default=math.nan)
        ),
    }


def _build_event_count_review(
    feature_frame: pd.DataFrame,
    local_signal: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    min_deterioration = _policy_int(
        policy,
        ("event_policy", "min_deterioration_event_count"),
        50,
    )
    min_recovery = _policy_int(policy, ("event_policy", "min_recovery_event_count"), 50)
    deterioration_rows = [
        row
        for row in local_signal.get("breadth_deterioration_vs_future_drawdown", [])
        if isinstance(row, Mapping)
    ]
    deterioration_count = 0
    non_deterioration_count = 0
    for row in deterioration_rows:
        if bool(row.get("breadth_deterioration")):
            deterioration_count = int(row.get("sample_count", 0) or 0)
        else:
            non_deterioration_count = int(row.get("sample_count", 0) or 0)
    recovery_count = _recovery_event_count(feature_frame)
    sufficient = deterioration_count >= min_deterioration and recovery_count >= min_recovery
    return {
        "status": "EVENT_COUNTS_SUFFICIENT" if sufficient else "EVENT_COUNTS_WEAK",
        "event_count_sufficient": sufficient,
        "deterioration_event_count": deterioration_count,
        "non_deterioration_event_count": non_deterioration_count,
        "recovery_event_count": recovery_count,
        "policy_min_deterioration_event_count": min_deterioration,
        "policy_min_recovery_event_count": min_recovery,
    }


def _build_baseline_increment_review(
    local_signal: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    baseline = dict(local_signal.get("baseline_comparison", {}))
    base_stats = dict(baseline.get("baseline_first_layer_proxy", {}))
    plus_stats = dict(baseline.get("baseline_plus_breadth", {}))
    false_off_delta = _float(baseline.get("false_risk_off_delta"))
    false_on_delta = _float(baseline.get("false_risk_on_delta"))
    capture_delta = _float(plus_stats.get("future_drawdown_event_capture_ratio")) - _float(
        base_stats.get("future_drawdown_event_capture_ratio")
    )
    near_zero_threshold = _policy_float(
        policy,
        ("baseline_incremental_policy", "max_abs_false_signal_delta_for_near_zero"),
        0.02,
    )
    capture_near_zero_threshold = _policy_float(
        policy,
        ("baseline_incremental_policy", "max_abs_capture_delta_for_near_zero"),
        0.02,
    )
    false_rates_worse = (
        false_off_delta > near_zero_threshold or false_on_delta > near_zero_threshold
    )
    near_zero = (
        abs(false_off_delta) <= near_zero_threshold
        and abs(false_on_delta) <= near_zero_threshold
        and abs(capture_delta) <= capture_near_zero_threshold
    )
    improves = false_off_delta <= 0 and false_on_delta <= 0 and capture_delta >= 0
    if improves:
        direction = "improves"
    elif false_rates_worse:
        direction = "worse_false_signal_rates"
    elif near_zero:
        direction = "near_zero"
    else:
        direction = "unstable"
    return {
        "status": "BASELINE_INCREMENTAL_VALUE_PRESENT" if improves else "NO_STABLE_INCREMENT",
        "baseline_increment_direction": direction,
        "incremental_value_positive": improves,
        "incremental_value_near_zero": near_zero,
        "false_risk_off_delta": partial._round_or_none(false_off_delta),
        "false_risk_on_delta": partial._round_or_none(false_on_delta),
        "future_drawdown_capture_delta": partial._round_or_none(capture_delta),
        "baseline_risk_off_day_count": int(base_stats.get("risk_off_day_count", 0) or 0),
        "baseline_plus_breadth_risk_off_day_count": int(
            plus_stats.get("risk_off_day_count", 0) or 0
        ),
    }


def _build_benchmark_consistency_review(
    benchmark_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    horizons = _policy_int_list(policy, ("benchmark_consistency_policy", "horizons"), [5, 10, 20])
    min_supporting_horizons = _policy_int(
        policy,
        ("benchmark_consistency_policy", "min_supporting_horizons_per_benchmark"),
        2,
    )
    min_supporting_benchmarks = _policy_int(
        policy,
        ("benchmark_consistency_policy", "min_supporting_benchmark_count_for_consistent_signal"),
        2,
    )
    by_benchmark: dict[str, dict[str, Any]] = {}
    for symbol in _benchmark_symbols(policy):
        rows = [row for row in benchmark_rows if row.get("benchmark") == symbol]
        supporting = len([row for row in rows if bool(row.get("supporting_breadth_signal"))])
        ready = len([row for row in rows if int(row.get("sample_count", 0) or 0) > 0])
        by_benchmark[symbol] = {
            "ready_horizon_count": ready,
            "supporting_horizon_count": supporting,
            "supports_signal": ready == len(horizons) and supporting >= min_supporting_horizons,
        }
    supporting_benchmark_count = len(
        [row for row in by_benchmark.values() if bool(row.get("supports_signal"))]
    )
    consistent = supporting_benchmark_count >= min_supporting_benchmarks
    return {
        "status": "BENCHMARK_SIGNAL_CONSISTENT" if consistent else "BENCHMARK_SIGNAL_INCONSISTENT",
        "benchmark_signal_consistent": consistent,
        "supporting_benchmark_count": supporting_benchmark_count,
        "required_supporting_benchmark_count": min_supporting_benchmarks,
        "benchmark_rows": by_benchmark,
    }


def _build_stress_window_review(
    coverage: Mapping[str, Any],
    prior_conclusion: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    earliest = str(
        coverage.get("earliest_price_date") or prior_conclusion.get("earliest_price_date")
    )
    latest = str(coverage.get("latest_price_date") or prior_conclusion.get("latest_price_date"))
    primary_start = str(
        prior_conclusion.get("primary_window_start")
        or _policy_value(policy, ("trial_window_policy", "primary_window_start"), "2021-02-22")
    )
    stress_start = str(
        _policy_value(policy, ("stress_window_policy", "stress_window_start"), "2022-01-03")
    )
    stress_end = str(
        _policy_value(policy, ("stress_window_policy", "stress_window_end"), "2022-12-30")
    )
    earliest_ts = pd.Timestamp(earliest) if earliest else pd.NaT
    stress_start_ts = pd.Timestamp(stress_start)
    stress_end_ts = pd.Timestamp(stress_end)
    primary_start_ts = pd.Timestamp(primary_start)
    includes_2022_stress = bool(
        pd.notna(earliest_ts)
        and earliest_ts <= stress_start_ts
        and pd.Timestamp(latest) >= stress_end_ts
    )
    primary_window_covered = bool(pd.notna(earliest_ts) and earliest_ts <= primary_start_ts)
    full_history_needed = not primary_window_covered or not includes_2022_stress
    return {
        "status": (
            "TRIAL_WINDOW_INCLUDES_STRESS"
            if includes_2022_stress
            else "TRIAL_WINDOW_MISSING_2022_STRESS"
        ),
        "earliest_price_date": earliest,
        "latest_price_date": latest,
        "primary_window_start": primary_start,
        "stress_window_start": stress_start,
        "stress_window_end": stress_end,
        "primary_window_covered": primary_window_covered,
        "stress_2022_sample_available": includes_2022_stress,
        "trial_2y_missing_2022_stress_sample": not includes_2022_stress,
        "full_history_needed_for_final_answer": full_history_needed,
    }


def _build_conclusion_matrix(
    *,
    coverage: Mapping[str, Any],
    prior_conclusion: Mapping[str, Any],
    feature_variation: Mapping[str, Any],
    bucket_balance: Mapping[str, Any],
    outcome_dominance: Mapping[str, Any],
    event_counts: Mapping[str, Any],
    baseline_increment: Mapping[str, Any],
    benchmark_consistency: Mapping[str, Any],
    stress_window: Mapping[str, Any],
    policy: Mapping[str, Any],
    benchmark_environment_status: str,
) -> dict[str, Any]:
    reason = _classify_local_signal_evidence_reason(
        feature_variation=feature_variation,
        bucket_balance=bucket_balance,
        outcome_dominance=outcome_dominance,
        event_counts=event_counts,
        baseline_increment=baseline_increment,
        benchmark_consistency=benchmark_consistency,
    )
    trial_feature_value = _classify_trial_feature_value(
        feature_variation=feature_variation,
        bucket_balance=bucket_balance,
        event_counts=event_counts,
        baseline_increment=baseline_increment,
        benchmark_consistency=benchmark_consistency,
    )
    purchase_recommendation, purchase_rationale = _classify_purchase_decision(
        prior_conclusion=prior_conclusion,
        feature_variation=feature_variation,
        stress_window=stress_window,
        trial_feature_value=trial_feature_value,
    )
    return {
        "schema_version": "norgate_2y_partial_evidence_conclusion_matrix.v1",
        "report_type": "norgate_2y_partial_evidence_conclusion_matrix",
        "status": "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_READY",
        "generated_at": partial._now(),
        "review_policy": str(policy.get("policy_id", "")),
        "benchmark_environment_status": benchmark_environment_status,
        "source_engineering_useful": bool(prior_conclusion.get("source_engineering_useful")),
        "source_feature_useful_2y": prior_conclusion.get("source_feature_useful_2y", "weak"),
        "purchase_platinum_evidence_strength": prior_conclusion.get(
            "purchase_platinum_evidence_strength",
            "weak",
        ),
        "engineering_validated": bool(prior_conclusion.get("engineering_validated")),
        "feature_numeric_validated": bool(prior_conclusion.get("feature_numeric_validated")),
        "local_signal_evidence": prior_conclusion.get("local_signal_evidence", "none"),
        "local_signal_evidence_reason": reason,
        "trial_2y_feature_value": trial_feature_value,
        "full_history_needed_for_final_answer": bool(
            stress_window.get("full_history_needed_for_final_answer")
        ),
        "purchase_platinum_recommendation": purchase_recommendation,
        "purchase_rationale": purchase_rationale,
        "purchase_decision_owner_approval_required": True,
        "earliest_price_date": coverage.get("earliest_price_date", ""),
        "latest_price_date": coverage.get("latest_price_date", ""),
        "member_day_coverage_ratio": coverage.get("member_day_coverage_ratio", 0.0),
        "missing_price_ratio": coverage.get("missing_price_ratio", 1.0),
        "failed_join_count": coverage.get("failed_join_count", 0),
        "feature_variation_sufficient": bool(feature_variation.get("variation_sufficient")),
        "bucket_sample_sufficient": bool(bucket_balance.get("bucket_sample_sufficient")),
        "outcome_dominated_by_few_days": bool(
            outcome_dominance.get("outcome_dominated_by_few_days")
        ),
        "event_count_sufficient": bool(event_counts.get("event_count_sufficient")),
        "baseline_increment_direction": baseline_increment.get("baseline_increment_direction"),
        "benchmark_signal_consistent": bool(
            benchmark_consistency.get("benchmark_signal_consistent")
        ),
        "stress_2022_sample_available": bool(stress_window.get("stress_2022_sample_available")),
        "trial_price_history_limited_to_2y": True,
        "primary_window_start": stress_window.get("primary_window_start", "2021-02-22"),
        "research_window_id": "norgate_trial_2y_partial",
        **PARTIAL_EVIDENCE_SAFETY_BOUNDARY,
        "research_audit_metadata": _audit_metadata(policy),
    }


def _build_review_payload(
    *,
    coverage: Mapping[str, Any],
    prior_conclusion: Mapping[str, Any],
    feature_variation: Mapping[str, Any],
    bucket_balance: Mapping[str, Any],
    outcome_dominance: Mapping[str, Any],
    event_counts: Mapping[str, Any],
    baseline_increment: Mapping[str, Any],
    benchmark_consistency: Mapping[str, Any],
    stress_window: Mapping[str, Any],
    benchmark_rows: Sequence[Mapping[str, Any]],
    conclusion: Mapping[str, Any],
    policy: Mapping[str, Any],
    benchmark_environment_status: str,
) -> dict[str, Any]:
    return {
        "schema_version": "norgate_2y_partial_evidence_review.v1",
        "report_type": "norgate_2y_partial_evidence_review",
        "status": "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_READY",
        "generated_at": partial._now(),
        "review_policy": str(policy.get("policy_id", "")),
        "benchmark_environment_status": benchmark_environment_status,
        "coverage_summary": {
            "earliest_price_date": coverage.get("earliest_price_date", ""),
            "latest_price_date": coverage.get("latest_price_date", ""),
            "member_day_coverage_ratio": coverage.get("member_day_coverage_ratio", 0.0),
            "missing_price_ratio": coverage.get("missing_price_ratio", 1.0),
            "failed_join_count": coverage.get("failed_join_count", 0),
        },
        "prior_2267_conclusion": {
            "local_signal_evidence": prior_conclusion.get("local_signal_evidence"),
            "source_feature_useful_2y": prior_conclusion.get("source_feature_useful_2y"),
            "purchase_platinum_evidence_strength": prior_conclusion.get(
                "purchase_platinum_evidence_strength"
            ),
        },
        "feature_variation_review": feature_variation,
        "bucket_balance_review": bucket_balance,
        "outcome_dominance_review": outcome_dominance,
        "event_count_review": event_counts,
        "baseline_increment_review": baseline_increment,
        "benchmark_consistency_review": benchmark_consistency,
        "benchmark_consistency_rows": list(benchmark_rows),
        "stress_window_review": stress_window,
        "conclusion_matrix": dict(conclusion),
        "research_window_id": "norgate_trial_2y_partial",
        **PARTIAL_EVIDENCE_SAFETY_BOUNDARY,
        "research_audit_metadata": _audit_metadata(policy),
    }


def _build_decision_memo_payload(
    review: Mapping[str, Any],
    conclusion: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "norgate_platinum_decision_memo.v1",
        "report_type": "norgate_platinum_decision_memo",
        "status": "NORGATE_PLATINUM_OWNER_DECISION_MEMO_READY"
        if conclusion.get("status") != "NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_BLOCKED"
        else "NORGATE_PLATINUM_OWNER_DECISION_MEMO_BLOCKED",
        "generated_at": partial._now(),
        "review_policy": str(policy.get("policy_id", "")),
        "decision_summary": {
            "purchase_platinum_recommendation": conclusion.get(
                "purchase_platinum_recommendation"
            ),
            "purchase_rationale": conclusion.get("purchase_rationale"),
            "purchase_decision_owner_approval_required": True,
            "purchase_allowed_without_owner_approval": False,
            "full_history_needed_for_final_answer": conclusion.get(
                "full_history_needed_for_final_answer"
            ),
            "trial_2y_feature_value": conclusion.get("trial_2y_feature_value"),
            "local_signal_evidence_reason": conclusion.get("local_signal_evidence_reason"),
        },
        "evidence_summary": {
            "feature_variation_sufficient": conclusion.get("feature_variation_sufficient"),
            "bucket_sample_sufficient": conclusion.get("bucket_sample_sufficient"),
            "outcome_dominated_by_few_days": conclusion.get("outcome_dominated_by_few_days"),
            "event_count_sufficient": conclusion.get("event_count_sufficient"),
            "baseline_increment_direction": conclusion.get("baseline_increment_direction"),
            "benchmark_signal_consistent": conclusion.get("benchmark_signal_consistent"),
            "stress_2022_sample_available": conclusion.get("stress_2022_sample_available"),
        },
        "review_artifact_status": review.get("status"),
        "research_window_id": "norgate_trial_2y_partial",
        **PARTIAL_EVIDENCE_SAFETY_BOUNDARY,
        "research_audit_metadata": _audit_metadata(policy),
    }


def _classify_local_signal_evidence_reason(
    *,
    feature_variation: Mapping[str, Any],
    bucket_balance: Mapping[str, Any],
    outcome_dominance: Mapping[str, Any],
    event_counts: Mapping[str, Any],
    baseline_increment: Mapping[str, Any],
    benchmark_consistency: Mapping[str, Any],
) -> str:
    if not bool(bucket_balance.get("bucket_sample_sufficient")) or not bool(
        event_counts.get("event_count_sufficient")
    ):
        return "insufficient_sample"
    if not bool(feature_variation.get("variation_sufficient")):
        return "metric_design_issue"
    if bool(outcome_dominance.get("outcome_dominated_by_few_days")):
        return "inconclusive"
    if (
        baseline_increment.get("baseline_increment_direction")
        in {"worse_false_signal_rates", "near_zero", "unstable"}
        and not bool(benchmark_consistency.get("benchmark_signal_consistent"))
    ):
        return "no_incremental_value"
    return "inconclusive"


def _classify_trial_feature_value(
    *,
    feature_variation: Mapping[str, Any],
    bucket_balance: Mapping[str, Any],
    event_counts: Mapping[str, Any],
    baseline_increment: Mapping[str, Any],
    benchmark_consistency: Mapping[str, Any],
) -> str:
    if not bool(feature_variation.get("variation_sufficient")):
        return "weak"
    if not bool(bucket_balance.get("bucket_sample_sufficient")) or not bool(
        event_counts.get("event_count_sufficient")
    ):
        return "weak"
    if bool(benchmark_consistency.get("benchmark_signal_consistent")) and bool(
        baseline_increment.get("incremental_value_positive")
    ):
        return "strong"
    if bool(benchmark_consistency.get("benchmark_signal_consistent")):
        return "moderate"
    return "weak"


def _classify_purchase_decision(
    *,
    prior_conclusion: Mapping[str, Any],
    feature_variation: Mapping[str, Any],
    stress_window: Mapping[str, Any],
    trial_feature_value: str,
) -> tuple[str, str]:
    if trial_feature_value == "strong":
        return "yes", "strong_trial_signal"
    if (
        bool(prior_conclusion.get("source_engineering_useful"))
        and bool(prior_conclusion.get("feature_numeric_validated"))
        and bool(feature_variation.get("variation_sufficient"))
        and bool(stress_window.get("full_history_needed_for_final_answer"))
        and bool(stress_window.get("trial_2y_missing_2022_stress_sample"))
    ):
        return "yes", "stress_window_required"
    if bool(prior_conclusion.get("source_engineering_useful")):
        return "defer", "engineering_only"
    return "defer", "weak_evidence"


def _prepared_signal_frame(feature_frame: pd.DataFrame) -> pd.DataFrame:
    frame = feature_frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    for column in ("pct_above_ma50", "breadth_momentum"):
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame = partial._assign_breadth_buckets(frame)
    frame = partial._assign_deterioration_flag(frame)
    return frame[["date", "pct_above_ma50", "breadth_momentum", "breadth_bucket"]].copy()


def _benchmark_forward_frame(prices: pd.DataFrame, horizons: Sequence[int]) -> pd.DataFrame:
    frame = _normalize_price_frame(prices)
    close = frame["Close"]
    output = pd.DataFrame({"date": frame.index})
    for horizon in horizons:
        output[f"next_{horizon}d_return"] = (
            close.shift(-horizon).to_numpy() / close.to_numpy() - 1
        )
    return output


def _bucket_return_stats(frame: pd.DataFrame, return_column: str) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for bucket in ("low", "mid", "high"):
        series = pd.to_numeric(
            frame.loc[frame["breadth_bucket"] == bucket, return_column],
            errors="coerce",
        ).dropna()
        output[bucket] = {
            "sample_count": int(len(series)),
            "avg_return": float(series.mean()) if len(series) else math.nan,
        }
    return output


def _return_dominance_stats(series: pd.Series, policy: Mapping[str, Any]) -> dict[str, Any]:
    returns = pd.to_numeric(series, errors="coerce").dropna()
    if returns.empty:
        return {
            "top_decile_abs_contribution_share": None,
            "max_single_day_abs_contribution_share": None,
            "outlier_dominated": False,
        }
    top_fraction = _policy_float(
        policy,
        ("outcome_dominance_policy", "top_abs_contribution_fraction"),
        0.1,
    )
    max_top_share = _policy_float(
        policy,
        ("outcome_dominance_policy", "max_top_decile_abs_contribution_share"),
        0.5,
    )
    max_single_share = _policy_float(
        policy,
        ("outcome_dominance_policy", "max_single_day_abs_contribution_share"),
        0.25,
    )
    abs_returns = returns.abs().sort_values(ascending=False)
    denominator = float(abs_returns.sum())
    if not denominator:
        top_share = 0.0
        single_share = 0.0
    else:
        top_n = max(1, math.ceil(len(abs_returns) * top_fraction))
        top_share = float(abs_returns.head(top_n).sum() / denominator)
        single_share = float(abs_returns.iloc[0] / denominator)
    return {
        "top_decile_abs_contribution_share": partial._round_or_none(top_share),
        "max_single_day_abs_contribution_share": partial._round_or_none(single_share),
        "outlier_dominated": top_share > max_top_share or single_share > max_single_share,
    }


def _recovery_event_count(feature_frame: pd.DataFrame) -> int:
    series = pd.to_numeric(feature_frame.get("breadth_momentum"), errors="coerce").dropna()
    if len(series) < 3:
        return 0
    threshold = series.quantile(2 / 3)
    return int((series >= threshold).sum())


def _resolve_benchmark_price_frames(
    *,
    policy: Mapping[str, Any],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    benchmark_price_frames: Mapping[str, pd.DataFrame] | None,
) -> tuple[dict[str, pd.DataFrame], str]:
    if benchmark_price_frames is not None:
        return {
            symbol: _normalize_price_frame(frame)
            for symbol, frame in benchmark_price_frames.items()
            if not frame.empty
        }, "INJECTED_BENCHMARK_PRICE_FRAMES"

    connector = NorgateConnector()
    environment = connector.inspect_environment()
    if environment.status != "NORGATE_ENV_READY":
        return {}, environment.status
    module = connector._module
    assert module is not None
    frames: dict[str, pd.DataFrame] = {}
    for symbol in _benchmark_symbols(policy):
        frame = partial._load_price_frame(module, symbol, start_ts=start_ts, end_ts=end_ts)
        if not frame.empty:
            frames[symbol] = _normalize_price_frame(frame)
    return frames, environment.status


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prices = frame.copy()
    prices.index = pd.to_datetime(prices.index).normalize()
    prices = prices.sort_index()
    if "Close" not in prices.columns:
        return pd.DataFrame()
    return prices[["Close"]].dropna()


def _read_feature_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if "date" not in frame.columns:
        return pd.DataFrame()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def _read_json(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _load_policy(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _write_review_artifacts(
    *,
    output_root: Path,
    docs_root: Path,
    inputs_root: Path,
    review: Mapping[str, Any],
    memo: Mapping[str, Any],
    conclusion: Mapping[str, Any],
    benchmark_rows: Sequence[Mapping[str, Any]],
) -> None:
    partial._write_json(output_root / "norgate_2y_partial_evidence_review.json", review)
    partial._write_csv(output_root / "norgate_2y_benchmark_consistency.csv", benchmark_rows)
    partial._write_json(
        output_root / "norgate_2y_partial_evidence_conclusion_matrix.json",
        conclusion,
    )
    partial._write_yaml(inputs_root / "norgate_2y_partial_evidence_review.yaml", review)
    partial._write_yaml(inputs_root / "norgate_platinum_decision_memo.yaml", memo)
    partial._write_yaml(
        inputs_root / "norgate_2y_partial_evidence_conclusion_matrix.yaml",
        conclusion,
    )
    partial._write_markdown(
        docs_root / "norgate_2y_partial_evidence_review.md",
        _render_evidence_review(review),
    )
    partial._write_markdown(
        docs_root / "norgate_platinum_decision_memo.md",
        _render_decision_memo(memo, conclusion),
    )


def _render_evidence_review(review: Mapping[str, Any]) -> str:
    conclusion = dict(review.get("conclusion_matrix", {}))
    feature = dict(review.get("feature_variation_review", {}))
    bucket = dict(review.get("bucket_balance_review", {}))
    outcome = dict(review.get("outcome_dominance_review", {}))
    events = dict(review.get("event_count_review", {}))
    baseline = dict(review.get("baseline_increment_review", {}))
    benchmark = dict(review.get("benchmark_consistency_review", {}))
    stress = dict(review.get("stress_window_review", {}))
    return "\n".join(
        [
            "# Norgate 2Y Partial Evidence Review",
            "",
            f"- status: `{review.get('status')}`",
            f"- local_signal_evidence_reason: `{conclusion.get('local_signal_evidence_reason')}`",
            f"- trial_2y_feature_value: `{conclusion.get('trial_2y_feature_value')}`",
            (
                "- full_history_needed_for_final_answer: "
                f"`{conclusion.get('full_history_needed_for_final_answer')}`"
            ),
            "",
            "## 复盘结论",
            "",
            (
                "- feature_variation_sufficient: "
                f"`{feature.get('variation_sufficient')}`；"
                f"nonflat_feature_count: `{feature.get('nonflat_feature_count')}`"
            ),
            (
                "- bucket_sample_sufficient: "
                f"`{bucket.get('bucket_sample_sufficient')}`；"
                f"bucket_imbalance_ratio: `{bucket.get('bucket_imbalance_ratio')}`"
            ),
            (
                "- outcome_dominated_by_few_days: "
                f"`{outcome.get('outcome_dominated_by_few_days')}`；"
                "dominated_benchmark_horizon_count: "
                f"`{outcome.get('dominated_benchmark_horizon_count')}`"
            ),
            (
                "- event_count_sufficient: "
                f"`{events.get('event_count_sufficient')}`；"
                f"deterioration_event_count: `{events.get('deterioration_event_count')}`；"
                f"recovery_event_count: `{events.get('recovery_event_count')}`"
            ),
            (
                "- baseline_increment_direction: "
                f"`{baseline.get('baseline_increment_direction')}`；"
                f"false_risk_off_delta: `{baseline.get('false_risk_off_delta')}`；"
                f"false_risk_on_delta: `{baseline.get('false_risk_on_delta')}`"
            ),
            (
                "- benchmark_signal_consistent: "
                f"`{benchmark.get('benchmark_signal_consistent')}`；"
                f"supporting_benchmark_count: `{benchmark.get('supporting_benchmark_count')}`"
            ),
            (
                "- stress_2022_sample_available: "
                f"`{stress.get('stress_2022_sample_available')}`；"
                f"earliest_price_date: `{stress.get('earliest_price_date')}`"
            ),
            "",
            "2Y trial 可以解释局部 feature 行为和购买 full-history 的必要性，"
            "但不能替代 2021 primary-window validation。",
        ]
    ) + "\n"


def _render_decision_memo(memo: Mapping[str, Any], conclusion: Mapping[str, Any]) -> str:
    decision = dict(memo.get("decision_summary", {}))
    evidence = dict(memo.get("evidence_summary", {}))
    return "\n".join(
        [
            "# Norgate Platinum Decision Memo",
            "",
            f"- status: `{memo.get('status')}`",
            (
                "- purchase_platinum_recommendation: "
                f"`{decision.get('purchase_platinum_recommendation')}`"
            ),
            f"- purchase_rationale: `{decision.get('purchase_rationale')}`",
            (
                "- purchase_decision_owner_approval_required: "
                f"`{decision.get('purchase_decision_owner_approval_required')}`"
            ),
            (
                "- purchase_allowed_without_owner_approval: "
                f"`{memo.get('purchase_allowed_without_owner_approval')}`"
            ),
            "",
            "## Owner Decision Context",
            "",
            f"- local_signal_evidence_reason: `{decision.get('local_signal_evidence_reason')}`",
            f"- trial_2y_feature_value: `{decision.get('trial_2y_feature_value')}`",
            (
                "- full_history_needed_for_final_answer: "
                f"`{decision.get('full_history_needed_for_final_answer')}`"
            ),
            f"- feature_variation_sufficient: `{evidence.get('feature_variation_sufficient')}`",
            f"- benchmark_signal_consistent: `{evidence.get('benchmark_signal_consistent')}`",
            f"- stress_2022_sample_available: `{evidence.get('stress_2022_sample_available')}`",
            "",
            "结论：当前 recommendation 只面向 owner 是否购买正式历史数据。"
            "即使 recommendation 为 `yes`，也不允许自动购买、自动升级 provider、"
            "恢复 first-layer、paper-shadow、production 或 broker action。",
            "",
            "## Gate Status",
            "",
            f"- primary_window_validated: `{conclusion.get('primary_window_validated')}`",
            (
                "- model_ready_for_2021_primary_window: "
                f"`{conclusion.get('model_ready_for_2021_primary_window')}`"
            ),
            f"- reopen_gate_allowed: `{conclusion.get('reopen_gate_allowed')}`",
            f"- promotion_allowed: `{conclusion.get('promotion_allowed')}`",
            f"- paper_shadow_allowed: `{conclusion.get('paper_shadow_allowed')}`",
            f"- production_allowed: `{conclusion.get('production_allowed')}`",
            f"- broker_action: `{conclusion.get('broker_action')}`",
        ]
    ) + "\n"


def _audit_metadata(policy: Mapping[str, Any]) -> dict[str, Any]:
    policy_id = str(policy.get("policy_id", "norgate_2y_partial_evidence_review_policy_v1"))
    return {
        "modified_layer": "validation_only",
        "modified_channel": "norgate_2y_partial_evidence_review",
        "frozen_first_layer_version": "first_layer_channel_archive_policy_v1",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": "norgate_trial_2y_partial",
        "label_version": "norgate_trial_no_labels_v1",
        "feature_set_version": "norgate_trial_breadth_features_2y_v1",
        "model_version": "norgate_2y_partial_evidence_review_v1",
        "threshold_policy": policy_id,
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "candidate_count": 0,
        "pre_registered_selection_rule": policy_id,
        "selection_rule_version": policy_id,
        "boundary_contract_version": "norgate_source_contract_v1",
    }


def _benchmark_symbols(policy: Mapping[str, Any]) -> list[str]:
    return _policy_list(
        policy,
        ("benchmark_consistency_policy", "benchmark_symbols"),
        ["QQQ", "SPY", "SMH"],
    )


def _policy_value(policy: Mapping[str, Any], path: Sequence[str], default: Any) -> Any:
    value: Any = policy
    for key in path:
        if not isinstance(value, Mapping):
            return default
        value = value.get(key)
    return default if value is None else value


def _policy_float(policy: Mapping[str, Any], path: Sequence[str], default: float) -> float:
    return partial._policy_float(policy, path, default)


def _policy_int(policy: Mapping[str, Any], path: Sequence[str], default: int) -> int:
    return partial._policy_int(policy, path, default)


def _policy_list(
    policy: Mapping[str, Any],
    path: Sequence[str],
    default: Sequence[str],
) -> list[str]:
    value = _policy_value(policy, path, list(default))
    return [str(item) for item in value] if isinstance(value, list) else list(default)


def _policy_int_list(
    policy: Mapping[str, Any],
    path: Sequence[str],
    default: Sequence[int],
) -> list[int]:
    value = _policy_value(policy, path, list(default))
    if not isinstance(value, list):
        return list(default)
    return [int(item) for item in value if partial._finite(item)]


def _float(value: Any) -> float:
    return float(value) if partial._finite(value) else math.nan
