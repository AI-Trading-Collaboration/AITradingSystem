from __future__ import annotations

import csv
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.vendor_adapters.norgate_connector import (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    DEFAULT_RESEARCH_DOCS_ROOT,
    DEFAULT_RESEARCH_INPUTS_ROOT,
    NorgateConnector,
    NorgateEnvironment,
    resolve_norgate_index_id,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PARTIAL_EFFECTIVENESS_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "norgate_trial_partial_effectiveness_policy.yaml"
)

PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY = {
    "research_only": True,
    "engineering_validated": False,
    "feature_numeric_validated": False,
    "local_signal_evidence": "blocked",
    "primary_window_validated": False,
    "model_ready_for_2021_primary_window": False,
    "reopen_gate_allowed": False,
    "candidate_count": 0,
    "first_layer_reopen_allowed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def run_norgate_trial_partial_effectiveness(
    *,
    index_id: str = "nasdaq100",
    output_root: Path = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Path = DEFAULT_RESEARCH_INPUTS_ROOT,
    policy_path: Path = DEFAULT_PARTIAL_EFFECTIVENESS_POLICY_PATH,
    max_symbols: int = 0,
) -> dict[str, Any]:
    """Generate summary-only Norgate 2Y partial effectiveness artifacts."""

    _ensure_roots(output_root, docs_root, inputs_root)
    policy = _load_policy(policy_path)
    connector = NorgateConnector()
    environment = connector.inspect_environment()
    index_name = resolve_norgate_index_id(index_id)
    if environment.status != "NORGATE_ENV_READY":
        return _write_blocked_partial_effectiveness_pack(
            environment=environment,
            index_id=index_name,
            output_root=output_root,
            docs_root=docs_root,
            inputs_root=inputs_root,
            policy=policy,
        )
    module = connector._module
    assert module is not None

    qqq_prices = _load_price_frame(module, "QQQ")
    if qqq_prices.empty:
        blocked_environment = NorgateEnvironment(
            module_present=environment.module_present,
            module_version=environment.module_version,
            database_available=environment.database_available,
            database_names=environment.database_names,
            status="NORGATE_QQQ_PRICE_HISTORY_UNAVAILABLE",
            warnings=environment.warnings,
            errors=environment.errors,
        )
        return _write_blocked_partial_effectiveness_pack(
            environment=blocked_environment,
            index_id=index_name,
            output_root=output_root,
            docs_root=docs_root,
            inputs_root=inputs_root,
            policy=policy,
        )

    start_ts = qqq_prices.index.min()
    end_ts = qqq_prices.index.max()
    trading_dates = list(qqq_prices.index)
    candidate_symbols = _candidate_symbols(module, max_symbols=max_symbols)
    membership_by_symbol, membership_failures = _scan_membership_by_symbol(
        module,
        candidate_symbols,
        index_name,
        start_ts.date(),
        end_ts.date(),
    )
    member_symbols = sorted(membership_by_symbol)
    price_frames = {
        symbol: _load_price_frame(module, symbol, start_ts=start_ts, end_ts=end_ts)
        for symbol in member_symbols
    }
    price_frames = {symbol: frame for symbol, frame in price_frames.items() if not frame.empty}

    coverage = _build_coverage_report(
        environment=environment,
        index_id=index_name,
        qqq_prices=qqq_prices,
        membership_by_symbol=membership_by_symbol,
        price_frames=price_frames,
        trading_dates=trading_dates,
        membership_scan_symbol_count=len(candidate_symbols),
        membership_scan_failure_count=membership_failures,
        policy=policy,
    )
    features = _build_breadth_feature_report(
        index_id=index_name,
        qqq_prices=qqq_prices,
        membership_by_symbol=membership_by_symbol,
        price_frames=price_frames,
        trading_dates=trading_dates,
        coverage=coverage,
        policy=policy,
    )
    local_signal = _build_local_signal_report(
        feature_rows=features["rows"],
        qqq_prices=qqq_prices,
        coverage=coverage,
        policy=policy,
    )
    conclusion = _build_conclusion_matrix(
        coverage=coverage,
        features=features,
        local_signal=local_signal,
        policy=policy,
    )
    _write_partial_effectiveness_artifacts(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        coverage=coverage,
        features=features,
        local_signal=local_signal,
        conclusion=conclusion,
    )
    return conclusion


def _write_blocked_partial_effectiveness_pack(
    *,
    environment: NorgateEnvironment,
    index_id: str,
    output_root: Path,
    docs_root: Path,
    inputs_root: Path,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    coverage = {
        "schema_version": "norgate_trial_partial_effectiveness_coverage.v1",
        "report_type": "norgate_trial_partial_effectiveness_coverage",
        "status": "NORGATE_2Y_PARTIAL_EFFECTIVENESS_BLOCKED",
        "generated_at": _now(),
        "environment_status": environment.status,
        "index_id": index_id,
        "earliest_price_date": "",
        "latest_price_date": "",
        "member_day_coverage_ratio": 0.0,
        "missing_price_ratio": 1.0,
        "failed_join_count": 0,
        "membership_scan_failure_count": 0,
        "raw_member_symbols_committed": False,
        "raw_vendor_prices_committed": False,
        **PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY,
    }
    features = {
        "schema_version": "norgate_trial_breadth_feature_report_2y.v1",
        "report_type": "norgate_trial_breadth_feature_report_2y",
        "status": "NORGATE_2Y_BREADTH_FEATURES_BLOCKED",
        "generated_at": _now(),
        "rows": [],
        **PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY,
    }
    local_signal = {
        "schema_version": "norgate_trial_local_signal_report_2y.v1",
        "report_type": "norgate_trial_local_signal_report_2y",
        "status": "NORGATE_2Y_LOCAL_SIGNAL_BLOCKED",
        "generated_at": _now(),
        "breadth_bucket_vs_forward_return": [],
        "breadth_deterioration_vs_future_drawdown": [],
        "baseline_comparison": {},
        **PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY,
    }
    conclusion = _build_conclusion_matrix(
        coverage=coverage,
        features=features,
        local_signal=local_signal,
        policy=policy,
    )
    _write_partial_effectiveness_artifacts(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        coverage=coverage,
        features=features,
        local_signal=local_signal,
        conclusion=conclusion,
    )
    return conclusion


def _build_coverage_report(
    *,
    environment: NorgateEnvironment,
    index_id: str,
    qqq_prices: pd.DataFrame,
    membership_by_symbol: Mapping[str, set[pd.Timestamp]],
    price_frames: Mapping[str, pd.DataFrame],
    trading_dates: Sequence[pd.Timestamp],
    membership_scan_symbol_count: int,
    membership_scan_failure_count: int,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    by_date: list[dict[str, Any]] = []
    total_member_days = 0
    covered_member_days = 0
    failed_join_count = 0
    for day in trading_dates:
        members = _members_on_day(membership_by_symbol, day)
        member_count = len(members)
        covered_count = sum(
            1
            for symbol in members
            if symbol in price_frames and _has_price_on_day(price_frames[symbol], day)
        )
        missing_count = max(member_count - covered_count, 0)
        total_member_days += member_count
        covered_member_days += covered_count
        failed_join_count += missing_count
        by_date.append(
            {
                "date": _date_text(day),
                "index_id": index_id,
                "member_count": member_count,
                "covered_member_count": covered_count,
                "missing_price_count": missing_count,
                "member_day_coverage_ratio": _ratio(covered_count, member_count),
                "missing_price_ratio": _ratio(missing_count, member_count),
            }
        )
    coverage_ratio = _ratio(covered_member_days, total_member_days)
    min_coverage = _policy_float(
        policy,
        ("coverage_policy", "min_member_day_coverage_ratio_for_feature_numeric_validated"),
        0.9,
    )
    return {
        "schema_version": "norgate_trial_partial_effectiveness_coverage.v1",
        "report_type": "norgate_trial_partial_effectiveness_coverage",
        "status": (
            "NORGATE_2Y_COVERAGE_READY"
            if total_member_days and coverage_ratio >= min_coverage
            else "NORGATE_2Y_COVERAGE_WEAK_OR_BLOCKED"
        ),
        "generated_at": _now(),
        "environment_status": environment.status,
        "index_id": index_id,
        "earliest_price_date": _date_text(qqq_prices.index.min()),
        "latest_price_date": _date_text(qqq_prices.index.max()),
        "trading_day_count": len(trading_dates),
        "membership_scan_symbol_count": membership_scan_symbol_count,
        "membership_scan_failure_count": membership_scan_failure_count,
        "historical_member_symbol_count": len(membership_by_symbol),
        "historical_member_symbols_hash": _hash_symbols(sorted(membership_by_symbol)),
        "total_member_days": total_member_days,
        "covered_member_days": covered_member_days,
        "member_day_coverage_ratio": coverage_ratio,
        "missing_price_ratio": _ratio(failed_join_count, total_member_days),
        "failed_join_count": failed_join_count,
        "coverage_by_date": by_date,
        "raw_member_symbols_committed": False,
        "raw_vendor_prices_committed": False,
        "engineering_validated": environment.status == "NORGATE_ENV_READY"
        and bool(total_member_days),
        "feature_numeric_validated": bool(total_member_days) and coverage_ratio >= min_coverage,
        "local_signal_evidence": "not_evaluated",
        "primary_window_validated": False,
        "model_ready_for_2021_primary_window": False,
        "reopen_gate_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "production_effect": "none",
        "research_only": True,
        "candidate_count": 0,
    }


def _build_breadth_feature_report(
    *,
    index_id: str,
    qqq_prices: pd.DataFrame,
    membership_by_symbol: Mapping[str, set[pd.Timestamp]],
    price_frames: Mapping[str, pd.DataFrame],
    trading_dates: Sequence[pd.Timestamp],
    coverage: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    pct_above_ma50_by_date: dict[pd.Timestamp, float] = {}
    for day in trading_dates:
        members = _members_on_day(membership_by_symbol, day)
        frames = [(symbol, price_frames[symbol]) for symbol in members if symbol in price_frames]
        pct_above_ma20 = _pct_above_ma(frames, day, "ma20")
        pct_above_ma50 = _pct_above_ma(frames, day, "ma50")
        pct_above_ma200 = _pct_above_ma(frames, day, "ma200")
        returns = [
            float(frame.at[day, "return_1d"])
            for _, frame in frames
            if day in frame.index and _finite(frame.at[day, "return_1d"])
        ]
        cap_weight_proxy_return = _cap_weight_proxy_return(frames, day)
        pct_above_ma50_by_date[day] = pct_above_ma50
        row = {
            "date": _date_text(day),
            "index_id": index_id,
            "member_count": len(members),
            "priced_member_count": len(frames),
            "pct_above_ma20": _round_or_none(pct_above_ma20),
            "pct_above_ma50": _round_or_none(pct_above_ma50),
            "pct_above_ma200": _round_or_none(pct_above_ma200),
            "equal_weight_return": _round_or_none(_mean(returns)),
            "cap_weight_proxy_return": _round_or_none(cap_weight_proxy_return),
            "advance_decline_proxy": _round_or_none(_advance_decline_proxy(returns)),
            "breadth_momentum": None,
            "qqq_return": _round_or_none(_value_at(qqq_prices, day, "return_1d")),
        }
        rows.append(row)
    for index, row in enumerate(rows):
        if index >= 20:
            current = pct_above_ma50_by_date[pd.Timestamp(row["date"])]
            previous = pct_above_ma50_by_date[pd.Timestamp(rows[index - 20]["date"])]
            row["breadth_momentum"] = _round_or_none(current - previous)
    min_feature_days = _policy_int(
        policy,
        ("coverage_policy", "min_feature_days_for_local_signal_review"),
        100,
    )
    numeric_days = len([row for row in rows if row.get("pct_above_ma50") is not None])
    feature_numeric_validated = bool(coverage.get("feature_numeric_validated")) and (
        numeric_days >= min_feature_days
    )
    return {
        "schema_version": "norgate_trial_breadth_feature_report_2y.v1",
        "report_type": "norgate_trial_breadth_feature_report_2y",
        "status": (
            "NORGATE_2Y_BREADTH_FEATURES_NUMERIC_VALIDATED"
            if feature_numeric_validated
            else "NORGATE_2Y_BREADTH_FEATURES_WEAK_OR_BLOCKED"
        ),
        "generated_at": _now(),
        "feature_policy": policy.get("policy_id"),
        "numeric_feature_day_count": numeric_days,
        "trial_price_history_limited_to_2y": True,
        "primary_window_validated": False,
        "feature_numeric_validated": feature_numeric_validated,
        "engineering_validated": bool(coverage.get("engineering_validated")),
        "local_signal_evidence": "not_evaluated",
        "raw_member_symbols_committed": False,
        "raw_vendor_prices_committed": False,
        "rows": rows,
        **_feature_safety_boundary(feature_numeric_validated),
    }


def _build_local_signal_report(
    *,
    feature_rows: Sequence[Mapping[str, Any]],
    qqq_prices: pd.DataFrame,
    coverage: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    frame = pd.DataFrame([dict(row) for row in feature_rows])
    if frame.empty or not coverage.get("feature_numeric_validated"):
        return _blocked_local_signal_report("feature_numeric_not_validated")
    frame["date"] = pd.to_datetime(frame["date"])
    qqq = qqq_prices.copy()
    qqq = qqq.sort_index()
    frame = frame.merge(_qqq_forward_frame(qqq), on="date", how="left")
    frame = _assign_breadth_buckets(frame)
    frame = _assign_deterioration_flag(frame)
    bucket_rows = _bucket_forward_return_rows(frame)
    deterioration_rows = _deterioration_rows(frame)
    baseline = _baseline_comparison(frame, qqq, policy)
    local_signal_evidence = _classify_local_signal_evidence(
        bucket_rows=bucket_rows,
        baseline=baseline,
    )
    return {
        "schema_version": "norgate_trial_local_signal_report_2y.v1",
        "report_type": "norgate_trial_local_signal_report_2y",
        "status": "NORGATE_2Y_LOCAL_SIGNAL_REPORT_READY",
        "generated_at": _now(),
        "feature_policy": policy.get("policy_id"),
        "sample_day_count": int(len(frame)),
        "breadth_bucket_vs_forward_return": bucket_rows,
        "breadth_deterioration_vs_future_drawdown": deterioration_rows,
        "baseline_comparison": baseline,
        "local_signal_evidence": local_signal_evidence,
        "primary_window_validated": False,
        "model_ready_for_2021_primary_window": False,
        "reopen_gate_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "research_only": True,
        "candidate_count": 0,
    }


def _build_conclusion_matrix(
    *,
    coverage: Mapping[str, Any],
    features: Mapping[str, Any],
    local_signal: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    engineering_validated = bool(coverage.get("engineering_validated"))
    feature_numeric_validated = bool(features.get("feature_numeric_validated"))
    local_signal_evidence = str(local_signal.get("local_signal_evidence", "blocked"))
    source_feature_useful_2y: bool | str
    if feature_numeric_validated and local_signal_evidence in {"moderate", "strong"}:
        source_feature_useful_2y = True
    elif feature_numeric_validated:
        source_feature_useful_2y = "weak"
    else:
        source_feature_useful_2y = False
    if source_feature_useful_2y is True and local_signal_evidence == "strong":
        purchase_strength = "strong"
    elif engineering_validated and feature_numeric_validated:
        purchase_strength = "moderate"
    else:
        purchase_strength = "weak"
    return {
        "schema_version": "norgate_trial_partial_effectiveness_conclusion_matrix.v1",
        "report_type": "norgate_trial_partial_effectiveness_conclusion_matrix",
        "status": (
            "NORGATE_2Y_PARTIAL_EFFECTIVENESS_READY"
            if engineering_validated and feature_numeric_validated
            else "NORGATE_2Y_PARTIAL_EFFECTIVENESS_BLOCKED_OR_WEAK"
        ),
        "generated_at": _now(),
        "feature_policy": policy.get("policy_id"),
        "source_engineering_useful": engineering_validated,
        "source_feature_useful_2y": source_feature_useful_2y,
        "purchase_platinum_evidence_strength": purchase_strength,
        "engineering_validated": engineering_validated,
        "feature_numeric_validated": feature_numeric_validated,
        "local_signal_evidence": local_signal_evidence,
        "trial_price_history_limited_to_2y": True,
        "earliest_price_date": coverage.get("earliest_price_date", ""),
        "latest_price_date": coverage.get("latest_price_date", ""),
        "member_day_coverage_ratio": coverage.get("member_day_coverage_ratio", 0.0),
        "missing_price_ratio": coverage.get("missing_price_ratio", 1.0),
        "failed_join_count": coverage.get("failed_join_count", 0),
        "primary_window_start": "2021-02-22",
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
        "research_only": True,
        "interpretation": (
            "2Y trial can inform paid Platinum purchase value, but it does not "
            "replace 2021 primary-window validation."
        ),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "norgate_trial_partial_effectiveness",
            "frozen_first_layer_version": "first_layer_channel_archive_policy_v1",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": "norgate_trial_2y_partial",
            "label_version": "norgate_trial_no_labels_v1",
            "feature_set_version": "norgate_trial_breadth_features_2y_v1",
            "model_version": "norgate_trial_partial_effectiveness_v1",
            "threshold_policy": str(policy.get("policy_id", "")),
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": 0,
            "pre_registered_selection_rule": str(policy.get("policy_id", "")),
            "selection_rule_version": str(policy.get("policy_id", "")),
            "boundary_contract_version": "norgate_source_contract_v1",
        },
    }


def _write_partial_effectiveness_artifacts(
    *,
    output_root: Path,
    docs_root: Path,
    inputs_root: Path,
    coverage: Mapping[str, Any],
    features: Mapping[str, Any],
    local_signal: Mapping[str, Any],
    conclusion: Mapping[str, Any],
) -> None:
    coverage_rows = list(_records(coverage.get("coverage_by_date")))
    feature_rows = list(_records(features.get("rows")))
    bucket_rows = list(_records(local_signal.get("breadth_bucket_vs_forward_return")))
    deterioration_rows = list(
        _records(local_signal.get("breadth_deterioration_vs_future_drawdown"))
    )
    _write_json(output_root / "norgate_trial_partial_effectiveness_coverage_report.json", coverage)
    _write_csv(
        output_root / "norgate_trial_partial_effectiveness_coverage_by_date.csv",
        coverage_rows,
    )
    _write_json(output_root / "norgate_trial_breadth_feature_report_2y.json", features)
    _write_csv(output_root / "norgate_trial_breadth_feature_report_2y.csv", feature_rows)
    _write_json(output_root / "norgate_trial_local_signal_report_2y.json", local_signal)
    _write_csv(output_root / "norgate_trial_local_signal_bucket_report_2y.csv", bucket_rows)
    _write_csv(
        output_root / "norgate_trial_local_signal_deterioration_report_2y.csv",
        deterioration_rows,
    )
    _write_json(
        output_root / "norgate_trial_partial_effectiveness_conclusion_matrix.json",
        conclusion,
    )
    _write_yaml(
        inputs_root / "norgate_trial_partial_effectiveness_coverage_report.yaml",
        coverage,
    )
    _write_yaml(inputs_root / "norgate_trial_breadth_feature_report_2y.yaml", features)
    _write_yaml(inputs_root / "norgate_trial_local_signal_report_2y.yaml", local_signal)
    _write_yaml(
        inputs_root / "norgate_trial_partial_effectiveness_conclusion_matrix.yaml",
        conclusion,
    )
    _write_markdown(
        docs_root / "norgate_trial_partial_effectiveness_coverage_report.md",
        _render_coverage_review(coverage),
    )
    _write_markdown(
        docs_root / "norgate_trial_breadth_feature_report_2y.md",
        _render_feature_review(features),
    )
    _write_markdown(
        docs_root / "norgate_trial_local_signal_report_2y.md",
        _render_local_signal_review(local_signal),
    )
    _write_markdown(
        docs_root / "norgate_trial_partial_effectiveness_conclusion_matrix.md",
        _render_conclusion_review(conclusion),
    )


def _candidate_symbols(module: Any, *, max_symbols: int) -> list[str]:
    symbols: set[str] = set()
    for database_name in ("US Equities", "US Equities Delisted"):
        try:
            symbols.update(str(symbol) for symbol in module.database_symbols(database_name))
        except Exception:
            continue
    sorted_symbols = sorted(symbols)
    return sorted_symbols[:max_symbols] if max_symbols > 0 else sorted_symbols


def _scan_membership_by_symbol(
    module: Any,
    symbols: Sequence[str],
    index_id: str,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, set[pd.Timestamp]], int]:
    membership_by_symbol: dict[str, set[pd.Timestamp]] = {}
    failure_count = 0
    for symbol in symbols:
        try:
            frame = module.index_constituent_timeseries(
                symbol,
                index_id,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                timeseriesformat="pandas-dataframe",
            )
        except Exception:
            failure_count += 1
            continue
        dates = _true_membership_dates(frame)
        if dates:
            membership_by_symbol[symbol] = dates
    return membership_by_symbol, failure_count


def _true_membership_dates(frame: Any) -> set[pd.Timestamp]:
    if frame is None or bool(getattr(frame, "empty", False)):
        return set()
    series = frame.iloc[:, 0] if hasattr(frame, "iloc") else frame
    dates: set[pd.Timestamp] = set()
    for raw_date, value in series.items():
        if _truthy(value):
            dates.add(pd.Timestamp(raw_date).normalize())
    return dates


def _load_price_frame(
    module: Any,
    symbol: str,
    *,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    try:
        frame = module.price_timeseries(symbol, timeseriesformat="pandas-dataframe")
    except Exception:
        return pd.DataFrame()
    if frame is None or bool(getattr(frame, "empty", False)) or "Close" not in frame.columns:
        return pd.DataFrame()
    prices = frame.copy()
    prices.index = pd.to_datetime(prices.index).normalize()
    prices = prices.sort_index()
    if start_ts is not None:
        prices = prices[prices.index >= start_ts]
    if end_ts is not None:
        prices = prices[prices.index <= end_ts]
    if prices.empty:
        return pd.DataFrame()
    prices["return_1d"] = prices["Close"].pct_change()
    prices["ma20"] = prices["Close"].rolling(20).mean()
    prices["ma50"] = prices["Close"].rolling(50).mean()
    prices["ma200"] = prices["Close"].rolling(200).mean()
    if "Turnover" not in prices.columns:
        prices["Turnover"] = 0.0
    prices["turnover_lag1"] = prices["Turnover"].shift(1)
    return prices


def _members_on_day(
    membership_by_symbol: Mapping[str, set[pd.Timestamp]],
    day: pd.Timestamp,
) -> list[str]:
    normalized = day.normalize()
    return [symbol for symbol, dates in membership_by_symbol.items() if normalized in dates]


def _has_price_on_day(frame: pd.DataFrame, day: pd.Timestamp) -> bool:
    return day in frame.index and _finite(frame.at[day, "Close"])


def _pct_above_ma(
    frames: Sequence[tuple[str, pd.DataFrame]],
    day: pd.Timestamp,
    ma_column: str,
) -> float:
    values: list[bool] = []
    for _, frame in frames:
        if day not in frame.index:
            continue
        close = frame.at[day, "Close"]
        ma_value = frame.at[day, ma_column]
        if _finite(close) and _finite(ma_value):
            values.append(float(close) > float(ma_value))
    return _ratio(sum(values), len(values)) if values else math.nan


def _cap_weight_proxy_return(
    frames: Sequence[tuple[str, pd.DataFrame]],
    day: pd.Timestamp,
) -> float:
    weighted_sum = 0.0
    weight_sum = 0.0
    for _, frame in frames:
        if day not in frame.index:
            continue
        ret = frame.at[day, "return_1d"]
        weight = frame.at[day, "turnover_lag1"]
        if _finite(ret) and _finite(weight) and float(weight) > 0:
            weighted_sum += float(ret) * float(weight)
            weight_sum += float(weight)
    return weighted_sum / weight_sum if weight_sum else math.nan


def _qqq_forward_frame(qqq: pd.DataFrame) -> pd.DataFrame:
    close = qqq["Close"]
    frame = pd.DataFrame({"date": qqq.index})
    for horizon in (5, 10, 20):
        frame[f"next_{horizon}d_qqq_return"] = (
            close.shift(-horizon).to_numpy() / close.to_numpy() - 1
        )
    future_drawdowns: list[float] = []
    values = list(close)
    for index, value in enumerate(values):
        future = values[index + 1 : index + 21]
        if not future:
            future_drawdowns.append(math.nan)
        else:
            future_drawdowns.append(min(future) / value - 1)
    frame["future_20d_drawdown"] = future_drawdowns
    return frame


def _assign_breadth_buckets(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    valid = output["pct_above_ma50"].notna()
    output["breadth_bucket"] = None
    if int(valid.sum()) >= 3:
        ranks = output.loc[valid, "pct_above_ma50"].rank(method="first")
        output.loc[valid, "breadth_bucket"] = pd.qcut(
            ranks,
            3,
            labels=["low", "mid", "high"],
        ).astype(str)
    return output


def _assign_deterioration_flag(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    valid = output["breadth_momentum"].notna()
    output["breadth_deterioration"] = False
    if int(valid.sum()) >= 3:
        threshold = output.loc[valid, "breadth_momentum"].quantile(1 / 3)
        output.loc[valid, "breadth_deterioration"] = (
            output.loc[valid, "breadth_momentum"] <= threshold
        )
    return output


def _bucket_forward_return_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for bucket in ("low", "mid", "high"):
        subset = frame[frame["breadth_bucket"] == bucket]
        rows.append(_forward_summary_row(subset, {"breadth_bucket": bucket}))
    return rows


def _deterioration_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        _forward_summary_row(
            frame[frame["breadth_deterioration"] == flag],
            {"breadth_deterioration": bool(flag)},
        )
        for flag in (False, True)
    ]


def _forward_summary_row(frame: pd.DataFrame, labels: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **labels,
        "sample_count": int(len(frame)),
        "avg_next_5d_qqq_return": _round_or_none(_series_mean(frame, "next_5d_qqq_return")),
        "avg_next_10d_qqq_return": _round_or_none(_series_mean(frame, "next_10d_qqq_return")),
        "avg_next_20d_qqq_return": _round_or_none(_series_mean(frame, "next_20d_qqq_return")),
        "avg_future_20d_drawdown": _round_or_none(_series_mean(frame, "future_20d_drawdown")),
    }


def _baseline_comparison(
    frame: pd.DataFrame,
    qqq_prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    ma_window = _policy_int(policy, ("baseline_proxy_policy", "qqq_ma_window"), 50)
    qqq_proxy = qqq_prices[["Close"]].copy()
    qqq_proxy["qqq_ma"] = qqq_proxy["Close"].rolling(ma_window).mean()
    qqq_proxy["date"] = qqq_proxy.index
    merged = frame.merge(qqq_proxy[["date", "Close", "qqq_ma"]], on="date", how="left")
    merged["baseline_risk_off"] = merged["Close"] < merged["qqq_ma"]
    merged["baseline_plus_breadth_risk_off"] = (
        merged["baseline_risk_off"]
        | (merged["breadth_bucket"] == "low")
        | merged["breadth_deterioration"].astype(bool)
    )
    drawdown_threshold = merged["future_20d_drawdown"].quantile(0.25)
    merged["future_drawdown_event"] = merged["future_20d_drawdown"] <= drawdown_threshold
    baseline = _risk_signal_stats(merged, "baseline_risk_off")
    plus_breadth = _risk_signal_stats(merged, "baseline_plus_breadth_risk_off")
    return {
        "baseline_definition": "qqq_close_below_ma50_proxy",
        "baseline_plus_breadth_definition": (
            "baseline_risk_off_or_low_breadth_or_breadth_deterioration"
        ),
        "future_drawdown_event_threshold": _round_or_none(drawdown_threshold),
        "baseline_first_layer_proxy": baseline,
        "baseline_plus_breadth": plus_breadth,
        "false_risk_off_delta": _round_or_none(
            plus_breadth["false_risk_off_rate"] - baseline["false_risk_off_rate"]
        ),
        "false_risk_on_delta": _round_or_none(
            plus_breadth["false_risk_on_rate"] - baseline["false_risk_on_rate"]
        ),
    }


def _risk_signal_stats(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    valid = frame[frame["next_20d_qqq_return"].notna() & frame["future_drawdown_event"].notna()]
    risk_off = valid[valid[column]]
    risk_on = valid[~valid[column]]
    false_risk_off = risk_off[risk_off["next_20d_qqq_return"] > 0]
    false_risk_on = risk_on[risk_on["future_drawdown_event"]]
    return {
        "sample_count": int(len(valid)),
        "risk_off_day_count": int(len(risk_off)),
        "risk_on_day_count": int(len(risk_on)),
        "avg_next_20d_return_when_risk_off": _round_or_none(
            _series_mean(risk_off, "next_20d_qqq_return")
        ),
        "avg_next_20d_return_when_risk_on": _round_or_none(
            _series_mean(risk_on, "next_20d_qqq_return")
        ),
        "future_drawdown_event_capture_ratio": _round_or_none(
            _ratio(
                int(risk_off["future_drawdown_event"].sum()),
                int(valid["future_drawdown_event"].sum()),
            )
        ),
        "false_risk_off_rate": _round_or_none(_ratio(len(false_risk_off), len(risk_off))),
        "false_risk_on_rate": _round_or_none(_ratio(len(false_risk_on), len(risk_on))),
    }


def _classify_local_signal_evidence(
    *,
    bucket_rows: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
) -> str:
    rows_by_bucket = {str(row.get("breadth_bucket")): row for row in bucket_rows}
    low = rows_by_bucket.get("low", {})
    high = rows_by_bucket.get("high", {})
    supporting_horizons = 0
    for key in (
        "avg_next_5d_qqq_return",
        "avg_next_10d_qqq_return",
        "avg_next_20d_qqq_return",
    ):
        if _finite(low.get(key)) and _finite(high.get(key)) and high[key] > low[key]:
            supporting_horizons += 1
    false_risk_on_delta = baseline.get("false_risk_on_delta")
    if supporting_horizons == 3 and _finite(false_risk_on_delta) and false_risk_on_delta <= 0:
        return "strong"
    if supporting_horizons >= 2:
        return "moderate"
    if supporting_horizons >= 1:
        return "weak"
    return "none"


def _blocked_local_signal_report(reason: str) -> dict[str, Any]:
    return {
        "schema_version": "norgate_trial_local_signal_report_2y.v1",
        "report_type": "norgate_trial_local_signal_report_2y",
        "status": "NORGATE_2Y_LOCAL_SIGNAL_BLOCKED",
        "generated_at": _now(),
        "blocked_reason": reason,
        "breadth_bucket_vs_forward_return": [],
        "breadth_deterioration_vs_future_drawdown": [],
        "baseline_comparison": {},
        **PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY,
    }


def _feature_safety_boundary(feature_numeric_validated: bool) -> dict[str, Any]:
    payload = dict(PARTIAL_EFFECTIVENESS_SAFETY_BOUNDARY)
    payload["feature_numeric_validated"] = feature_numeric_validated
    payload["engineering_validated"] = True
    payload["local_signal_evidence"] = "not_evaluated"
    return payload


def _render_coverage_review(coverage: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Partial Effectiveness Coverage Report",
            "",
            f"- status: `{coverage.get('status')}`",
            f"- earliest_price_date: `{coverage.get('earliest_price_date')}`",
            f"- latest_price_date: `{coverage.get('latest_price_date')}`",
            f"- member_day_coverage_ratio: `{coverage.get('member_day_coverage_ratio')}`",
            f"- missing_price_ratio: `{coverage.get('missing_price_ratio')}`",
            f"- failed_join_count: `{coverage.get('failed_join_count')}`",
            "",
            "该报告只提交 coverage summary，不提交 member symbol list 或 vendor raw prices。",
        ]
    ) + "\n"


def _render_feature_review(features: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Breadth Feature Report 2Y",
            "",
            f"- status: `{features.get('status')}`",
            f"- feature_numeric_validated: `{features.get('feature_numeric_validated')}`",
            f"- numeric_feature_day_count: `{features.get('numeric_feature_day_count')}`",
            "",
            "Feature columns include pct_above_ma20/50/200, equal-weight return, "
            "cap-weight proxy return, advance/decline proxy and breadth momentum.",
        ]
    ) + "\n"


def _render_local_signal_review(local_signal: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Local Signal Report 2Y",
            "",
            f"- status: `{local_signal.get('status')}`",
            f"- local_signal_evidence: `{local_signal.get('local_signal_evidence')}`",
            f"- sample_day_count: `{local_signal.get('sample_day_count')}`",
            "",
            "该报告评估局部 2Y signal evidence，不允许作为 primary-window validation。",
        ]
    ) + "\n"


def _render_conclusion_review(conclusion: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Norgate Trial Partial Effectiveness Conclusion Matrix",
            "",
            f"- status: `{conclusion.get('status')}`",
            f"- source_engineering_useful: `{conclusion.get('source_engineering_useful')}`",
            f"- source_feature_useful_2y: `{conclusion.get('source_feature_useful_2y')}`",
            (
                f"- purchase_platinum_evidence_strength: "
                f"`{conclusion.get('purchase_platinum_evidence_strength')}`"
            ),
            (
                f"- model_ready_for_2021_primary_window: "
                f"`{conclusion.get('model_ready_for_2021_primary_window')}`"
            ),
            f"- reopen_gate_allowed: `{conclusion.get('reopen_gate_allowed')}`",
            f"- promotion_allowed: `{conclusion.get('promotion_allowed')}`",
            f"- paper_shadow_allowed: `{conclusion.get('paper_shadow_allowed')}`",
            f"- production_allowed: `{conclusion.get('production_allowed')}`",
            f"- broker_action: `{conclusion.get('broker_action')}`",
            "",
            (
                "2Y trial 不是无效证据；它可以支持购买决策，"
                "但不能替代 2021 primary-window validation。"
            ),
        ]
    ) + "\n"


def _load_policy(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _ensure_roots(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_scalar(payload), indent=2, sort_keys=True), encoding="utf-8")


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_scalar(row.get(key, "")) for key in fieldnames})


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return _date_text(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_scalar(value.item())
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _records(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _hash_symbols(symbols: Sequence[str]) -> str:
    import hashlib

    return hashlib.sha256("|".join(sorted(symbols)).encode("utf-8")).hexdigest()


def _truthy(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    return bool(value)


def _finite(value: Any) -> bool:
    try:
        return value is not None and not pd.isna(value) and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _mean(values: Sequence[float]) -> float:
    finite_values = [float(value) for value in values if _finite(value)]
    return sum(finite_values) / len(finite_values) if finite_values else math.nan


def _advance_decline_proxy(returns: Sequence[float]) -> float:
    finite_returns = [float(value) for value in returns if _finite(value)]
    if not finite_returns:
        return math.nan
    advances = len([value for value in finite_returns if value > 0])
    declines = len([value for value in finite_returns if value < 0])
    return (advances - declines) / len(finite_returns)


def _series_mean(frame: pd.DataFrame, column: str) -> float:
    if column not in frame:
        return math.nan
    return float(frame[column].dropna().mean()) if not frame[column].dropna().empty else math.nan


def _value_at(frame: pd.DataFrame, day: pd.Timestamp, column: str) -> Any:
    return frame.at[day, column] if day in frame.index and column in frame.columns else math.nan


def _round_or_none(value: Any, digits: int = 6) -> float | None:
    return round(float(value), digits) if _finite(value) else None


def _date_text(value: Any) -> str:
    return pd.Timestamp(value).date().isoformat()


def _policy_float(policy: Mapping[str, Any], path: Sequence[str], default: float) -> float:
    value: Any = policy
    for key in path:
        if not isinstance(value, Mapping):
            return default
        value = value.get(key)
    return float(value) if _finite(value) else default


def _policy_int(policy: Mapping[str, Any], path: Sequence[str], default: int) -> int:
    return int(_policy_float(policy, path, float(default)))
