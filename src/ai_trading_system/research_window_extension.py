from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _constant_target_frame,
    _data_quality_gate,
    _load_price_matrix,
    _metrics_row,
    _simulate_rebalanced_portfolio,
    _slice_prices,
)
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_SCORE_POLICY_PATH,
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    STATE_ORDER,
    _confidence_from_margin,
)
from ai_trading_system.first_layer_up_state_learning import build_hierarchical_trend_labels
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "research_window_registry.yaml"
)
DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "window_aware_walk_forward_policy.yaml"
)

DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "windowed_static_frontier"
)
DEFAULT_WINDOWED_ACTUAL_PATH_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "windowed_actual_path"
)
DEFAULT_REPORTS_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "reports"

DEFAULT_DATA_AVAILABILITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "data_availability_matrix_for_window_extension.md"
)
DEFAULT_DATA_AVAILABILITY_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "data_availability_matrix_for_window_extension.yaml"
)
DEFAULT_DATA_QUALITY_WINDOW_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "data_quality_window_summary.yaml"
)
DEFAULT_STATIC_FRONTIER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "static_frontier_multi_window_review.md"
)
DEFAULT_STATIC_FRONTIER_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "static_frontier_multi_window_matrix.yaml"
)
DEFAULT_SECOND_LAYER_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "second_layer_probe_multi_window_review.md"
)
DEFAULT_SECOND_LAYER_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "second_layer_probe_window_stability_matrix.yaml"
)
DEFAULT_FIRST_LAYER_LABEL_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_label_multi_window_review.md"
)
DEFAULT_FIRST_LAYER_LABEL_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_label_window_matrix.yaml"
)
DEFAULT_ACTUAL_PATH_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "actual_path_multi_window_rebacktest_review.md"
)
DEFAULT_ACTUAL_PATH_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "actual_path_multi_window_matrix.yaml"
)
DEFAULT_STABILITY_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "window_stability_classification.yaml"
)
DEFAULT_STABILITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "window_stability_classification_review.md"
)
DEFAULT_UP_STATE_DEPENDENCY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_up_state_window_dependency_update.md"
)
DEFAULT_OWNER_PACK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "research_window_extension_owner_review_pack.md"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "research_window_extension_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "research_window_extension_final_matrix.yaml"
)

WINDOW_IDS_FOR_RERUN = [
    "exact_three_asset_validated",
    "exact_three_asset_primary_only_extension",
    "legacy_research_window_2022_12",
]
ASSETS = ["QQQ", "SGOV", "TQQQ"]


def run_research_window_extension_validation_pack(
    *,
    registry_path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    walk_forward_policy_path: Path = DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH,
    probe_registry_path: Path = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Path = DEFAULT_SCORE_POLICY_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    static_output_root: Path = DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT,
    actual_path_output_root: Path = DEFAULT_WINDOWED_ACTUAL_PATH_ROOT,
    reports_output_root: Path = DEFAULT_REPORTS_OUTPUT_ROOT,
) -> dict[str, Any]:
    registry = load_research_window_registry(registry_path)
    windows = [registry["windows"][window_id] for window_id in WINDOW_IDS_FOR_RERUN]
    probe_registry = _load_yaml_mapping(probe_registry_path)
    score_policy = _load_yaml_mapping(score_policy_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)
    prices = _load_price_matrix(prices_path, ASSETS)
    marketstack = _load_price_matrix(marketstack_prices_path, ASSETS)
    rates = _load_rates_long(rates_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=None,
        expected_tickers=ASSETS,
    )
    if not data_gate["passed"]:
        raise RuntimeError(
            f"Cached data quality gate failed for research window extension: {data_gate['status']}"
        )

    data_availability = build_data_availability_matrix(
        registry=registry,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    data_quality_summary = build_window_data_quality_summary(
        registry=registry,
        prices=prices,
        marketstack=marketstack,
        rates=rates,
        data_gate=data_gate,
        reports_output_root=reports_output_root,
    )
    static_metrics = build_static_frontier_multi_window(
        windows=windows,
        prices=prices,
        output_root=static_output_root,
    )
    label_frames = {
        str(window["research_window_id"]): _window_consensus_labels(
            prices=slice_window_prices(prices, window),
            probe_registry=probe_registry,
            score_policy=score_policy,
        )
        for window in windows
    }
    probe_metrics = build_second_layer_probe_multi_window(
        windows=windows,
        prices=prices,
        probe_registry=probe_registry,
        score_policy=score_policy,
        static_metrics=static_metrics,
        label_frames=label_frames,
        output_root=actual_path_output_root,
    )
    label_matrix = build_first_layer_label_multi_window(
        windows=windows,
        prices=prices,
        probe_registry=probe_registry,
        score_policy=score_policy,
        label_frames=label_frames,
    )
    actual_path_matrix = build_actual_path_multi_window(
        static_metrics=static_metrics,
        probe_metrics=probe_metrics,
        output_root=actual_path_output_root,
    )
    stability = build_window_stability_classification(actual_path_matrix)
    owner_pack = build_owner_pack(
        registry=registry,
        data_availability=data_availability,
        data_quality_summary=data_quality_summary,
        static_metrics=static_metrics,
        probe_metrics=probe_metrics,
        label_matrix=label_matrix,
        actual_path_matrix=actual_path_matrix,
        stability=stability,
    )
    final_matrix = build_final_matrix(
        data_quality_summary=data_quality_summary,
        stability=stability,
        label_matrix=label_matrix,
    )
    write_window_extension_outputs(
        data_availability=data_availability,
        data_quality_summary=data_quality_summary,
        static_metrics=static_metrics,
        probe_metrics=probe_metrics,
        label_matrix=label_matrix,
        actual_path_matrix=actual_path_matrix,
        stability=stability,
        owner_pack=owner_pack,
        final_matrix=final_matrix,
        walk_forward_policy_path=walk_forward_policy_path,
    )
    return owner_pack


def load_research_window_registry(
    path: Path = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = _load_yaml_mapping(path)
    windows = {
        str(window_id): {**dict(window), "research_window_id": str(window_id)}
        for window_id, window in _mapping(registry.get("windows")).items()
        if isinstance(window, Mapping)
    }
    registry["windows"] = windows
    return registry


def validate_research_window_contracts(
    registry: Mapping[str, Any],
    artifact: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    windows = _mapping(registry.get("windows"))
    issues: list[dict[str, str]] = []
    inception = _mapping(windows.get("requested_sgov_inception_range"))
    if inception.get("requested_start") == inception.get("actual_portfolio_start"):
        issues.append(_issue("requested_start_before_common_date_not_adjusted"))
    extension = _mapping(windows.get("exact_three_asset_primary_only_extension"))
    if "sgov_secondary_gap_2020_05_28_to_2021_02_19" not in _string_list(extension.get("caveats")):
        issues.append(_issue("primary_only_extension_missing_sgov_secondary_gap_caveat"))
    proxy = _mapping(windows.get("qqq_tqqq_sgov_proxy_robustness"))
    if proxy.get("exact_or_proxy") == "exact":
        issues.append(_issue("proxy_window_marked_exact"))
    for window_id, window in windows.items():
        if window.get(
            "exact_or_proxy"
        ) == "proxy" and "exact_three_asset_leaderboard" not in _string_list(
            window.get("blocked_usage")
        ):
            issues.append(_issue(f"{window_id}_proxy_can_mix_with_exact_leaderboard"))
    if artifact is not None:
        required = {
            "research_window_id",
            "requested_start",
            "actual_start",
            "actual_portfolio_start",
            "window_role",
            "data_quality_contract",
        }
        missing = sorted(required - set(artifact))
        if missing:
            issues.append(_issue(f"artifact_missing_window_fields:{','.join(missing)}"))
    return {
        "status": "PASS" if not issues else "FAIL",
        "issues": issues,
        **SAFETY_BOUNDARY,
    }


def build_data_availability_matrix(
    *,
    registry: Mapping[str, Any],
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
    primary = pd.read_csv(prices_path, parse_dates=["date"])
    secondary = pd.read_csv(marketstack_prices_path, parse_dates=["date"])
    rates = pd.read_csv(rates_path, parse_dates=["date"])
    rows: list[dict[str, Any]] = []
    for ticker in ASSETS:
        p = _ticker_rows(primary, ticker)
        s = _ticker_rows(secondary, ticker)
        rows.append(
            {
                "asset_or_feature": ticker,
                "earliest_available_date": _min_date(p),
                "first_common_tradable_date": _first_common_tradable_date(primary, ASSETS),
                "primary_source_status": "available" if not p.empty else "missing",
                "secondary_source_status": _secondary_status(ticker, s),
                "exact_or_proxy": "exact",
                "PIT_status": "PIT_APPROVED" if not p.empty else "PIT_BLOCKED",
                "revision_risk": "low_for_adjusted_close_cache",
                "survivorship_risk": "low_for_etf_ticker_history",
                "allowed_usage": ["exact_three_asset_research"],
                "blocked_usage": ["production", "paper_shadow", "broker_order"],
            }
        )
    for series in ("DGS2", "DGS10", "DTWEXBGS"):
        frame = rates.loc[rates["series"].astype(str) == series]
        rows.append(
            {
                "asset_or_feature": f"FRED_{series}",
                "earliest_available_date": _min_date(frame),
                "first_common_tradable_date": _first_common_tradable_date(primary, ASSETS),
                "primary_source_status": "available" if not frame.empty else "missing",
                "secondary_source_status": "not_required_for_fred_rate",
                "exact_or_proxy": "exact",
                "PIT_status": "PIT_APPROVED" if not frame.empty else "PIT_BLOCKED",
                "revision_risk": "public_rate_series_revision_possible",
                "survivorship_risk": "not_applicable",
                "allowed_usage": ["feature_context", "cash_yield_context"],
                "blocked_usage": ["sgov_exact_return_proxy"],
            }
        )
    rows.extend(
        [
            _metadata_feature_row("BIL_proxy", "proxy", ["proxy_robustness_only"]),
            _metadata_feature_row("VIX", "proxy", ["future_optional_feature_if_cache_validated"]),
            _metadata_feature_row("major_price_trend_features", "exact", ["PIT_price_features"]),
            _metadata_feature_row(
                "AI_or_semiconductor_features",
                "metadata_only",
                ["blocked_until_validated_cache_available"],
            ),
        ]
    )
    payload = _payload(
        report_type="data_availability_matrix_for_window_extension",
        title="Data Availability Matrix for Window Extension",
        status="DATA_AVAILABILITY_WINDOW_EXTENSION_READY_PROMOTION_BLOCKED",
        summary={
            "row_count": len(rows),
            "first_common_tradable_date": _first_common_tradable_date(primary, ASSETS),
            "primary_window_start": _mapping(
                _mapping(registry.get("windows")).get("exact_three_asset_validated")
            ).get("start"),
            "extension_window_start": _mapping(
                _mapping(registry.get("windows")).get("exact_three_asset_primary_only_extension")
            ).get("start"),
        },
        rows=rows,
    )
    return payload


def build_window_data_quality_summary(
    *,
    registry: Mapping[str, Any],
    prices: pd.DataFrame,
    marketstack: pd.DataFrame,
    rates: pd.DataFrame,
    data_gate: Mapping[str, Any],
    reports_output_root: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for window_id, window in _mapping(registry.get("windows")).items():
        if window_id == "qqq_tqqq_sgov_proxy_robustness":
            continue
        metadata = window_metadata(window)
        if str(window.get("role")) == "metadata_only":
            status = "METADATA_ONLY"
        else:
            primary = slice_window_prices(prices, window)
            secondary = slice_window_prices(marketstack, window, allow_partial=True)
            missing_secondary_assets = [
                asset
                for asset in ASSETS
                if secondary.get(asset, pd.Series(dtype=float)).dropna().empty
            ]
            status = "PASS"
            if missing_secondary_assets and window_id == "exact_three_asset_validated":
                status = "WARNING_SECONDARY_SOURCE_GAPS"
            if (
                window_id == "exact_three_asset_primary_only_extension"
                and "sgov_secondary_gap_2020_05_28_to_2021_02_19"
                in _string_list(window.get("caveats"))
            ):
                status = "PASS_WITH_EXPECTED_SGOV_SECONDARY_GAP_CAVEAT"
            if primary.empty:
                status = "BLOCKED_NO_PRIMARY_PRICE_ROWS"
            _write_markdown(
                reports_output_root / f"data_quality_{window_id}.md",
                _render_window_quality_doc(window_id, status, metadata),
            )
        rows.append(
            {
                **metadata,
                "window_data_quality_status": status,
                "global_cache_quality_status": data_gate.get("status"),
                "global_cache_quality_passed": bool(data_gate.get("passed")),
            }
        )
    return _payload(
        report_type="data_quality_window_summary",
        title="Data Quality Window Summary",
        status="DATA_QUALITY_WINDOW_SUMMARY_READY_PROMOTION_BLOCKED",
        summary={
            "window_count": len(rows),
            "primary_window_status": _row_status(rows, "exact_three_asset_validated"),
            "extension_window_status": _row_status(
                rows, "exact_three_asset_primary_only_extension"
            ),
        },
        window_rows=rows,
    )


def build_static_frontier_multi_window(
    *,
    windows: Sequence[Mapping[str, Any]],
    prices: pd.DataFrame,
    output_root: Path,
    step: float = 0.10,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for window in windows:
        window_prices = slice_window_prices(prices, window)
        for rank, weights in enumerate(_weight_grid(step), start=1):
            sim = _simulate_rebalanced_portfolio(
                window_prices,
                _constant_target_frame(window_prices.index, window_prices.columns, weights),
                rebalance="monthly",
                transaction_cost_bps=0.0,
            )
            row = _metrics_row(
                strategy_id="static_"
                + "_".join(f"{asset}{weight:.1f}" for asset, weight in weights.items()),
                candidate_family="windowed_static_simplex_grid",
                weights=weights,
                sim=sim,
                annualization=252,
                selection_rank=rank,
            )
            rows.append({**window_metadata(window), **row})
    frame = pd.DataFrame(rows)
    output_root.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_root / "static_simplex_grid_metrics_by_window.csv", index=False)
    return frame


def build_second_layer_probe_multi_window(
    *,
    windows: Sequence[Mapping[str, Any]],
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
    static_metrics: pd.DataFrame,
    output_root: Path,
    label_frames: Mapping[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    probes = _records(probe_registry.get("probes"))
    for window in windows:
        window_prices = slice_window_prices(prices, window)
        labels = (
            label_frames.get(str(window["research_window_id"]))
            if label_frames is not None
            else None
        )
        if labels is None:
            labels = _window_consensus_labels(
                prices=window_prices,
                probe_registry=probe_registry,
                score_policy=score_policy,
            )
        predictions = _label_predictions(labels)
        for probe in [*_limited_adjustment_probe(), *probes]:
            metrics = _probe_metrics(
                prices=window_prices,
                predictions=predictions,
                probe=probe,
                model_id="window_consensus_label_path_diagnostic",
            )
            same_risk = _same_risk_static_baseline(
                static_metrics=static_metrics,
                window_id=str(window["research_window_id"]),
                qqq_equiv=float(metrics["qqq_equivalent_exposure"]),
            )
            rows.append(
                {
                    **window_metadata(window),
                    **metrics,
                    "same_risk_static_strategy_id": same_risk.get("strategy_id"),
                    "same_risk_static_delta": round(
                        _float(metrics.get("annual_return"))
                        - _float(same_risk.get("annual_return")),
                        GRID_ROUND_DIGITS,
                    ),
                    "net_of_cost_delta": round(
                        _float(metrics.get("net_annual_return"))
                        - _float(same_risk.get("net_annual_return")),
                        GRID_ROUND_DIGITS,
                    ),
                }
            )
    frame = pd.DataFrame(rows)
    output_root.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_root / "second_layer_probe_metrics_by_window.csv", index=False)
    return frame


def build_first_layer_label_multi_window(
    *,
    windows: Sequence[Mapping[str, Any]],
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
    label_frames: Mapping[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for window in windows:
        window_prices = slice_window_prices(prices, window)
        labels = (
            label_frames.get(str(window["research_window_id"]))
            if label_frames is not None
            else None
        )
        if labels is None:
            labels = _window_consensus_labels(
                prices=window_prices,
                probe_registry=probe_registry,
                score_policy=score_policy,
            )
        hierarchical = build_hierarchical_trend_labels(labels)
        by_state = _state_counts(labels["consensus_state"])
        high_confidence = int(labels["train_usable"].astype(bool).sum())
        rows.append(
            {
                **window_metadata(window),
                "label_count": len(labels),
                "risk_off_count": by_state.get("risk_off", 0),
                "defensive_count": by_state.get("defensive", 0),
                "neutral_count": by_state.get("neutral", 0),
                "constructive_count": by_state.get("constructive", 0),
                "risk_on_count": by_state.get("risk_on", 0),
                "upper_state_count": int(hierarchical["upper_state_binary_label"].sum()),
                "high_confidence_count": high_confidence,
                "high_confidence_share": round(_ratio(high_confidence, len(labels)), 6),
                "avg_disagreement_score": round(
                    float(labels["disagreement_score"].astype(float).mean()), 6
                ),
                "window_aware_split_policy": "per_window_required",
            }
        )
    return _payload(
        report_type="first_layer_label_window_matrix",
        title="First-Layer Label Multi-Window Matrix",
        status="FIRST_LAYER_LABEL_MULTI_WINDOW_READY_PROMOTION_BLOCKED",
        summary={
            "window_count": len(rows),
            "primary_upper_state_count": _row_value(
                rows, "exact_three_asset_validated", "upper_state_count"
            ),
            "legacy_upper_state_count": _row_value(
                rows, "legacy_research_window_2022_12", "upper_state_count"
            ),
        },
        window_rows=rows,
    )


def build_actual_path_multi_window(
    *,
    static_metrics: pd.DataFrame,
    probe_metrics: pd.DataFrame,
    output_root: Path,
) -> pd.DataFrame:
    static_subset = (
        static_metrics.sort_values(["research_window_id", "annual_return"], ascending=[True, False])
        .groupby("research_window_id")
        .head(5)
    )
    static_rows = []
    for _, row in static_subset.iterrows():
        static_rows.append(
            {
                **_window_fields_from_row(row),
                "strategy_id": row["strategy_id"],
                "strategy_family": "static_frontier_compact",
                "annual_return": row["annual_return"],
                "max_drawdown": row["max_drawdown_daily_equity"],
                "sharpe": row["sharpe_daily_zero_rf"],
                "calmar": row["calmar_daily_equity_dd"],
                "turnover": row["turnover"],
                "tqqq_exposure": row["tqqq_weight"],
                "comparison_group": "window_local_static",
            }
        )
    probe_rows = []
    for _, row in probe_metrics.iterrows():
        probe_rows.append(
            {
                **_window_fields_from_row(row),
                "strategy_id": row["strategy_id"],
                "strategy_family": row["strategy_family"],
                "annual_return": row["annual_return"],
                "max_drawdown": row["max_drawdown"],
                "sharpe": row["sharpe"],
                "calmar": row["calmar"],
                "turnover": row["turnover"],
                "tqqq_exposure": row["tqqq_exposure"],
                "comparison_group": "second_layer_probe_diagnostic",
            }
        )
    frame = pd.DataFrame([*static_rows, *probe_rows])
    baseline = frame.loc[
        frame["research_window_id"] == "legacy_research_window_2022_12",
        ["strategy_id", "annual_return", "calmar"],
    ].rename(columns={"annual_return": "legacy_annual_return", "calmar": "legacy_calmar"})
    primary = frame.loc[
        frame["research_window_id"] == "exact_three_asset_validated",
        ["strategy_id", "annual_return", "calmar"],
    ].rename(columns={"annual_return": "primary_annual_return", "calmar": "primary_calmar"})
    extension = frame.loc[
        frame["research_window_id"] == "exact_three_asset_primary_only_extension",
        ["strategy_id", "annual_return", "calmar"],
    ].rename(columns={"annual_return": "extension_annual_return", "calmar": "extension_calmar"})
    compare = (
        frame.merge(baseline, on="strategy_id", how="left")
        .merge(primary, on="strategy_id", how="left")
        .merge(extension, on="strategy_id", how="left")
    )
    compare["delta_2021_vs_2022"] = (
        compare["primary_annual_return"] - compare["legacy_annual_return"]
    ).round(GRID_ROUND_DIGITS)
    compare["delta_2020_vs_2021"] = (
        compare["extension_annual_return"] - compare["primary_annual_return"]
    ).round(GRID_ROUND_DIGITS)
    compare["stability_status"] = compare.apply(_stability_from_row, axis=1)
    output_root.mkdir(parents=True, exist_ok=True)
    compare.to_csv(output_root / "actual_path_metrics_by_window.csv", index=False)
    return compare


def build_window_stability_classification(actual_path_matrix: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for strategy_id, frame in actual_path_matrix.groupby("strategy_id"):
        primary = frame.loc[frame["research_window_id"] == "exact_three_asset_validated"]
        legacy = frame.loc[frame["research_window_id"] == "legacy_research_window_2022_12"]
        extension = frame.loc[
            frame["research_window_id"] == "exact_three_asset_primary_only_extension"
        ]
        classification = "WINDOW_SENSITIVE_UNEXPLAINED"
        if not primary.empty and not legacy.empty:
            delta = abs(
                _float(primary.iloc[0].get("annual_return"))
                - _float(legacy.iloc[0].get("annual_return"))
            )
            if delta < 0.03:
                classification = "WINDOW_STABLE"
            elif _float(primary.iloc[0].get("annual_return")) > 0:
                classification = "WINDOW_SENSITIVE_BUT_EXPLAINED"
            elif _float(legacy.iloc[0].get("annual_return")) > 0:
                classification = "LEGACY_WINDOW_ONLY_EDGE"
        if not extension.empty and str(extension.iloc[0].get("window_role")) == "sensitivity":
            classification = (
                "EXTENSION_WINDOW_CAVEATED" if classification == "WINDOW_STABLE" else classification
            )
        rows.append({"strategy_id": strategy_id, "classification": classification})
    return _payload(
        report_type="window_stability_classification",
        title="Window Stability Classification",
        status="WINDOW_STABILITY_CLASSIFICATION_READY_PROMOTION_BLOCKED",
        summary={
            "strategy_count": len(rows),
            "window_stable_count": sum(row["classification"] == "WINDOW_STABLE" for row in rows),
            "legacy_window_only_edge_count": sum(
                row["classification"] == "LEGACY_WINDOW_ONLY_EDGE" for row in rows
            ),
        },
        strategy_rows=rows,
    )


def build_owner_pack(
    *,
    registry: Mapping[str, Any],
    data_availability: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    static_metrics: pd.DataFrame,
    probe_metrics: pd.DataFrame,
    label_matrix: Mapping[str, Any],
    actual_path_matrix: pd.DataFrame,
    stability: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type="research_window_extension_owner_review_pack",
        title="Research Window Extension Owner Review Pack",
        status="RESEARCH_WINDOW_EXTENSION_OWNER_REVIEW_READY_PROMOTION_BLOCKED",
        summary={
            "primary_window": "exact_three_asset_validated",
            "primary_start": "2021-02-22",
            "extension_window": "exact_three_asset_primary_only_extension",
            "extension_start": "2020-05-28",
            "legacy_window": "legacy_research_window_2022_12",
            "static_metric_rows": len(static_metrics),
            "probe_metric_rows": len(probe_metrics),
            "actual_path_metric_rows": len(actual_path_matrix),
            "promotion_status": "BLOCKED",
        },
        owner_answers={
            "why_extend_from_2022_12": "2022-12 窗口过短，且高度集中在 AI/tech 强趋势阶段。",
            "why_not_2020_05_26_portfolio_start": (
                "SGOV primary cache 缺少 2020-05-26 和 2020-05-27 可交易行。"
            ),
            "extension_caveat": (
                "2020-05-28 extension 在 2021-02-22 前存在 SGOV secondary-source gap。"
            ),
            "effect_on_conclusions": "所有结果继续是 research-only，并且必须按 window tag 解读。",
            "why_promotion_blocked": (
                "Window extension 只是验证证据，不是 owner approval 或 production readiness。"
            ),
        },
        data_availability_summary=_mapping(data_availability.get("summary")),
        data_quality_summary=_mapping(data_quality_summary.get("summary")),
        label_summary=_mapping(label_matrix.get("summary")),
        stability_summary=_mapping(stability.get("summary")),
    )


def build_final_matrix(
    *,
    data_quality_summary: Mapping[str, Any],
    stability: Mapping[str, Any],
    label_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    primary_status = _mapping(data_quality_summary.get("summary")).get("primary_window_status")
    extension_status = _mapping(data_quality_summary.get("summary")).get("extension_window_status")
    stable_count = _int(_mapping(stability.get("summary")).get("window_stable_count"))
    if str(primary_status).startswith("BLOCKED"):
        base_status = "DATA_GAP_BLOCKS_EXTENSION"
    elif stable_count > 0:
        base_status = "PRIMARY_WINDOW_ADOPTED_WITH_CAVEATS"
    else:
        base_status = "WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT"
    return _payload(
        report_type="research_window_extension_final_matrix",
        title="Research Window Extension Final Matrix",
        status=f"{base_status}_PROMOTION_BLOCKED",
        summary={
            "base_status": base_status,
            "primary_window_status": primary_status,
            "extension_window_status": extension_status,
            "primary_upper_state_count": _mapping(label_matrix.get("summary")).get(
                "primary_upper_state_count"
            ),
            "legacy_upper_state_count": _mapping(label_matrix.get("summary")).get(
                "legacy_upper_state_count"
            ),
            "promotion_status": "BLOCKED",
        },
        stability_summary=_mapping(stability.get("summary")),
    )


def write_window_extension_outputs(
    *,
    data_availability: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    static_metrics: pd.DataFrame,
    probe_metrics: pd.DataFrame,
    label_matrix: Mapping[str, Any],
    actual_path_matrix: pd.DataFrame,
    stability: Mapping[str, Any],
    owner_pack: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
    walk_forward_policy_path: Path,
) -> None:
    _write_yaml(DEFAULT_DATA_AVAILABILITY_YAML_PATH, data_availability)
    _write_yaml(DEFAULT_DATA_QUALITY_WINDOW_SUMMARY_PATH, data_quality_summary)
    _write_yaml(DEFAULT_STATIC_FRONTIER_YAML_PATH, _static_frontier_payload(static_metrics))
    _write_yaml(DEFAULT_SECOND_LAYER_YAML_PATH, _second_layer_payload(probe_metrics))
    _write_yaml(DEFAULT_FIRST_LAYER_LABEL_YAML_PATH, label_matrix)
    _write_yaml(DEFAULT_ACTUAL_PATH_YAML_PATH, _actual_path_payload(actual_path_matrix))
    _write_yaml(DEFAULT_STABILITY_YAML_PATH, stability)
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)

    _write_markdown(DEFAULT_DATA_AVAILABILITY_DOC_PATH, _render_payload_doc(data_availability))
    _write_markdown(
        DEFAULT_STATIC_FRONTIER_DOC_PATH,
        _render_payload_doc(_static_frontier_payload(static_metrics)),
    )
    _write_markdown(
        DEFAULT_SECOND_LAYER_DOC_PATH, _render_payload_doc(_second_layer_payload(probe_metrics))
    )
    _write_markdown(DEFAULT_FIRST_LAYER_LABEL_DOC_PATH, _render_payload_doc(label_matrix))
    _write_markdown(
        DEFAULT_ACTUAL_PATH_DOC_PATH, _render_payload_doc(_actual_path_payload(actual_path_matrix))
    )
    _write_markdown(DEFAULT_STABILITY_DOC_PATH, _render_payload_doc(stability))
    _write_markdown(
        DEFAULT_UP_STATE_DEPENDENCY_DOC_PATH,
        _render_dependency_doc(walk_forward_policy_path),
    )
    _write_markdown(DEFAULT_OWNER_PACK_DOC_PATH, _render_owner_doc(owner_pack))
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_payload_doc(final_matrix))


def slice_window_prices(
    prices: pd.DataFrame,
    window: Mapping[str, Any],
    *,
    allow_partial: bool = False,
) -> pd.DataFrame:
    start = date.fromisoformat(str(window.get("actual_portfolio_start", window.get("start"))))
    frame = _slice_prices(prices, start_date=start, end_date=None)
    if allow_partial:
        return frame
    return frame.dropna(subset=ASSETS)


def window_metadata(window: Mapping[str, Any]) -> dict[str, Any]:
    requested_start = str(window.get("requested_start", window.get("start")))
    actual_start = str(window.get("actual_start", window.get("start")))
    return {
        "research_window_id": str(window.get("research_window_id")),
        "requested_start": requested_start,
        "actual_start": actual_start,
        "actual_portfolio_start": str(window.get("actual_portfolio_start", actual_start)),
        "end": str(window.get("end", "latest")),
        "window_role": str(window.get("role")),
        "data_quality_contract": str(window.get("data_quality_contract")),
        "exact_or_proxy": str(window.get("exact_or_proxy", "exact")),
    }


def artifact_has_required_window_fields(artifact: Mapping[str, Any]) -> bool:
    required = {
        "research_window_id",
        "requested_start",
        "actual_start",
        "actual_portfolio_start",
        "window_role",
        "data_quality_contract",
    }
    return required <= set(artifact)


def _window_consensus_labels(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
) -> pd.DataFrame:
    action_value = _build_fast_action_value_matrix(
        prices=prices,
        probe_registry=probe_registry,
        score_policy=score_policy,
    )
    return _build_fast_consensus_trend_labels(
        action_value=action_value, scope_config=_label_scope()
    )


def _build_fast_action_value_matrix(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    score_policy: Mapping[str, Any],
) -> pd.DataFrame:
    horizons = [_int(value) for value in _list_values(score_policy.get("horizons"))] or [
        5,
        10,
        20,
        60,
    ]
    probes = _records(probe_registry.get("probes"))
    max_horizon = max(horizons)
    row_count = max(0, len(prices.index) - max_horizon - 1)
    if row_count <= 0:
        return pd.DataFrame()
    returns = prices.pct_change().fillna(0.0)
    metric_cache: dict[tuple[tuple[float, float, float], int], list[dict[str, float]]] = {}
    rows: list[dict[str, Any]] = []
    full_score_policy = _mapping(score_policy.get("full_allocation_score"))
    tqqq_policy = _mapping(score_policy.get("tqqq_risk_penalty"))
    for probe in probes:
        probe_id = str(probe.get("probe_id"))
        weights_by_state = _mapping(probe.get("weights_by_trend_state"))
        normalized_by_state = {
            state: _normalize_action_weights(_mapping(weights_by_state.get(state)))
            for state in STATE_ORDER
        }
        neutral_weights = normalized_by_state.get("neutral", {})
        neutral_key = _weight_key(neutral_weights)
        for horizon in horizons:
            neutral_metrics = _cached_future_metrics(
                returns=returns,
                weights=neutral_weights,
                horizon=horizon,
                row_count=row_count,
                cache=metric_cache,
                weight_key=neutral_key,
            )
            for idx in range(row_count):
                timestamp = prices.index[idx]
                neutral = neutral_metrics[idx]
                for state in STATE_ORDER:
                    weights = normalized_by_state[state]
                    metrics = _cached_future_metrics(
                        returns=returns,
                        weights=weights,
                        horizon=horizon,
                        row_count=row_count,
                        cache=metric_cache,
                        weight_key=_weight_key(weights),
                    )[idx]
                    estimated_cost = 0.0
                    tqqq_penalty = _float(weights.get("TQQQ")) * _float(
                        tqqq_policy.get("penalty_per_weight")
                    )
                    full_score = (
                        metrics["future_return"]
                        - _float(full_score_policy.get("lambda_dd"))
                        * abs(metrics["future_max_drawdown"])
                        - _float(full_score_policy.get("lambda_worst5"))
                        * abs(metrics["worst_5d_return"])
                        - tqqq_penalty
                    )
                    avoided_drawdown = abs(neutral["future_max_drawdown"]) - abs(
                        metrics["future_max_drawdown"]
                    )
                    missed_upside = max(
                        0.0,
                        neutral["future_return"] - metrics["future_return"],
                    )
                    overlay_score = avoided_drawdown - missed_upside - estimated_cost
                    rows.append(
                        {
                            "date": timestamp.date().isoformat(),
                            "probe_id": probe_id,
                            "assumed_trend_state": state,
                            "horizon_days": horizon,
                            "portfolio_weights": weights,
                            "future_return": round(metrics["future_return"], GRID_ROUND_DIGITS),
                            "future_max_drawdown": round(
                                metrics["future_max_drawdown"], GRID_ROUND_DIGITS
                            ),
                            "worst_1d_return": round(metrics["worst_1d_return"], GRID_ROUND_DIGITS),
                            "worst_5d_return": round(metrics["worst_5d_return"], GRID_ROUND_DIGITS),
                            "worst_20d_return": round(
                                metrics["worst_20d_return"], GRID_ROUND_DIGITS
                            ),
                            "avoided_drawdown_vs_neutral": round(
                                avoided_drawdown, GRID_ROUND_DIGITS
                            ),
                            "missed_upside_vs_neutral": round(missed_upside, GRID_ROUND_DIGITS),
                            "same_risk_static_delta": round(
                                metrics["future_return"] - neutral["future_return"],
                                GRID_ROUND_DIGITS,
                            ),
                            "net_of_cost_score": round(metrics["future_return"], GRID_ROUND_DIGITS),
                            "stress_penalty": round(abs(min(0.0, metrics["worst_5d_return"])), 6),
                            "tqqq_risk_penalty": round(tqqq_penalty, GRID_ROUND_DIGITS),
                            "estimated_cost": estimated_cost,
                            "action_value_score": round(full_score, GRID_ROUND_DIGITS),
                            "overlay_action_value_score": round(overlay_score, GRID_ROUND_DIGITS),
                            "label_uses_future_outcome": True,
                            "feature_cutoff_used": False,
                            **SAFETY_BOUNDARY,
                        }
                    )
    return pd.DataFrame(rows)


def _build_fast_consensus_trend_labels(
    *,
    action_value: pd.DataFrame,
    scope_config: Mapping[str, Any],
) -> pd.DataFrame:
    if action_value.empty:
        return pd.DataFrame()
    group_cols = ["date", "probe_id", "horizon_days"]
    single_rows: list[dict[str, Any]] = []
    for keys, frame in action_value.groupby(group_cols, sort=True):
        ordered = frame.sort_values("action_value_score", ascending=False)
        best = ordered.iloc[0]
        single_rows.append(
            {
                "date": keys[0],
                "probe_id": keys[1],
                "horizon_days": int(keys[2]),
                "best_trend_state": str(best["assumed_trend_state"]),
                "best_action_value_score": _float(best["action_value_score"]),
            }
        )
    single = pd.DataFrame(single_rows)
    score_means = (
        action_value.groupby(["date", "horizon_days", "assumed_trend_state"])["action_value_score"]
        .mean()
        .unstack("assumed_trend_state")
        .reindex(columns=STATE_ORDER)
    )
    score_means = score_means.fillna(float("-inf"))
    vote_counts = (
        single.groupby(["date", "horizon_days", "best_trend_state"])
        .size()
        .unstack("best_trend_state")
        .reindex(columns=STATE_ORDER, fill_value=0)
    )
    probe_counts = single.groupby(["date", "horizon_days"])["probe_id"].nunique()
    usage = _mapping(scope_config.get("label_usage"))
    confidence_floor = _float(usage.get("high_confidence_floor"), default=0.55)
    max_disagreement = _float(usage.get("max_disagreement_score"), default=0.5)
    rows: list[dict[str, Any]] = []
    for keys, scores in score_means.iterrows():
        finite_scores = [float(value) for value in scores if math.isfinite(float(value))]
        if not finite_scores:
            continue
        best_state = str(scores.idxmax())
        sorted_scores = sorted(finite_scores, reverse=True)
        best_score = sorted_scores[0]
        second_score = sorted_scores[1] if len(sorted_scores) > 1 else best_score
        margin = best_score - second_score
        votes = vote_counts.loc[keys].fillna(0) if keys in vote_counts.index else pd.Series()
        vote_dict = {state: int(votes.get(state, 0)) for state in STATE_ORDER}
        probe_count = int(probe_counts.get(keys, 0))
        vote_share = vote_dict.get(best_state, 0) / max(1, probe_count)
        disagreement = round(1.0 - vote_share, GRID_ROUND_DIGITS)
        confidence = round(
            max(vote_share, _confidence_from_margin(margin, best_score)),
            GRID_ROUND_DIGITS,
        )
        train_usable = confidence >= confidence_floor and disagreement <= max_disagreement
        rows.append(
            {
                "date": keys[0],
                "horizon_days": int(keys[1]),
                "consensus_state": best_state,
                "consensus_confidence": confidence,
                "probe_votes": vote_dict,
                "disagreement_score": disagreement,
                "score_margin": round(margin, GRID_ROUND_DIGITS),
                "train_usable": train_usable,
                "allowed_training_usage": (
                    ["train_if_confidence_above_threshold"] if train_usable else []
                ),
            }
        )
    return pd.DataFrame(rows)


def _cached_future_metrics(
    *,
    returns: pd.DataFrame,
    weights: Mapping[str, float],
    horizon: int,
    row_count: int,
    cache: dict[tuple[tuple[float, float, float], int], list[dict[str, float]]],
    weight_key: tuple[float, float, float],
) -> list[dict[str, float]]:
    cache_key = (weight_key, horizon)
    if cache_key not in cache:
        cache[cache_key] = _future_metric_rows(
            returns=returns,
            weights=weights,
            horizon=horizon,
            row_count=row_count,
        )
    return cache[cache_key]


def _future_metric_rows(
    *,
    returns: pd.DataFrame,
    weights: Mapping[str, float],
    horizon: int,
    row_count: int,
) -> list[dict[str, float]]:
    weight_series = pd.Series(weights).reindex(returns.columns).fillna(0.0)
    portfolio_returns = (returns * weight_series).sum(axis=1).to_numpy(dtype=float)
    rows = []
    for idx in range(row_count):
        window = portfolio_returns[idx + 1 : idx + horizon + 1]
        if window.size == 0:
            rows.append(
                {
                    "future_return": 0.0,
                    "future_max_drawdown": 0.0,
                    "worst_1d_return": 0.0,
                    "worst_5d_return": 0.0,
                    "worst_20d_return": 0.0,
                }
            )
            continue
        equity = np.cumprod(1.0 + window)
        running_max = np.maximum.accumulate(equity)
        drawdown = equity / running_max - 1.0
        rows.append(
            {
                "future_return": float(equity[-1] - 1.0),
                "future_max_drawdown": float(drawdown.min()),
                "worst_1d_return": float(window.min()),
                "worst_5d_return": _worst_array_window_return(window, 5),
                "worst_20d_return": _worst_array_window_return(window, 20),
            }
        )
    return rows


def _worst_array_window_return(values: np.ndarray, window: int) -> float:
    if len(values) < window:
        return float(values.min()) if len(values) else 0.0
    worst = 0.0
    for idx in range(len(values) - window + 1):
        compounded = float(np.prod(1.0 + values[idx : idx + window]) - 1.0)
        worst = min(worst, compounded)
    return worst


def _normalize_action_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    output = {asset: round(_float(weights.get(asset)), GRID_ROUND_DIGITS) for asset in ASSETS}
    if abs(sum(output.values())) <= 1e-12:
        return {}
    return output


def _weight_key(weights: Mapping[str, float]) -> tuple[float, float, float]:
    return (
        round(_float(weights.get("QQQ")), GRID_ROUND_DIGITS),
        round(_float(weights.get("SGOV")), GRID_ROUND_DIGITS),
        round(_float(weights.get("TQQQ")), GRID_ROUND_DIGITS),
    )


def _label_predictions(labels: pd.DataFrame) -> pd.DataFrame:
    frame = labels.loc[labels["horizon_days"].astype(int) == 20].copy()
    return pd.DataFrame(
        {
            "date": frame["date"],
            "trend_state": frame["consensus_state"],
            "confidence": frame["consensus_confidence"],
            "validity_days": 10,
            "decay_profile": "medium",
        }
    )


def _probe_metrics(
    *,
    prices: pd.DataFrame,
    predictions: pd.DataFrame,
    probe: Mapping[str, Any],
    model_id: str,
) -> dict[str, Any]:
    from ai_trading_system.first_layer_policy_calibration import _backtest_probe_predictions

    raw = _backtest_probe_predictions(
        prices=prices,
        predictions=predictions,
        probe=probe,
        model_id=model_id,
    )
    weights_by_state = _mapping(probe.get("weights_by_trend_state"))
    avg_exposure = sum(
        _qqq_equiv(_mapping(weights_by_state.get(state))) for state in weights_by_state
    ) / max(len(weights_by_state), 1)
    return {
        "strategy_id": str(probe.get("probe_id")),
        "strategy_family": str(probe.get("role", "second_layer_probe")),
        "annual_return": raw["actual_path_annual_return"],
        "max_drawdown": raw["max_drawdown_daily_equity"],
        "sharpe": raw["sharpe_daily_zero_rf"],
        "calmar": raw["calmar_daily_equity_dd"],
        "turnover": raw["turnover"],
        "net_annual_return": raw["net_of_cost_return"],
        "tqqq_exposure": raw["tqqq_max_weight"],
        "qqq_equivalent_exposure": round(avg_exposure, GRID_ROUND_DIGITS),
    }


def _limited_adjustment_probe() -> list[dict[str, Any]]:
    return [
        {
            "probe_id": "limited_adjustment",
            "role": "limited_adjustment_baseline",
            "weights_by_trend_state": {
                "risk_off": {"QQQ": 0.45, "SGOV": 0.55, "TQQQ": 0.0},
                "defensive": {"QQQ": 0.45, "SGOV": 0.55, "TQQQ": 0.0},
                "neutral": {"QQQ": 0.65, "SGOV": 0.35, "TQQQ": 0.0},
                "constructive": {"QQQ": 0.65, "SGOV": 0.35, "TQQQ": 0.0},
                "risk_on": {"QQQ": 0.65, "SGOV": 0.35, "TQQQ": 0.0},
            },
        }
    ]


def _weight_grid(step: float) -> list[dict[str, float]]:
    units = int(round(1.0 / step))
    rows = []
    for qqq_units in range(units + 1):
        for sgov_units in range(units - qqq_units + 1):
            tqqq_units = units - qqq_units - sgov_units
            rows.append(
                {
                    "QQQ": round(qqq_units / units, 6),
                    "SGOV": round(sgov_units / units, 6),
                    "TQQQ": round(tqqq_units / units, 6),
                }
            )
    return rows


def _same_risk_static_baseline(
    *,
    static_metrics: pd.DataFrame,
    window_id: str,
    qqq_equiv: float,
) -> dict[str, Any]:
    frame = static_metrics.loc[static_metrics["research_window_id"] == window_id].copy()
    if frame.empty:
        return {}
    frame["risk_distance"] = (frame["qqq_equivalent_exposure"].astype(float) - qqq_equiv).abs()
    selected = frame.sort_values(["risk_distance", "annual_return"], ascending=[True, False]).iloc[
        0
    ]
    return dict(selected)


def _static_frontier_payload(static_metrics: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for window_id, frame in static_metrics.groupby("research_window_id"):
        top = frame.sort_values("annual_return", ascending=False).iloc[0]
        rows.append(
            {
                "research_window_id": window_id,
                "best_strategy_id": top["strategy_id"],
                "best_annual_return": _float(top["annual_return"]),
                "best_calmar": _float(top["calmar_daily_equity_dd"]),
                "grid_size": len(frame),
            }
        )
    return _payload(
        report_type="static_frontier_multi_window_matrix",
        title="Static Frontier Multi-Window Matrix",
        status="STATIC_FRONTIER_MULTI_WINDOW_READY_PROMOTION_BLOCKED",
        summary={"window_count": len(rows), "grid_step": 0.10},
        window_rows=rows,
    )


def _second_layer_payload(probe_metrics: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for strategy_id, frame in probe_metrics.groupby("strategy_id"):
        rows.append(
            {
                "strategy_id": strategy_id,
                "window_count": int(frame["research_window_id"].nunique()),
                "annual_return_range": round(
                    float(frame["annual_return"].max() - frame["annual_return"].min()),
                    GRID_ROUND_DIGITS,
                ),
                "best_window": str(
                    frame.sort_values("annual_return", ascending=False).iloc[0][
                        "research_window_id"
                    ]
                ),
            }
        )
    return _payload(
        report_type="second_layer_probe_window_stability_matrix",
        title="Second-Layer Probe Window Stability Matrix",
        status="SECOND_LAYER_PROBE_MULTI_WINDOW_READY_PROMOTION_BLOCKED",
        summary={
            "strategy_count": len(rows),
            "window_count": probe_metrics["research_window_id"].nunique(),
        },
        strategy_rows=rows,
    )


def _actual_path_payload(actual_path_matrix: pd.DataFrame) -> dict[str, Any]:
    return _payload(
        report_type="actual_path_multi_window_matrix",
        title="Actual-Path Multi-Window Matrix",
        status="ACTUAL_PATH_MULTI_WINDOW_READY_PROMOTION_BLOCKED",
        summary={
            "row_count": len(actual_path_matrix),
            "window_count": int(actual_path_matrix["research_window_id"].nunique()),
            "strategy_count": int(actual_path_matrix["strategy_id"].nunique()),
        },
        sample_rows=_json_records(actual_path_matrix.head(30).to_dict("records")),
    )


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "market_regime": "ai_after_chatgpt",
        "summary": dict(summary),
        **SAFETY_BOUNDARY,
        **extra,
    }


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- 市场周期：`{payload.get('market_regime')}`",
        "",
        "## 摘要",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## 安全边界",
            f"- promotion_allowed: `{payload.get('promotion_allowed')}`",
            f"- paper_shadow_allowed: `{payload.get('paper_shadow_allowed')}`",
            f"- production_allowed: `{payload.get('production_allowed')}`",
            f"- broker_action: `{payload.get('broker_action')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_owner_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Owner 问答", ""]
    for key, value in _mapping(payload.get("owner_answers")).items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _render_dependency_doc(walk_forward_policy_path: Path) -> str:
    payload = _payload(
        report_type="first_layer_up_state_window_dependency_update",
        title="First-Layer Up-State Window Dependency Update",
        status="FIRST_LAYER_UP_STATE_WINDOW_DEPENDENCY_READY_PROMOTION_BLOCKED",
        summary={
            "primary_window": "exact_three_asset_validated",
            "legacy_window_role": "legacy_comparison_only",
            "extension_window_role": "sensitivity_only",
            "walk_forward_policy": str(walk_forward_policy_path),
        },
    )
    return _render_payload_doc(payload)


def _render_window_quality_doc(window_id: str, status: str, metadata: Mapping[str, Any]) -> str:
    payload = _payload(
        report_type="data_quality_window_report",
        title=f"Data Quality Window Report: {window_id}",
        status=status,
        summary=dict(metadata),
    )
    return _render_payload_doc(payload)


def _ticker_rows(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    column = "ticker" if "ticker" in frame.columns else "symbol"
    return frame.loc[frame[column].astype(str) == ticker].copy()


def _min_date(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    return pd.to_datetime(frame["date"]).min().date().isoformat()


def _first_common_tradable_date(frame: pd.DataFrame, tickers: Sequence[str]) -> str:
    dates = []
    for ticker in tickers:
        rows = _ticker_rows(frame, ticker)
        if rows.empty:
            return ""
        dates.append(pd.to_datetime(rows["date"]).min().date())
    return max(dates).isoformat()


def _secondary_status(ticker: str, frame: pd.DataFrame) -> str:
    if frame.empty:
        return "missing"
    earliest = pd.to_datetime(frame["date"]).min().date()
    if ticker == "SGOV" and earliest > date(2020, 5, 28):
        return "unavailable_before_2021_02_22"
    return "available"


def _metadata_feature_row(
    name: str, exact_or_proxy: str, allowed_usage: list[str]
) -> dict[str, Any]:
    return {
        "asset_or_feature": name,
        "earliest_available_date": "",
        "first_common_tradable_date": "",
        "primary_source_status": "registered_metadata",
        "secondary_source_status": "not_validated_for_this_batch",
        "exact_or_proxy": exact_or_proxy,
        "PIT_status": "PIT_BLOCKED" if exact_or_proxy == "metadata_only" else "PIT_REVIEW_REQUIRED",
        "revision_risk": "review_required",
        "survivorship_risk": "review_required",
        "allowed_usage": allowed_usage,
        "blocked_usage": [
            "exact_three_asset_leaderboard",
            "production",
            "paper_shadow",
            "broker_order",
        ],
    }


def _load_rates_long(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    if {"date", "series", "value"} <= set(frame.columns):
        return frame
    return pd.DataFrame(columns=["date", "series", "value"])


def _row_status(rows: Sequence[Mapping[str, Any]], window_id: str) -> str:
    for row in rows:
        if row.get("research_window_id") == window_id:
            return str(row.get("window_data_quality_status"))
    return "MISSING"


def _row_value(rows: Sequence[Mapping[str, Any]], window_id: str, key: str) -> Any:
    for row in rows:
        if row.get("research_window_id") == window_id:
            return row.get(key)
    return None


def _window_fields_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "research_window_id",
            "requested_start",
            "actual_start",
            "actual_portfolio_start",
            "end",
            "window_role",
            "data_quality_contract",
            "exact_or_proxy",
        )
    }


def _stability_from_row(row: Mapping[str, Any]) -> str:
    delta_primary = abs(_float(row.get("delta_2021_vs_2022")))
    delta_extension = abs(_float(row.get("delta_2020_vs_2021")))
    if delta_primary < 0.03 and delta_extension < 0.03:
        return "WINDOW_STABLE"
    if delta_primary < 0.08:
        return "WINDOW_SENSITIVE_BUT_EXPLAINED"
    if (
        _float(row.get("legacy_annual_return")) > 0
        and _float(row.get("primary_annual_return")) <= 0
    ):
        return "LEGACY_WINDOW_ONLY_EDGE"
    if str(row.get("window_role")) == "sensitivity":
        return "EXTENSION_WINDOW_CAVEATED"
    return "WINDOW_SENSITIVE_UNEXPLAINED"


def _state_counts(series: pd.Series) -> dict[str, int]:
    return {str(key): int(value) for key, value in series.astype(str).value_counts().items()}


def _label_scope() -> dict[str, Any]:
    return {"label_usage": {"high_confidence_floor": 0.55, "max_disagreement_score": 0.50}}


def _qqq_equiv(weights: Mapping[str, Any]) -> float:
    return _float(weights.get("QQQ")) + 3.0 * _float(weights.get("TQQQ"))


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _string_list(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _list_values(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    if abs(denom) <= 1e-12:
        return 0.0
    return _float(numerator) / denom


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _json_records(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(_json_scalar(row)) for row in rows]


def _json_scalar(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def _issue(code: str) -> dict[str, str]:
    return {"code": code, "severity": "ERROR"}
