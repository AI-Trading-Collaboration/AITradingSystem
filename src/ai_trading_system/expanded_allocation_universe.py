from __future__ import annotations

import json
import math
import subprocess
from collections.abc import Mapping, Sequence
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.data_foundation import AI_REGIME_START, utc_now_iso
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "expanded_allocation_universe.yaml"
)
DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "expanded_universe"
)
DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT = (
    DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT / "static_simplex_grid"
)
DEFAULT_ACTUAL_PATH_OUTPUT_ROOT = (
    DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT / "actual_path_rebacktest"
)
DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH = (
    DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT / "state_portfolio_candidates.json"
)
DEFAULT_FAILURE_MATRIX_CSV_PATH = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT / "candidate_failure_matrix.csv"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"

DEFAULT_SCOPE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_qqq_sgov_tqqq_universe_scope.md"
)
DEFAULT_TQQQ_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "tqqq_data_quality_blocking_review.md"
)
DEFAULT_TQQQ_REVIEW_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "tqqq_data_quality_blocking_review.yaml"
)
DEFAULT_STATIC_FRONTIER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_static_frontier_review.md"
)
DEFAULT_STATIC_FRONTIER_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_universe_static_frontier_matrix.yaml"
)
DEFAULT_TQQQ_ATTRIBUTION_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "tqqq_risk_attribution_review.md"
)
DEFAULT_TQQQ_ATTRIBUTION_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "tqqq_risk_attribution_matrix.yaml"
)
DEFAULT_SAME_RISK_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_universe_same_risk_baseline_comparison.yaml"
)
DEFAULT_SURVIVAL_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_dynamic_candidate_survival_review.md"
)
DEFAULT_SURVIVAL_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_dynamic_candidate_survival_matrix.yaml"
)
DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_walk_forward_review.md"
)
DEFAULT_WALK_FORWARD_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_universe_walk_forward_matrix.yaml"
)
DEFAULT_NET_COST_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_net_of_cost_review.md"
)
DEFAULT_STRESS_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_universe_stress_risk_review.md"
)
DEFAULT_FAILURE_MATRIX_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "expanded_actual_path_candidate_failure_matrix.md"
)
DEFAULT_FAILURE_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "expanded_actual_path_candidate_failure_matrix.yaml"
)
DEFAULT_OWNER_REVIEW_PACK_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "expanded_qqq_sgov_tqqq_allocation_owner_review_pack.md"
)

DEFAULT_AI_REGIME_BACKTEST_START = (
    AI_REGIME_START
    if isinstance(AI_REGIME_START, date)
    else date.fromisoformat(str(AI_REGIME_START))
)
GRID_ROUND_DIGITS = 6
SAFETY_BOUNDARY: dict[str, Any] = {
    "research_only": True,
    "candidate_only": True,
    "actual_path_required": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "manual_review_required": True,
    "dynamic_promotion_status": "BLOCKED",
    "target_path_metrics_role": "diagnostic_only",
}


def run_expanded_universe_scope_review(
    *,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    docs_path: Path = DEFAULT_SCOPE_DOC_PATH,
) -> dict[str, Any]:
    config = _load_config(config_path)
    assets = _assets_from_config(config)
    payload = _payload(
        report_type="expanded_qqq_sgov_tqqq_universe_scope",
        title="Expanded QQQ / SGOV / TQQQ Universe Scope",
        status="EXPANDED_UNIVERSE_SCOPE_READY",
        summary={
            "asset_count": len(assets),
            "assets": ",".join(assets),
            "tqqq_role": "research_only",
            "promotion_status": "BLOCKED",
            "config_path": str(config_path),
        },
        config_hash=_file_sha256(config_path),
        asset_universe=_mapping(config.get("asset_universe")),
        exploration_weight_constraints=_mapping(config.get("exploration_weight_constraints")),
        promotion_candidate_constraints=_mapping(config.get("promotion_candidate_constraints")),
        grid_levels=_mapping(config.get("grid_levels")),
        safety_boundary=dict(SAFETY_BOUNDARY),
    )
    _write_markdown(docs_path, _render_scope_doc(payload))
    payload["artifact_paths"] = {"markdown_path": str(docs_path), "config_path": str(config_path)}
    return payload


def run_tqqq_data_quality_blocking_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    docs_path: Path = DEFAULT_TQQQ_REVIEW_DOC_PATH,
    yaml_path: Path = DEFAULT_TQQQ_REVIEW_YAML_PATH,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=["QQQ", "SGOV", "TQQQ"],
    )
    tqqq_issues = [
        issue
        for issue in _records(data_gate.get("issues"))
        if "TQQQ" in str(issue.get("message")) or "TQQQ" in str(issue.get("sample"))
    ]
    status = str(_mapping(config.get("data_quality")).get("tqqq_default_research_status"))
    if not data_gate["passed"]:
        status = "TQQQ_BLOCKED"
    review = {
        **_payload(
            report_type="tqqq_data_quality_blocking_review",
            title="TQQQ Data Quality Blocking Review",
            status=status,
            summary={
                "data_quality_status": data_gate["status"],
                "tqqq_issue_count": len(tqqq_issues),
                "research_universe_status": status,
                "promotion_universe_status": "TQQQ_DATA_QUALITY_BLOCKING_REVIEW_REQUIRED",
            },
            config_hash=_file_sha256(config_path),
            data_quality=data_gate,
            tqqq_issues=tqqq_issues,
            required_reviews=[
                "primary_adjusted_close_review",
                "secondary_source_comparison",
                "corporate_action_split_dividend_adjustment_review",
                "sample_date_inspection",
                "affected_strategy_impact_diagnosis",
            ],
        ),
        "schema_version": "tqqq_data_quality_blocking_review.v1",
        "promotion_universe_status": "TQQQ_DATA_QUALITY_BLOCKING_REVIEW_REQUIRED",
    }
    _write_yaml(yaml_path, review)
    _write_markdown(docs_path, _render_tqqq_review_doc(review))
    review["artifact_paths"] = {"yaml_path": str(yaml_path), "markdown_path": str(docs_path)}
    return review


def run_static_simplex_grid(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    output_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    assets: Sequence[str] | None = None,
    step: float | None = None,
    rebalance: str | None = None,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    selected_assets = [str(asset) for asset in (assets or _assets_from_config(config))]
    grid_step = float(step or _grid_step(config, "medium"))
    rebalance_frequency = rebalance or str(
        _mapping(config.get("backtest_policy")).get("rebalance_frequency", "monthly")
    )
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=list(selected_assets),
    )
    output_root.mkdir(parents=True, exist_ok=True)
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="static_simplex_grid_index",
            title="Static Simplex Grid Index",
            status="STATIC_SIMPLEX_GRID_BLOCKED",
            data_gate=data_gate,
        )
        _write_json(output_root / "static_simplex_grid_index.json", payload)
        return payload

    prices = _slice_prices(
        _load_price_matrix(prices_path, list(selected_assets)),
        start_date=start_date,
        end_date=end_date,
    )
    cost_bps = _float(_mapping(config.get("backtest_policy")).get("transaction_cost_bps"))
    annualization = _int(
        _mapping(config.get("backtest_policy")).get("annualization_trading_days"),
        default=252,
    )
    rows: list[dict[str, Any]] = []
    for combo_id, weights in enumerate(_simplex_weights(selected_assets, grid_step), start=1):
        target = _constant_target_frame(prices.index, prices.columns, weights)
        sim = _simulate_rebalanced_portfolio(
            prices,
            target,
            rebalance=rebalance_frequency,
            transaction_cost_bps=cost_bps,
        )
        rows.append(
            _metrics_row(
                strategy_id=_static_combo_id(weights),
                candidate_family="static_simplex_grid",
                weights=weights,
                sim=sim,
                annualization=annualization,
                selection_rank=combo_id,
            )
        )

    buckets = _records(config.get("risk_buckets"))
    for row in rows:
        row["risk_bucket"] = _bucket_for_exposure(
            _float(row.get("qqq_equivalent_exposure")),
            buckets,
        )

    metrics = pd.DataFrame(rows).sort_values(
        ["qqq_equivalent_exposure", "tqqq_weight", "qqq_weight", "sgov_weight"]
    )
    frontier = pd.DataFrame(_frontier_rows(rows))
    bucket_summary = pd.DataFrame(_risk_bucket_summary(rows, config))

    metrics_path = output_root / "static_simplex_grid_metrics.csv"
    frontier_path = output_root / "static_simplex_grid_pareto_frontier.csv"
    bucket_path = output_root / "static_simplex_grid_risk_bucket_summary.csv"
    metrics.to_csv(metrics_path, index=False)
    frontier.to_csv(frontier_path, index=False)
    bucket_summary.to_csv(bucket_path, index=False)
    index_payload = _payload(
        report_type="static_simplex_grid_index",
        title="Static Simplex Grid Index",
        status="STATIC_SIMPLEX_GRID_READY",
        summary={
            "grid_step": grid_step,
            "static_grid_size": int(len(metrics)),
            "frontier_row_count": int(len(frontier)),
            "risk_bucket_count": int(len(bucket_summary)),
            "data_quality_status": data_gate["status"],
            "rebalance_frequency": rebalance_frequency,
        },
        config_hash=_file_sha256(config_path),
        data_quality=data_gate,
        requested_date_range=_date_range_from_index(prices.index, start_date, end_date),
        artifact_paths={
            "index": str(output_root / "static_simplex_grid_index.json"),
            "metrics_csv": str(metrics_path),
            "pareto_frontier_csv": str(frontier_path),
            "risk_bucket_summary_csv": str(bucket_path),
        },
    )
    _write_json(output_root / "static_simplex_grid_index.json", index_payload)
    _write_markdown(
        output_root / "static_simplex_grid_index.md",
        _render_static_index(index_payload),
    )
    return index_payload


def run_static_frontier_review(
    *,
    static_grid_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    docs_path: Path = DEFAULT_STATIC_FRONTIER_REVIEW_DOC_PATH,
    yaml_path: Path = DEFAULT_STATIC_FRONTIER_MATRIX_PATH,
) -> dict[str, Any]:
    metrics_path = static_grid_root / "static_simplex_grid_metrics.csv"
    frontier_path = static_grid_root / "static_simplex_grid_pareto_frontier.csv"
    if not metrics_path.exists() or not frontier_path.exists():
        payload = _payload(
            report_type="expanded_universe_static_frontier_review",
            title="Expanded Universe Static Frontier Review",
            status="STATIC_FRONTIER_REVIEW_BLOCKED",
            summary={"blocked_reason": "static_grid_artifacts_missing"},
            blockers=[str(metrics_path), str(frontier_path)],
        )
        _write_yaml(yaml_path, payload)
        _write_markdown(docs_path, _render_review_doc(payload))
        return payload

    metrics = pd.read_csv(metrics_path)
    frontier = pd.read_csv(frontier_path)
    top_static = _top_records(metrics, "calmar_daily_equity_dd", 10)
    payload = _payload(
        report_type="expanded_universe_static_frontier_review",
        title="Expanded Universe Static Frontier Review",
        status="STATIC_FRONTIER_REVIEW_READY",
        summary={
            "static_grid_size": int(len(metrics)),
            "frontier_row_count": int(len(frontier)),
            "best_static_candidate": top_static[0]["strategy_id"] if top_static else None,
            "config_hash": _file_sha256(config_path),
        },
        schema_version="expanded_universe_static_frontier_matrix.v1",
        config_hash=_file_sha256(config_path),
        top_static_candidates=top_static,
        frontier_type_counts=_value_counts(frontier, "frontier_type"),
        source_artifacts={
            "metrics_csv": str(metrics_path),
            "pareto_frontier_csv": str(frontier_path),
        },
    )
    _write_yaml(yaml_path, payload)
    _write_markdown(docs_path, _render_static_frontier_doc(payload))
    payload["artifact_paths"] = {"yaml_path": str(yaml_path), "markdown_path": str(docs_path)}
    return payload


def run_risk_bucket_representatives(
    *,
    static_grid_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    output_path: Path | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    metrics_path = static_grid_root / "static_simplex_grid_metrics.csv"
    output_path = (
        output_path
        or DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT / "risk_bucket_representatives.csv"
    )
    if not metrics_path.exists():
        payload = _payload(
            report_type="risk_bucket_representatives",
            title="Risk Bucket Representatives",
            status="RISK_BUCKET_REPRESENTATIVES_BLOCKED",
            summary={"blocked_reason": "static_grid_metrics_missing"},
            blockers=[str(metrics_path)],
        )
        return payload

    metrics = pd.read_csv(metrics_path)
    rows: list[dict[str, Any]] = []
    max_dd_cap = _float(
        _mapping(config.get("risk_bucket_selector")).get("max_drawdown_cap_for_return_selector")
    )
    for bucket in _records(config.get("risk_buckets")):
        bucket_id = str(bucket["bucket_id"])
        subset = metrics.loc[metrics["risk_bucket"] == bucket_id].copy()
        if subset.empty:
            continue
        selectors = {
            "highest_sharpe": subset.sort_values("sharpe_daily_zero_rf", ascending=False),
            "lowest_drawdown": subset.assign(
                abs_drawdown=subset["max_drawdown_daily_equity"].abs()
            ).sort_values("abs_drawdown"),
            "highest_calmar": subset.sort_values("calmar_daily_equity_dd", ascending=False),
            "highest_return_under_max_dd_cap": subset.loc[
                subset["max_drawdown_daily_equity"] >= max_dd_cap
            ].sort_values("annual_return", ascending=False),
            "lowest_turnover": subset.sort_values("turnover"),
        }
        for selector_id, ordered in selectors.items():
            if ordered.empty:
                ordered = subset.sort_values("annual_return", ascending=False)
            row = dict(ordered.iloc[0].to_dict())
            row["bucket_id"] = bucket_id
            row["selector_id"] = selector_id
            rows.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False)
    payload = _payload(
        report_type="risk_bucket_representatives",
        title="Risk Bucket Representatives",
        status="RISK_BUCKET_REPRESENTATIVES_READY",
        summary={
            "representative_count": len(rows),
            "bucket_count": len({row["bucket_id"] for row in rows}),
        },
        artifact_paths={"csv_path": str(output_path)},
        representative_rows=_json_records(rows),
    )
    return payload


def run_state_portfolio_candidates(
    *,
    representatives_path: Path | None = None,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    output_path: Path | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    representatives_path = representatives_path or (
        DEFAULT_EXPANDED_UNIVERSE_OUTPUT_ROOT / "risk_bucket_representatives.csv"
    )
    output_path = output_path or DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH
    if not representatives_path.exists():
        payload = _payload(
            report_type="state_portfolio_candidates",
            title="State Portfolio Candidates",
            status="STATE_PORTFOLIO_CANDIDATES_BLOCKED",
            summary={"blocked_reason": "risk_bucket_representatives_missing"},
            blockers=[str(representatives_path)],
        )
        _write_json(output_path, payload)
        return payload

    reps = pd.read_csv(representatives_path)
    states = [
        str(value)
        for value in _mapping(config.get("state_mapping_policy")).get("states", [])
    ]
    preferred = _mapping(config.get("state_mapping_policy")).get("preferred_bucket_by_state", {})
    selectors = list(dict.fromkeys(str(value) for value in reps["selector_id"].tolist()))
    candidates = []
    for selector_id in selectors:
        state_weights: dict[str, dict[str, float]] = {}
        source_static_ids: dict[str, str] = {}
        warnings: list[str] = []
        for state in states:
            bucket_id = str(_mapping(preferred).get(state))
            row = _representative_row(reps, bucket_id=bucket_id, selector_id=selector_id)
            if row is None:
                row = dict(reps.iloc[0].to_dict())
                warnings.append(f"missing representative for {state}/{bucket_id}/{selector_id}")
            state_weights[state] = _weights_from_row(row)
            source_static_ids[state] = str(row.get("strategy_id"))
        classifier = classify_monotonic_risk_profile(state_weights, states=states)
        candidates.append(
            {
                "strategy_id": f"expanded_state_{selector_id}",
                "candidate_family": "state_to_portfolio_from_risk_buckets",
                "selector_id": selector_id,
                "state_portfolios": state_weights,
                "source_static_strategy_ids": source_static_ids,
                "monotonic_risk_profile": classifier["monotonic_risk_profile"],
                "risk_exposure_by_state": classifier["risk_exposure_by_state"],
                "violations": classifier["violations"],
                "warnings": warnings,
                **SAFETY_BOUNDARY,
            }
        )

    payload = _payload(
        report_type="state_portfolio_candidates",
        title="State Portfolio Candidates",
        status="STATE_PORTFOLIO_CANDIDATES_READY",
        summary={
            "candidate_count": len(candidates),
            "monotonic_candidate_count": sum(
                1 for item in candidates if item["monotonic_risk_profile"]
            ),
        },
        config_hash=_file_sha256(config_path),
        candidates=candidates,
        artifact_paths={"json_path": str(output_path)},
    )
    _write_json(output_path, payload)
    return payload


def classify_monotonic_risk_profile(
    state_portfolios: Mapping[str, Mapping[str, float]],
    *,
    states: Sequence[str],
) -> dict[str, Any]:
    exposures = {
        state: round(
            _float(_mapping(state_portfolios.get(state)).get("QQQ"))
            + 3.0 * _float(_mapping(state_portfolios.get(state)).get("TQQQ")),
            GRID_ROUND_DIGITS,
        )
        for state in states
    }
    violations = []
    for left, right in zip(states, states[1:], strict=False):
        if exposures[left] > exposures[right]:
            violations.append(
                {
                    "from_state": left,
                    "to_state": right,
                    "from_exposure": exposures[left],
                    "to_exposure": exposures[right],
                }
            )
    return {
        "monotonic_risk_profile": not violations,
        "risk_exposure_by_state": exposures,
        "violations": violations,
    }


def run_expanded_actual_path_rebacktest(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    static_grid_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    candidates_path: Path | None = None,
    output_root: Path = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_config(config_path)
    assets = _assets_from_config(config)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=assets,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    if not data_gate["passed"]:
        payload = _blocked_payload(
            report_type="expanded_universe_actual_path_rebacktest",
            title="Expanded Universe Actual-Path Rebacktest",
            status="EXPANDED_ACTUAL_PATH_REBACKTEST_BLOCKED",
            data_gate=data_gate,
        )
        _write_json(output_root / "expanded_universe_promotion_readiness.json", payload)
        return payload

    prices = _slice_prices(
        _load_price_matrix(prices_path, assets),
        start_date=start_date,
        end_date=end_date,
    )
    annualization = _int(
        _mapping(config.get("backtest_policy")).get("annualization_trading_days"),
        default=252,
    )
    cost_bps = _float(_mapping(config.get("backtest_policy")).get("transaction_cost_bps"))
    state_series = _trend_state_series(prices, config)
    strategy_targets = _actual_path_strategy_targets(
        prices=prices,
        states=state_series,
        static_grid_root=static_grid_root,
        candidates_path=candidates_path or DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH,
        config=config,
    )

    actual_rows = []
    target_rows = []
    gap_rows = []
    tqqq_rows = []
    for strategy_id, target_frame in strategy_targets.items():
        actual_target = target_frame.shift(1)
        if not actual_target.empty:
            actual_target.iloc[0] = target_frame.iloc[0]
        actual_target = actual_target.ffill().fillna(0.0)
        actual_sim = _simulate_rebalanced_portfolio(
            prices,
            actual_target,
            rebalance="daily",
            transaction_cost_bps=cost_bps,
        )
        target_sim = _simulate_rebalanced_portfolio(
            prices,
            target_frame,
            rebalance="daily",
            transaction_cost_bps=cost_bps,
        )
        actual_metric = _metrics_row(
            strategy_id=strategy_id,
            candidate_family=_candidate_family(strategy_id),
            weights=_average_weight_dict(actual_sim["applied_weights"]),
            sim=actual_sim,
            annualization=annualization,
            selection_rank=len(actual_rows) + 1,
            prefix="actual_path_",
        )
        target_metric = _metrics_row(
            strategy_id=strategy_id,
            candidate_family=_candidate_family(strategy_id),
            weights=_average_weight_dict(target_sim["applied_weights"]),
            sim=target_sim,
            annualization=annualization,
            selection_rank=len(target_rows) + 1,
            prefix="target_path_",
        )
        actual_rows.append(actual_metric)
        target_rows.append(target_metric)
        gap_rows.append(_target_actual_gap_row(strategy_id, actual_metric, target_metric))
        tqqq_rows.append(_tqqq_risk_row(strategy_id, actual_sim))
        _write_actual_path_positions(
            output_root / f"{strategy_id}_actual_daily_positions.csv",
            strategy_id=strategy_id,
            states=state_series,
            target_frame=target_frame,
            actual_frame=actual_target,
            sim=actual_sim,
        )

    actual_df = pd.DataFrame(actual_rows).sort_values(
        "actual_path_annual_return", ascending=False
    )
    target_df = pd.DataFrame(target_rows).sort_values(
        "target_path_annual_return", ascending=False
    )
    gap_df = pd.DataFrame(gap_rows)
    tqqq_df = pd.DataFrame(tqqq_rows)
    actual_df.to_csv(output_root / "leaderboard_actual_path.csv", index=False)
    target_df.to_csv(output_root / "leaderboard_target_path_diagnostic.csv", index=False)
    gap_df.to_csv(output_root / "target_vs_actual_gap_summary.csv", index=False)
    tqqq_df.to_csv(output_root / "tqqq_risk_summary.csv", index=False)

    readiness = _payload(
        report_type="expanded_universe_promotion_readiness",
        title="Expanded Universe Promotion Readiness",
        status="EXPANDED_UNIVERSE_RESEARCH_READY_PROMOTION_BLOCKED",
        summary={
            "strategy_count": len(actual_df),
            "best_actual_path_strategy": (
                str(actual_df.iloc[0]["strategy_id"]) if not actual_df.empty else None
            ),
            "data_quality_status": data_gate["status"],
            "dynamic_promotion_status": "BLOCKED",
            "target_path_metrics_role": "diagnostic_only",
        },
        schema_version="expanded_universe_promotion_readiness.v1",
        config_hash=_file_sha256(config_path),
        data_quality=data_gate,
        dynamic_promotion={"final_status": "BLOCKED"},
        promotion_blockers=[
            "OWNER_MANUAL_REVIEW_REQUIRED",
            "TQQQ_PROMOTION_UNIVERSE_BLOCKING_REVIEW_REQUIRED",
            "WALK_FORWARD_VALIDATION_REQUIRED",
            "STRESS_RISK_REVIEW_REQUIRED",
            "TARGET_PATH_METRICS_DIAGNOSTIC_ONLY",
        ],
        requested_date_range=_date_range_from_index(prices.index, start_date, end_date),
        artifact_paths={
            "leaderboard_actual_path": str(output_root / "leaderboard_actual_path.csv"),
            "leaderboard_target_path_diagnostic": str(
                output_root / "leaderboard_target_path_diagnostic.csv"
            ),
            "target_vs_actual_gap_summary": str(
                output_root / "target_vs_actual_gap_summary.csv"
            ),
            "tqqq_risk_summary": str(output_root / "tqqq_risk_summary.csv"),
            "promotion_readiness": str(
                output_root / "expanded_universe_promotion_readiness.json"
            ),
        },
    )
    _write_json(output_root / "expanded_universe_promotion_readiness.json", readiness)
    _write_markdown(output_root / "owner_review_pack.md", _render_actual_path_owner_pack(readiness))
    return readiness


def run_expanded_universe_owner_review_pack(
    *,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    static_grid_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    actual_path_root: Path = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    tqqq_review_yaml_path: Path = DEFAULT_TQQQ_REVIEW_YAML_PATH,
    tqqq_attribution_doc_path: Path = DEFAULT_TQQQ_ATTRIBUTION_REVIEW_DOC_PATH,
    tqqq_attribution_yaml_path: Path = DEFAULT_TQQQ_ATTRIBUTION_MATRIX_PATH,
    same_risk_yaml_path: Path = DEFAULT_SAME_RISK_MATRIX_PATH,
    survival_doc_path: Path = DEFAULT_SURVIVAL_REVIEW_DOC_PATH,
    survival_yaml_path: Path = DEFAULT_SURVIVAL_MATRIX_PATH,
    walk_forward_doc_path: Path = DEFAULT_WALK_FORWARD_REVIEW_DOC_PATH,
    walk_forward_yaml_path: Path = DEFAULT_WALK_FORWARD_MATRIX_PATH,
    net_cost_doc_path: Path = DEFAULT_NET_COST_REVIEW_DOC_PATH,
    stress_doc_path: Path = DEFAULT_STRESS_REVIEW_DOC_PATH,
    owner_doc_path: Path = DEFAULT_OWNER_REVIEW_PACK_DOC_PATH,
) -> dict[str, Any]:
    config = _load_config(config_path)
    metrics_path = static_grid_root / "static_simplex_grid_metrics.csv"
    actual_path = actual_path_root / "leaderboard_actual_path.csv"
    tqqq_risk_path = actual_path_root / "tqqq_risk_summary.csv"
    missing = [
        str(path)
        for path in (metrics_path, actual_path, tqqq_risk_path)
        if not path.exists()
    ]
    if missing:
        payload = _payload(
            report_type="expanded_universe_owner_review_pack",
            title="Expanded QQQ / SGOV / TQQQ Allocation Owner Review Pack",
            status="EXPANDED_OWNER_REVIEW_PACK_BLOCKED",
            summary={"missing_input_count": len(missing)},
            blockers=missing,
        )
        _write_markdown(owner_doc_path, _render_review_doc(payload))
        return payload

    static_metrics = pd.read_csv(metrics_path)
    actual_metrics = pd.read_csv(actual_path)
    tqqq_risk = pd.read_csv(tqqq_risk_path)
    tqqq_review = _read_yaml_or_empty(tqqq_review_yaml_path)

    same_risk_rows = _same_risk_rows(actual_metrics, static_metrics)
    survival_rows = _survival_rows(same_risk_rows, config)
    tqqq_attribution = _review_payload(
        report_type="tqqq_risk_attribution_matrix",
        status="TQQQ_RISK_ATTRIBUTION_READY",
        rows=_json_records(tqqq_risk.to_dict("records")),
        config_hash=_file_sha256(config_path),
    )
    same_risk = _review_payload(
        report_type="expanded_universe_same_risk_baseline_comparison",
        status="SAME_RISK_BASELINE_COMPARISON_READY",
        rows=same_risk_rows,
        config_hash=_file_sha256(config_path),
    )
    survival = _review_payload(
        report_type="expanded_dynamic_candidate_survival_matrix",
        status="EXPANDED_CANDIDATE_SURVIVAL_MATRIX_READY",
        rows=survival_rows,
        config_hash=_file_sha256(config_path),
    )
    walk_forward = _walk_forward_review_payload(actual_metrics, config_path)
    net_cost = _net_cost_review_payload(actual_metrics, config_path)
    stress = _stress_review_payload(tqqq_risk, config_path)

    _write_yaml(tqqq_attribution_yaml_path, tqqq_attribution)
    _write_markdown(
        tqqq_attribution_doc_path,
        _render_table_doc("TQQQ Risk Attribution Review", tqqq_attribution),
    )
    _write_yaml(same_risk_yaml_path, same_risk)
    _write_yaml(survival_yaml_path, survival)
    _write_markdown(survival_doc_path, _render_table_doc("Expanded Candidate Survival", survival))
    _write_yaml(walk_forward_yaml_path, walk_forward)
    _write_markdown(walk_forward_doc_path, _render_table_doc("Walk-Forward Review", walk_forward))
    _write_markdown(net_cost_doc_path, _render_table_doc("Net-of-Cost Review", net_cost))
    _write_markdown(stress_doc_path, _render_table_doc("Stress Risk Review", stress))

    best_static = _top_records(static_metrics, "calmar_daily_equity_dd", 5)
    best_dynamic = _json_records(actual_metrics.head(5).to_dict("records"))
    owner_payload = _payload(
        report_type="expanded_qqq_sgov_tqqq_allocation_owner_review_pack",
        title="Expanded QQQ / SGOV / TQQQ Allocation Owner Review Pack",
        status="EXPANDED_OWNER_REVIEW_PACK_READY_PROMOTION_BLOCKED",
        summary={
            "best_static_candidate": best_static[0]["strategy_id"] if best_static else None,
            "best_dynamic_candidate": best_dynamic[0]["strategy_id"] if best_dynamic else None,
            "surviving_candidate_count": sum(
                1 for row in survival_rows if row["verdict"] == "SURVIVES_EXPANDED_UNIVERSE"
            ),
            "tqqq_data_quality_status": tqqq_review.get("status", "UNKNOWN"),
            "promotion_status": "BLOCKED",
        },
        config_hash=_file_sha256(config_path),
        best_static_candidates=best_static,
        best_dynamic_candidates=best_dynamic,
        same_risk_baseline_comparison=same_risk_rows,
        survival_matrix_rows=survival_rows,
        tqqq_data_quality_review=tqqq_review,
        dynamic_promotion={"final_status": "BLOCKED"},
        remaining_blockers=[
            "owner_review_pending",
            "tqqq_promotion_universe_review_required",
            "walk_forward_validation_blocking_until_real_forward_evidence",
            "stress_review_blocks_high_tqqq_or_high_exposure_candidates",
        ],
        artifact_paths={
            "owner_review_pack": str(owner_doc_path),
            "survival_matrix": str(survival_yaml_path),
            "tqqq_risk_attribution": str(tqqq_attribution_yaml_path),
            "same_risk_comparison": str(same_risk_yaml_path),
            "walk_forward_matrix": str(walk_forward_yaml_path),
        },
    )
    _write_markdown(owner_doc_path, _render_owner_review_doc(owner_payload))
    return owner_payload


def run_expanded_candidate_failure_matrix(
    *,
    config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    static_grid_root: Path = DEFAULT_STATIC_SIMPLEX_GRID_OUTPUT_ROOT,
    actual_path_root: Path = DEFAULT_ACTUAL_PATH_OUTPUT_ROOT,
    candidates_path: Path = DEFAULT_STATE_PORTFOLIO_CANDIDATES_PATH,
    csv_path: Path = DEFAULT_FAILURE_MATRIX_CSV_PATH,
    yaml_path: Path = DEFAULT_FAILURE_MATRIX_YAML_PATH,
    docs_path: Path = DEFAULT_FAILURE_MATRIX_DOC_PATH,
) -> dict[str, Any]:
    config = _load_config(config_path)
    metrics_path = static_grid_root / "static_simplex_grid_metrics.csv"
    actual_path = actual_path_root / "leaderboard_actual_path.csv"
    tqqq_risk_path = actual_path_root / "tqqq_risk_summary.csv"
    missing = [
        str(path)
        for path in (metrics_path, actual_path, tqqq_risk_path, candidates_path)
        if not path.exists()
    ]
    if missing:
        payload = _payload(
            report_type="expanded_actual_path_candidate_failure_matrix",
            title="Expanded Actual-Path Candidate Failure Matrix",
            status="CANDIDATE_FAILURE_MATRIX_BLOCKED",
            summary={"missing_input_count": len(missing)},
            blockers=missing,
        )
        _write_markdown(docs_path, _render_review_doc(payload))
        return payload

    static_metrics = pd.read_csv(metrics_path)
    actual_metrics = pd.read_csv(actual_path)
    tqqq_risk = pd.read_csv(tqqq_risk_path)
    candidates = _read_json_or_empty(candidates_path)
    same_risk_rows = _same_risk_rows(actual_metrics, static_metrics)
    survival_rows = _survival_rows(same_risk_rows, config)
    walk_forward = _walk_forward_review_payload(actual_metrics, config_path)
    net_cost = _net_cost_review_payload(actual_metrics, config_path)
    stress = _stress_review_payload(tqqq_risk, config_path, config)
    rows = _candidate_failure_matrix_rows(
        actual_metrics=actual_metrics,
        same_risk_rows=same_risk_rows,
        survival_rows=survival_rows,
        walk_forward_rows=_records(walk_forward.get("rows")),
        net_cost_rows=_records(net_cost.get("rows")),
        stress_rows=_records(stress.get("rows")),
        tqqq_risk_rows=_json_records(tqqq_risk.to_dict("records")),
        candidates=candidates,
        config=config,
    )
    payload = _review_payload(
        report_type="expanded_actual_path_candidate_failure_matrix",
        status="CANDIDATE_FAILURE_MATRIX_READY_PROMOTION_BLOCKED",
        rows=rows,
        config_hash=_file_sha256(config_path),
    )
    payload["schema_version"] = "expanded_actual_path_candidate_failure_matrix.v1"
    payload["summary"]["candidate_count"] = len(rows)
    payload["artifact_paths"] = {
        "csv_path": str(csv_path),
        "yaml_path": str(yaml_path),
        "markdown_path": str(docs_path),
    }
    _write_failure_matrix_csv(csv_path, rows)
    _write_yaml(yaml_path, payload)
    _write_markdown(docs_path, _render_failure_matrix_doc(payload))
    return payload


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "source_commit": _source_commit(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _review_payload(
    *,
    report_type: str,
    status: str,
    rows: list[dict[str, Any]],
    config_hash: str | None,
) -> dict[str, Any]:
    return {
        **_payload(
            report_type=report_type,
            title=report_type.replace("_", " ").title(),
            status=status,
            summary={"row_count": len(rows), "promotion_status": "BLOCKED"},
            config_hash=config_hash,
            rows=rows,
        ),
        "dynamic_promotion": {"final_status": "BLOCKED"},
    }


def _blocked_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "error_count": data_gate.get("error_count"),
            "blocked_reason": "validate_data_cache_failed",
        },
        data_quality=data_gate,
        blockers=["validate_data_cache_failed"],
    )


def _load_config(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"expanded universe config must be a mapping: {path}")
    return dict(raw)


def _assets_from_config(config: Mapping[str, Any]) -> list[str]:
    universe = _mapping(config.get("asset_universe"))
    assets: list[str] = []
    for key in ("risk_asset", "cash_defensive_asset", "leveraged_growth_asset"):
        assets.extend(str(value) for value in universe.get(key, []) if str(value))
    return list(dict.fromkeys(assets))


def _grid_step(config: Mapping[str, Any], level: str) -> float:
    return _float(_mapping(_mapping(config.get("grid_levels")).get(level)).get("step"))


def _data_quality_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config: Mapping[str, Any],
    as_of_date: date | None,
    expected_tickers: list[str],
) -> dict[str, Any]:
    resolved_as_of = _coerce_date(as_of_date or _max_price_date(prices_path))
    expected_rates = [
        str(value)
        for value in _mapping(config.get("data_quality")).get("expected_rate_series", [])
    ]
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_tickers,
        expected_rate_series=expected_rates,
        quality_config=load_data_quality(),
        as_of=resolved_as_of,
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )
    return _data_quality_payload(report, prices_path, rates_path, marketstack_prices_path)


def _data_quality_payload(
    report: DataQualityReport,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path,
) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "price_path": str(prices_path),
        "rates_path": str(rates_path),
        "secondary_prices_path": str(marketstack_prices_path),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
        "issues": [
            {
                "severity": str(issue.severity),
                "code": issue.code,
                "message": issue.message,
                "rows": issue.rows,
                "sample": issue.sample,
                "source": issue.source,
            }
            for issue in report.issues
        ],
    }


def _max_price_date(path: Path) -> date:
    if not path.exists():
        return date.today()
    frame = pd.read_csv(path, usecols=["date"])
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return date.today()
    return pd.Timestamp(dates.max()).date()


def _coerce_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _load_price_matrix(path: Path, tickers: list[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    missing_columns = {"date", "ticker", "adj_close"} - set(frame.columns)
    if missing_columns:
        raise ValueError(f"price cache missing columns: {sorted(missing_columns)}")
    frame = frame.loc[frame["ticker"].astype(str).isin(tickers)].copy()
    pivot = frame.pivot_table(index="date", columns="ticker", values="adj_close", aggfunc="last")
    pivot = pivot.sort_index()
    missing_tickers = [ticker for ticker in tickers if ticker not in pivot.columns]
    if missing_tickers:
        raise ValueError(f"price cache missing required tickers: {missing_tickers}")
    return pivot.reindex(columns=tickers).ffill()


def _slice_prices(prices: pd.DataFrame, *, start_date: date, end_date: date | None) -> pd.DataFrame:
    frame = prices.loc[prices.index.date >= start_date].copy()
    if end_date is not None:
        frame = frame.loc[frame.index.date <= end_date].copy()
    if len(frame.index) < 3:
        raise ValueError("expanded universe backtest requires at least three price rows")
    return frame


def _simplex_weights(assets: Sequence[str], step: float) -> list[dict[str, float]]:
    if step <= 0.0 or step > 1.0:
        raise ValueError("--step must be in (0, 1]")
    units = round(1.0 / step)
    if not math.isclose(units * step, 1.0, abs_tol=1e-9):
        raise ValueError("--step must evenly divide 1.0")
    names = list(assets)
    results: list[dict[str, float]] = []

    def build(prefix: list[int], remaining: int, depth: int) -> None:
        if depth == len(names) - 1:
            counts = [*prefix, remaining]
            weights = {
                name: round(count * step, GRID_ROUND_DIGITS)
                for name, count in zip(names, counts, strict=True)
            }
            results.append(weights)
            return
        for count in range(remaining + 1):
            build([*prefix, count], remaining - count, depth + 1)

    build([], units, 0)
    return results


def _constant_target_frame(
    index: pd.DatetimeIndex,
    columns: pd.Index,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    frame = pd.DataFrame(0.0, index=index, columns=columns)
    for asset, weight in weights.items():
        if asset in frame.columns:
            frame[asset] = _float(weight)
    return frame


def _simulate_rebalanced_portfolio(
    prices: pd.DataFrame,
    target_weights: pd.DataFrame,
    *,
    rebalance: str,
    transaction_cost_bps: float,
) -> dict[str, Any]:
    returns = prices.pct_change().fillna(0.0)
    targets = (
        target_weights.reindex(index=prices.index, columns=prices.columns)
        .ffill()
        .fillna(0.0)
    )
    markers = _rebalance_markers(targets, rebalance)
    holdings = pd.Series(0.0, index=prices.columns, dtype=float)
    prev_equity = 1.0
    daily_returns: list[float] = []
    equity_rows: list[float] = []
    turnover_rows: list[float] = []
    cost_rows: list[float] = []
    applied_rows: list[dict[str, float]] = []
    contribution_rows: list[dict[str, float]] = []
    cost_rate = transaction_cost_bps / 10000.0
    for timestamp in prices.index:
        target = targets.loc[timestamp].astype(float)
        total_before = float(holdings.sum())
        current_weights = (
            holdings / total_before
            if total_before > 0.0
            else pd.Series(0.0, index=prices.columns, dtype=float)
        )
        turnover = 0.0
        cost = 0.0
        if bool(markers.loc[timestamp]):
            base_value = total_before if total_before > 0.0 else prev_equity
            turnover = float((current_weights - target).abs().sum())
            if total_before <= 0.0:
                turnover = float(target.abs().sum())
            cost = base_value * turnover * cost_rate
            base_value = max(base_value - cost, 0.0)
            holdings = target * base_value
        total_for_return = float(holdings.sum())
        pre_return_weights = (
            holdings / total_for_return
            if total_for_return > 0.0
            else pd.Series(0.0, index=prices.columns, dtype=float)
        )
        day_contrib = pre_return_weights * returns.loc[timestamp].astype(float)
        holdings = holdings * (1.0 + returns.loc[timestamp].astype(float))
        equity = float(holdings.sum())
        daily_return = equity / prev_equity - 1.0 if prev_equity > 0.0 else 0.0
        daily_returns.append(float(daily_return))
        equity_rows.append(equity)
        turnover_rows.append(turnover)
        cost_rows.append(cost)
        applied_rows.append({str(key): float(value) for key, value in pre_return_weights.items()})
        contribution_rows.append({str(key): float(value) for key, value in day_contrib.items()})
        prev_equity = equity if equity > 0.0 else prev_equity
    index = prices.index
    return {
        "daily_returns": pd.Series(daily_returns, index=index),
        "equity": pd.Series(equity_rows, index=index),
        "turnover": pd.Series(turnover_rows, index=index),
        "cost": pd.Series(cost_rows, index=index),
        "applied_weights": pd.DataFrame(applied_rows, index=index).reindex(columns=prices.columns),
        "asset_contributions": pd.DataFrame(contribution_rows, index=index).reindex(
            columns=prices.columns
        ),
    }


def _rebalance_markers(targets: pd.DataFrame, rebalance: str) -> pd.Series:
    if rebalance == "daily":
        return pd.Series(True, index=targets.index)
    if rebalance == "quarterly":
        period = targets.index.to_period("Q").to_series(index=targets.index)
    else:
        period = targets.index.to_period("M").to_series(index=targets.index)
    markers = period != period.shift(1)
    if not markers.empty:
        markers.iloc[0] = True
    return markers.fillna(False)


def _metrics_row(
    *,
    strategy_id: str,
    candidate_family: str,
    weights: Mapping[str, float],
    sim: Mapping[str, Any],
    annualization: int,
    selection_rank: int,
    prefix: str = "",
) -> dict[str, Any]:
    returns = pd.to_numeric(sim["daily_returns"], errors="coerce").fillna(0.0)
    equity = pd.to_numeric(sim["equity"], errors="coerce").fillna(1.0)
    turnover = pd.to_numeric(sim["turnover"], errors="coerce").fillna(0.0)
    cost = pd.to_numeric(sim["cost"], errors="coerce").fillna(0.0)
    drawdown = equity / equity.cummax() - 1.0
    annual_return = _annual_return(equity, len(returns), annualization)
    volatility_daily = float(returns.std(ddof=0))
    annual_vol = volatility_daily * math.sqrt(float(annualization))
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0
    sharpe = _ratio(annual_return, annual_vol)
    calmar = _ratio(annual_return, abs(max_dd))
    weight_dict = {
        asset: round(_float(weight), GRID_ROUND_DIGITS)
        for asset, weight in weights.items()
    }
    qqq = _float(weight_dict.get("QQQ"))
    sgov = _float(weight_dict.get("SGOV"))
    tqqq = _float(weight_dict.get("TQQQ"))
    exposure = qqq + 3.0 * tqqq
    base = {
        "strategy_id": strategy_id,
        "candidate_family": candidate_family,
        "selection_rank": selection_rank,
        "qqq_weight": round(qqq, GRID_ROUND_DIGITS),
        "sgov_weight": round(sgov, GRID_ROUND_DIGITS),
        "tqqq_weight": round(tqqq, GRID_ROUND_DIGITS),
        "qqq_equivalent_exposure": round(exposure, GRID_ROUND_DIGITS),
        "cash_defensive_ratio": round(sgov, GRID_ROUND_DIGITS),
        "leveraged_asset_ratio": round(tqqq, GRID_ROUND_DIGITS),
        "max_single_asset_weight": round(
            max((abs(value) for value in weight_dict.values()), default=0.0),
            GRID_ROUND_DIGITS,
        ),
        "risk_bucket": "",
    }
    metrics = {
        f"{prefix}annual_return": round(annual_return, GRID_ROUND_DIGITS),
        f"{prefix}terminal_value": round(float(equity.iloc[-1]), GRID_ROUND_DIGITS),
        f"{prefix}volatility_daily": round(volatility_daily, GRID_ROUND_DIGITS),
        f"{prefix}annual_volatility": round(annual_vol, GRID_ROUND_DIGITS),
        f"{prefix}max_drawdown_daily_equity": round(max_dd, GRID_ROUND_DIGITS),
        f"{prefix}sharpe_daily_zero_rf": round(sharpe, GRID_ROUND_DIGITS),
        f"{prefix}calmar_daily_equity_dd": round(calmar, GRID_ROUND_DIGITS),
        f"{prefix}worst_1d_return": round(float(returns.min()), GRID_ROUND_DIGITS),
        f"{prefix}worst_5d_return": round(_worst_window_return(returns, 5), GRID_ROUND_DIGITS),
        f"{prefix}worst_20d_return": round(_worst_window_return(returns, 20), GRID_ROUND_DIGITS),
        f"{prefix}turnover": round(float(turnover.sum()), GRID_ROUND_DIGITS),
        f"{prefix}cost": round(float(cost.sum()), GRID_ROUND_DIGITS),
        f"{prefix}net_annual_return": round(annual_return, GRID_ROUND_DIGITS),
    }
    return {**base, **metrics}


def _frontier_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frontier_specs = {
        "return_vs_drawdown_frontier": ("annual_return", "max_drawdown_daily_equity"),
        "return_vs_volatility_frontier": ("annual_return", "annual_volatility"),
        "sharpe_frontier": ("sharpe_daily_zero_rf", "max_drawdown_daily_equity"),
        "calmar_frontier": ("calmar_daily_equity_dd", "annual_volatility"),
        "net_of_cost_frontier": ("net_annual_return", "turnover"),
        "stress_adjusted_frontier": ("annual_return", "worst_20d_return"),
    }
    output: list[dict[str, Any]] = []
    for frontier_type, (reward_key, risk_key) in frontier_specs.items():
        for row in _non_dominated(rows, reward_key=reward_key, risk_key=risk_key):
            output.append({**row, "frontier_type": frontier_type})
    return output


def _non_dominated(
    rows: list[dict[str, Any]],
    *,
    reward_key: str,
    risk_key: str,
) -> list[dict[str, Any]]:
    frontier = []
    for row in rows:
        dominated = False
        row_reward = _float(row.get(reward_key))
        row_risk = _risk_value(row.get(risk_key), risk_key)
        for other in rows:
            if other is row:
                continue
            other_reward = _float(other.get(reward_key))
            other_risk = _risk_value(other.get(risk_key), risk_key)
            better_or_equal = other_reward >= row_reward and other_risk <= row_risk
            strictly_better = other_reward > row_reward or other_risk < row_risk
            if better_or_equal and strictly_better:
                dominated = True
                break
        if not dominated:
            frontier.append(row)
    return sorted(frontier, key=lambda item: _float(item.get(reward_key)), reverse=True)


def _risk_value(value: object, key: str) -> float:
    number = _float(value)
    if "drawdown" in key or "worst" in key:
        return abs(number)
    return number


def _risk_bucket_summary(
    rows: list[dict[str, Any]],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    buckets = _records(config.get("risk_buckets"))
    enriched = []
    for row in rows:
        bucket = _bucket_for_exposure(_float(row.get("qqq_equivalent_exposure")), buckets)
        row["risk_bucket"] = bucket
        enriched.append(row)
    summary = []
    for bucket in buckets:
        bucket_id = str(bucket["bucket_id"])
        selected = [row for row in enriched if row["risk_bucket"] == bucket_id]
        if not selected:
            summary.append({"risk_bucket": bucket_id, "combo_count": 0})
            continue
        top_return = max(selected, key=lambda row: _float(row.get("annual_return")))
        top_sharpe = max(selected, key=lambda row: _float(row.get("sharpe_daily_zero_rf")))
        low_dd = min(selected, key=lambda row: abs(_float(row.get("max_drawdown_daily_equity"))))
        summary.append(
            {
                "risk_bucket": bucket_id,
                "combo_count": len(selected),
                "highest_return_strategy_id": top_return["strategy_id"],
                "highest_return": top_return["annual_return"],
                "highest_sharpe_strategy_id": top_sharpe["strategy_id"],
                "highest_sharpe": top_sharpe["sharpe_daily_zero_rf"],
                "lowest_drawdown_strategy_id": low_dd["strategy_id"],
                "lowest_abs_drawdown": abs(_float(low_dd.get("max_drawdown_daily_equity"))),
            }
        )
    return summary


def _bucket_for_exposure(exposure: float, buckets: Sequence[Mapping[str, Any]]) -> str:
    for bucket in buckets:
        if exposure >= _float(bucket.get("min_qqq_equivalent_exposure")) and exposure < _float(
            bucket.get("max_qqq_equivalent_exposure")
        ):
            return str(bucket.get("bucket_id"))
    return str(buckets[-1].get("bucket_id")) if buckets else "unbucketed"


def _trend_state_series(prices: pd.DataFrame, config: Mapping[str, Any]) -> pd.Series:
    policy = _mapping(config.get("trend_state_policy"))
    qqq = prices["QQQ"]
    returns = qqq.pct_change().fillna(0.0)
    short_window = _int(policy.get("short_ma_window"))
    long_window = _int(policy.get("long_ma_window"))
    realized_vol_window = _int(policy.get("realized_vol_window"))
    vol_quantile_window = _int(policy.get("vol_quantile_window"))
    drawdown_window = _int(policy.get("drawdown_window"))
    short_ma = qqq.rolling(short_window).mean().shift(1)
    long_ma = qqq.rolling(long_window).mean().shift(1)
    vol = returns.rolling(realized_vol_window).std().shift(1)
    high = qqq.rolling(drawdown_window).max().shift(1)
    drawdown = qqq.shift(1) / high - 1.0
    min_quantile_periods = max(realized_vol_window, vol_quantile_window // 4)
    high_vol_threshold = vol.rolling(
        vol_quantile_window, min_periods=min_quantile_periods
    ).quantile(_float(policy.get("high_vol_quantile")))
    low_vol_threshold = vol.rolling(
        vol_quantile_window, min_periods=min_quantile_periods
    ).quantile(_float(policy.get("low_vol_quantile")))
    high_vol = vol >= high_vol_threshold
    low_vol = vol <= low_vol_threshold
    states = pd.Series("neutral", index=prices.index, dtype=object)
    states[
        (drawdown <= _float(policy.get("risk_off_drawdown_threshold")))
        | (qqq.shift(1) < long_ma)
    ] = "risk_off"
    states[
        (states == "neutral")
        & (
            (drawdown <= _float(policy.get("defensive_drawdown_threshold")))
            | high_vol.fillna(False)
        )
    ] = "defensive"
    states[
        (states == "neutral")
        & (qqq.shift(1) >= short_ma)
        & (short_ma >= long_ma)
        & low_vol.fillna(False)
        & (drawdown >= _float(policy.get("constructive_drawdown_floor")))
    ] = "risk_on"
    states[
        (states == "neutral")
        & (qqq.shift(1) >= long_ma)
        & (drawdown >= _float(policy.get("defensive_drawdown_threshold")))
    ] = "constructive"
    return states.fillna("neutral")


def _actual_path_strategy_targets(
    *,
    prices: pd.DataFrame,
    states: pd.Series,
    static_grid_root: Path,
    candidates_path: Path,
    config: Mapping[str, Any],
) -> dict[str, pd.DataFrame]:
    strategies: dict[str, pd.DataFrame] = {}
    limited = _mapping(config.get("limited_adjustment_baseline"))
    strategies["limited_adjustment"] = _state_target_frame(prices, states, limited)

    metrics_path = static_grid_root / "static_simplex_grid_metrics.csv"
    if metrics_path.exists():
        static_count = _int(
            _mapping(config.get("actual_path_rebacktest")).get(
                "best_static_frontier_candidate_count"
            ),
            default=5,
        )
        static = pd.read_csv(metrics_path).sort_values(
            "calmar_daily_equity_dd", ascending=False
        )
        for _, row in static.head(static_count).iterrows():
            weights = _weights_from_row(row.to_dict())
            strategies[f"static_{row['strategy_id']}"] = _constant_target_frame(
                prices.index, prices.columns, weights
            )

    candidates = _read_json_or_empty(candidates_path)
    for candidate in _records(candidates.get("candidates")):
        strategy_id = str(candidate.get("strategy_id"))
        portfolios = {
            str(state): _mapping(weights)
            for state, weights in _mapping(candidate.get("state_portfolios")).items()
        }
        if strategy_id and portfolios:
            strategies[strategy_id] = _state_target_frame(prices, states, portfolios)
    return strategies


def _state_target_frame(
    prices: pd.DataFrame,
    states: pd.Series,
    state_weights: Mapping[str, Mapping[str, float]],
) -> pd.DataFrame:
    frame = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    for timestamp in frame.index:
        weights = _mapping(state_weights.get(str(states.loc[timestamp])))
        for asset in frame.columns:
            frame.loc[timestamp, asset] = _float(weights.get(asset))
    return frame.ffill().fillna(0.0)


def _target_actual_gap_row(
    strategy_id: str,
    actual_metric: Mapping[str, Any],
    target_metric: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "actual_path_annual_return": actual_metric.get("actual_path_annual_return"),
        "target_path_annual_return": target_metric.get("target_path_annual_return"),
        "annual_return_gap": round(
            _float(target_metric.get("target_path_annual_return"))
            - _float(actual_metric.get("actual_path_annual_return")),
            GRID_ROUND_DIGITS,
        ),
        "actual_path_max_drawdown": actual_metric.get("actual_path_max_drawdown_daily_equity"),
        "target_path_max_drawdown": target_metric.get("target_path_max_drawdown_daily_equity"),
        "execution_lag_cost": round(
            _float(target_metric.get("target_path_net_annual_return"))
            - _float(actual_metric.get("actual_path_net_annual_return")),
            GRID_ROUND_DIGITS,
        ),
        "signal_staleness_cost": 0.0,
        "target_path_role": "diagnostic_only",
        "promotion_input_allowed": False,
    }


def _tqqq_risk_row(strategy_id: str, sim: Mapping[str, Any]) -> dict[str, Any]:
    weights = pd.DataFrame(sim["applied_weights"]).fillna(0.0)
    returns = pd.Series(sim["daily_returns"]).fillna(0.0)
    contributions = pd.DataFrame(sim["asset_contributions"]).fillna(0.0)
    equity = pd.Series(sim["equity"]).fillna(1.0)
    drawdown = equity / equity.cummax() - 1.0
    tqqq = weights["TQQQ"] if "TQQQ" in weights else pd.Series(0.0, index=weights.index)
    exposure = weights.get("QQQ", 0.0) + 3.0 * tqqq
    trough = drawdown.idxmin() if not drawdown.empty else None
    peak = equity.loc[:trough].idxmax() if trough is not None else None
    dd_slice = (
        contributions.loc[peak:trough]
        if peak is not None and trough is not None
        else contributions
    )
    total_dd_contrib = float(dd_slice.sum(axis=1).sum()) if not dd_slice.empty else 0.0
    tqqq_dd_contrib = float(dd_slice.get("TQQQ", pd.Series(dtype=float)).sum())
    tqqq_return_contrib = float(contributions.get("TQQQ", pd.Series(dtype=float)).sum())
    total_return_contrib = float(contributions.sum(axis=1).sum())
    return {
        "strategy_id": strategy_id,
        "tqqq_max_weight": round(float(tqqq.max()) if len(tqqq) else 0.0, GRID_ROUND_DIGITS),
        "tqqq_avg_weight": round(float(tqqq.mean()) if len(tqqq) else 0.0, GRID_ROUND_DIGITS),
        "tqqq_days_positive": int((tqqq > 0.0).sum()),
        "qqq_equivalent_exposure_max": round(float(pd.Series(exposure).max()), GRID_ROUND_DIGITS),
        "qqq_equivalent_exposure_avg": round(float(pd.Series(exposure).mean()), GRID_ROUND_DIGITS),
        "worst_1d_return": round(float(returns.min()), GRID_ROUND_DIGITS),
        "worst_5d_return": round(_worst_window_return(returns, 5), GRID_ROUND_DIGITS),
        "worst_20d_return": round(_worst_window_return(returns, 20), GRID_ROUND_DIGITS),
        "max_drawdown_daily_equity": round(float(drawdown.min()), GRID_ROUND_DIGITS),
        "tqqq_contribution_to_drawdown": round(
            _ratio(tqqq_dd_contrib, total_dd_contrib), GRID_ROUND_DIGITS
        ),
        "tqqq_contribution_to_return": round(
            _ratio(tqqq_return_contrib, total_return_contrib), GRID_ROUND_DIGITS
        ),
        "promotion_gate_status": "BLOCKED",
    }


def _write_actual_path_positions(
    path: Path,
    *,
    strategy_id: str,
    states: pd.Series,
    target_frame: pd.DataFrame,
    actual_frame: pd.DataFrame,
    sim: Mapping[str, Any],
) -> None:
    rows = []
    equity = pd.Series(sim["equity"]).fillna(1.0)
    turnover = pd.Series(sim["turnover"]).fillna(0.0)
    for idx, timestamp in enumerate(actual_frame.index):
        previous = actual_frame.index[max(0, idx - 1)]
        row: dict[str, Any] = {
            "date": timestamp.date().isoformat(),
            "strategy_id": strategy_id,
            "trend_state": str(states.loc[timestamp]),
            "known_at": previous.date().isoformat(),
            "decision_at": previous.date().isoformat(),
            "effective_at": timestamp.date().isoformat(),
            "actual_execution_date": timestamp.date().isoformat(),
            "target_vs_actual_gap": round(
                float((target_frame.loc[timestamp] - actual_frame.loc[timestamp]).abs().sum()),
                GRID_ROUND_DIGITS,
            ),
            "turnover": round(float(turnover.loc[timestamp]), GRID_ROUND_DIGITS),
            "equity": round(float(equity.loc[timestamp]), GRID_ROUND_DIGITS),
            "execution_lag_cost": 0.0,
            "signal_staleness_cost": 0.0,
            "pending_plan_supersede": False,
            "event_override": False,
        }
        for asset in actual_frame.columns:
            row[f"target_weight_{asset}"] = round(float(target_frame.loc[timestamp, asset]), 6)
            row[f"actual_weight_{asset}"] = round(float(actual_frame.loc[timestamp, asset]), 6)
        rows.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _same_risk_rows(
    actual_metrics: pd.DataFrame,
    static_metrics: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    static = static_metrics.copy()
    for _, dynamic in actual_metrics.iterrows():
        dynamic_exposure = _float(dynamic.get("qqq_equivalent_exposure"))
        static["exposure_distance"] = (
            static["qqq_equivalent_exposure"].astype(float) - dynamic_exposure
        ).abs()
        comparable = static.sort_values(
            ["exposure_distance", "calmar_daily_equity_dd"], ascending=[True, False]
        ).iloc[0]
        rows.append(
            {
                "strategy_id": str(dynamic["strategy_id"]),
                "candidate_family": str(dynamic.get("candidate_family")),
                "actual_path_annual_return": _float(dynamic.get("actual_path_annual_return")),
                "actual_path_max_drawdown": _float(
                    dynamic.get("actual_path_max_drawdown_daily_equity")
                ),
                "actual_path_sharpe": _float(dynamic.get("actual_path_sharpe_daily_zero_rf")),
                "actual_path_calmar": _float(dynamic.get("actual_path_calmar_daily_equity_dd")),
                "qqq_equivalent_exposure": dynamic_exposure,
                "comparable_static_strategy_id": str(comparable["strategy_id"]),
                "comparable_static_annual_return": _float(comparable.get("annual_return")),
                "comparable_static_max_drawdown": _float(
                    comparable.get("max_drawdown_daily_equity")
                ),
                "comparable_static_sharpe": _float(comparable.get("sharpe_daily_zero_rf")),
                "comparable_static_calmar": _float(comparable.get("calmar_daily_equity_dd")),
                "annual_return_edge": round(
                    _float(dynamic.get("actual_path_annual_return"))
                    - _float(comparable.get("annual_return")),
                    GRID_ROUND_DIGITS,
                ),
                "drawdown_delta": round(
                    _float(dynamic.get("actual_path_max_drawdown_daily_equity"))
                    - _float(comparable.get("max_drawdown_daily_equity")),
                    GRID_ROUND_DIGITS,
                ),
                "sharpe_edge": round(
                    _float(dynamic.get("actual_path_sharpe_daily_zero_rf"))
                    - _float(comparable.get("sharpe_daily_zero_rf")),
                    GRID_ROUND_DIGITS,
                ),
                "calmar_edge": round(
                    _float(dynamic.get("actual_path_calmar_daily_equity_dd"))
                    - _float(comparable.get("calmar_daily_equity_dd")),
                    GRID_ROUND_DIGITS,
                ),
            }
        )
    return rows


def _survival_rows(rows: list[dict[str, Any]], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    allowed = set(str(value) for value in config.get("survival_verdicts", []))
    policy = _mapping(config.get("survival_policy"))
    positive_edge_floor = _float(policy.get("positive_edge_floor"))
    drawdown_tolerance = _float(policy.get("max_drawdown_worse_tolerance_abs"))
    beta_only_exposure_floor = _float(policy.get("tqqq_beta_only_exposure_floor"))
    output = []
    for row in rows:
        verdict = "INSUFFICIENT_EVIDENCE"
        if row["annual_return_edge"] <= positive_edge_floor:
            verdict = "STATIC_FRONTIER_DOMINATES"
        elif row["drawdown_delta"] < -drawdown_tolerance:
            verdict = "RISK_TOO_HIGH"
        elif (
            row["qqq_equivalent_exposure"] > beta_only_exposure_floor
            and row["annual_return_edge"] > positive_edge_floor
        ):
            verdict = "TQQQ_BETA_ONLY"
        elif row["sharpe_edge"] > 0.0 or row["calmar_edge"] > 0.0:
            verdict = "SURVIVES_EXPANDED_UNIVERSE"
        elif row["annual_return_edge"] > positive_edge_floor:
            verdict = "NO_MATERIAL_IMPROVEMENT"
        if verdict not in allowed:
            verdict = "INSUFFICIENT_EVIDENCE"
        output.append(
            {
                **row,
                "verdict": verdict,
                "promotion_gate_status": "BLOCKED",
                "target_path_metrics_role": "diagnostic_only",
            }
        )
    return output


def _walk_forward_review_payload(actual_metrics: pd.DataFrame, config_path: Path) -> dict[str, Any]:
    rows = []
    for _, row in actual_metrics.iterrows():
        rows.append(
            {
                "strategy_id": str(row["strategy_id"]),
                "candidate_family_id": str(row.get("candidate_family")),
                "number_of_searched_candidates": int(len(actual_metrics)),
                "selection_rank": int(row.get("selection_rank", 0)),
                "in_sample_metric": _float(row.get("actual_path_calmar_daily_equity_dd")),
                "validation_metric": None,
                "holdout_metric": None,
                "stability_score": 0.0,
                "status": "WALK_FORWARD_BLOCKED_PENDING_SPLIT_EVIDENCE",
                "promotion_gate_status": "BLOCKED",
            }
        )
    return _review_payload(
        report_type="expanded_universe_walk_forward_matrix",
        status="WALK_FORWARD_REVIEW_READY_WITH_BLOCKERS",
        rows=rows,
        config_hash=_file_sha256(config_path),
    )


def _net_cost_review_payload(actual_metrics: pd.DataFrame, config_path: Path) -> dict[str, Any]:
    rows = []
    for _, row in actual_metrics.iterrows():
        rows.append(
            {
                "strategy_id": str(row["strategy_id"]),
                "actual_path_turnover": _float(row.get("actual_path_turnover")),
                "actual_path_cost": _float(row.get("actual_path_cost")),
                "net_annual_return": _float(row.get("actual_path_net_annual_return")),
                "status": "NET_OF_COST_REVIEW_READY",
                "promotion_gate_status": "BLOCKED",
            }
        )
    return _review_payload(
        report_type="expanded_universe_net_of_cost_review",
        status="NET_OF_COST_REVIEW_READY",
        rows=rows,
        config_hash=_file_sha256(config_path),
    )


def _stress_review_payload(
    tqqq_risk: pd.DataFrame,
    config_path: Path,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    stress_policy = _mapping((config or _load_config(config_path)).get("stress_policy"))
    max_exposure = _float(
        stress_policy.get("max_qqq_equivalent_exposure_for_promotion"),
        default=1.5,
    )
    max_tqqq_weight = _float(
        stress_policy.get("max_tqqq_weight_for_promotion"),
        default=0.2,
    )
    rows = []
    for _, row in tqqq_risk.iterrows():
        exposure = _float(row.get("qqq_equivalent_exposure_max"))
        tqqq_weight = _float(row.get("tqqq_max_weight"))
        status = "STRESS_REVIEW_READY"
        if exposure > max_exposure or tqqq_weight > max_tqqq_weight:
            status = "STRESS_RISK_BLOCKS_PROMOTION"
        rows.append(
            {
                **{str(key): _json_scalar(value) for key, value in row.to_dict().items()},
                "status": status,
                "promotion_gate_status": "BLOCKED",
            }
        )
    return _review_payload(
        report_type="expanded_universe_stress_risk_review",
        status="STRESS_RISK_REVIEW_READY_WITH_BLOCKERS",
        rows=rows,
        config_hash=_file_sha256(config_path),
    )


def _candidate_failure_matrix_rows(
    *,
    actual_metrics: pd.DataFrame,
    same_risk_rows: list[dict[str, Any]],
    survival_rows: list[dict[str, Any]],
    walk_forward_rows: list[dict[str, Any]],
    net_cost_rows: list[dict[str, Any]],
    stress_rows: list[dict[str, Any]],
    tqqq_risk_rows: list[dict[str, Any]],
    candidates: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    policy = _mapping(config.get("failure_matrix_policy"))
    survival_policy = _mapping(config.get("survival_policy"))
    return_edge_floor = _float(policy.get("same_risk_return_edge_floor"))
    net_cost_edge_floor = _float(policy.get("net_of_cost_edge_floor"))
    beta_only_exposure_floor = _float(survival_policy.get("tqqq_beta_only_exposure_floor"))
    walk_forward_blockers = {
        str(value) for value in policy.get("walk_forward_blocking_statuses", [])
    }
    next_actions = _mapping(policy.get("next_action_by_primary_failure"))
    same_risk_by_id = {str(row["strategy_id"]): row for row in same_risk_rows}
    survival_by_id = {str(row["strategy_id"]): row for row in survival_rows}
    walk_forward_by_id = {str(row["strategy_id"]): row for row in walk_forward_rows}
    net_cost_by_id = {str(row["strategy_id"]): row for row in net_cost_rows}
    stress_by_id = {str(row["strategy_id"]): row for row in stress_rows}
    tqqq_by_id = {str(row["strategy_id"]): row for row in tqqq_risk_rows}
    weights_lookup = _weights_by_state_lookup(actual_metrics, candidates, config)

    rows: list[dict[str, Any]] = []
    for _, actual in actual_metrics.iterrows():
        candidate_id = str(actual["strategy_id"])
        same_risk = same_risk_by_id[candidate_id]
        survival = survival_by_id[candidate_id]
        walk_forward = walk_forward_by_id.get(candidate_id, {})
        net_cost = net_cost_by_id.get(candidate_id, {})
        stress = stress_by_id.get(candidate_id, {})
        tqqq = tqqq_by_id.get(candidate_id, {})
        annual_return_edge = _float(same_risk.get("annual_return_edge"))
        net_return_edge = round(
            _float(net_cost.get("net_annual_return"))
            - _float(same_risk.get("comparable_static_annual_return")),
            GRID_ROUND_DIGITS,
        )
        verdict = str(survival.get("verdict"))
        walk_status = str(walk_forward.get("status", "UNKNOWN"))
        stress_status = str(stress.get("status", "UNKNOWN"))
        same_risk_not_advantaged = annual_return_edge <= return_edge_floor
        tqqq_beta_only = (
            verdict == "TQQQ_BETA_ONLY"
            or (
                _float(tqqq.get("tqqq_max_weight")) > 0.0
                and _float(same_risk.get("qqq_equivalent_exposure"))
                > beta_only_exposure_floor
                and annual_return_edge > return_edge_floor
            )
        )
        walk_forward_failed = walk_status in walk_forward_blockers or "BLOCKED" in walk_status
        stress_risk_too_high = stress_status == "STRESS_RISK_BLOCKS_PROMOTION"
        net_of_cost_failed = net_return_edge <= net_cost_edge_floor
        dominated_by_static = verdict == "STATIC_FRONTIER_DOMINATES" or same_risk_not_advantaged
        failure_codes = _candidate_failure_codes(
            verdict=verdict,
            same_risk_baseline=str(same_risk.get("comparable_static_strategy_id")),
            same_risk_not_advantaged=same_risk_not_advantaged,
            tqqq_beta_only=tqqq_beta_only,
            walk_forward_failed=walk_forward_failed,
            walk_forward_status=walk_status,
            stress_risk_too_high=stress_risk_too_high,
            stress_status=stress_status,
            net_of_cost_failed=net_of_cost_failed,
            net_return_edge=net_return_edge,
        )
        weights_by_state = weights_lookup.get(candidate_id, {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "strategy_id": candidate_id,
                "candidate_family": str(actual.get("candidate_family")),
                "selection_rank": _int(actual.get("selection_rank")),
                "verdict": verdict,
                "dominated_by_static_frontier": dominated_by_static,
                "dominating_static_frontier_baseline": (
                    str(same_risk.get("comparable_static_strategy_id"))
                    if dominated_by_static
                    else ""
                ),
                "tqqq_beta_only": tqqq_beta_only,
                "same_risk_not_advantaged": same_risk_not_advantaged,
                "walk_forward_failed": walk_forward_failed,
                "walk_forward_status": walk_status,
                "stress_risk_too_high": stress_risk_too_high,
                "stress_status": stress_status,
                "net_of_cost_failed": net_of_cost_failed,
                "weights_by_state": weights_by_state,
                "tqqq_weight_profile": _tqqq_weight_profile(tqqq, weights_by_state),
                "qqq_equivalent_exposure": _float(same_risk.get("qqq_equivalent_exposure")),
                "actual_return": _float(same_risk.get("actual_path_annual_return")),
                "max_dd": _float(same_risk.get("actual_path_max_drawdown")),
                "sharpe": _float(same_risk.get("actual_path_sharpe")),
                "calmar": _float(same_risk.get("actual_path_calmar")),
                "same_risk_baseline": str(same_risk.get("comparable_static_strategy_id")),
                "same_risk_baseline_metrics": {
                    "annual_return": _float(same_risk.get("comparable_static_annual_return")),
                    "max_drawdown": _float(same_risk.get("comparable_static_max_drawdown")),
                    "sharpe": _float(same_risk.get("comparable_static_sharpe")),
                    "calmar": _float(same_risk.get("comparable_static_calmar")),
                },
                "delta_vs_same_risk_baseline": {
                    "annual_return_edge": annual_return_edge,
                    "drawdown_delta": _float(same_risk.get("drawdown_delta")),
                    "sharpe_edge": _float(same_risk.get("sharpe_edge")),
                    "calmar_edge": _float(same_risk.get("calmar_edge")),
                    "net_annual_return_edge": net_return_edge,
                },
                "failure_reason": " | ".join(failure_codes),
                "failure_codes": failure_codes,
                "next_action": _next_action_for_failure(
                    verdict=verdict,
                    same_risk_not_advantaged=same_risk_not_advantaged,
                    tqqq_beta_only=tqqq_beta_only,
                    stress_risk_too_high=stress_risk_too_high,
                    net_of_cost_failed=net_of_cost_failed,
                    walk_forward_failed=walk_forward_failed,
                    next_actions=next_actions,
                ),
                "promotion_gate_status": "BLOCKED",
                "target_path_metrics_role": "diagnostic_only",
            }
        )
    return rows


def _candidate_failure_codes(
    *,
    verdict: str,
    same_risk_baseline: str,
    same_risk_not_advantaged: bool,
    tqqq_beta_only: bool,
    walk_forward_failed: bool,
    walk_forward_status: str,
    stress_risk_too_high: bool,
    stress_status: str,
    net_of_cost_failed: bool,
    net_return_edge: float,
) -> list[str]:
    codes = [f"verdict={verdict}"]
    if verdict == "STATIC_FRONTIER_DOMINATES" or same_risk_not_advantaged:
        codes.append(f"static_frontier_dominates={same_risk_baseline}")
    if tqqq_beta_only:
        codes.append("tqqq_beta_only=true")
    if same_risk_not_advantaged:
        codes.append("same_risk_not_advantaged=true")
    if walk_forward_failed:
        codes.append(f"walk_forward_failed={walk_forward_status}")
    if stress_risk_too_high:
        codes.append(f"stress_risk_too_high={stress_status}")
    if net_of_cost_failed:
        codes.append(f"net_of_cost_failed=edge:{net_return_edge}")
    if verdict == "NO_MATERIAL_IMPROVEMENT":
        codes.append("no_material_improvement=true")
    return codes


def _next_action_for_failure(
    *,
    verdict: str,
    same_risk_not_advantaged: bool,
    tqqq_beta_only: bool,
    stress_risk_too_high: bool,
    net_of_cost_failed: bool,
    walk_forward_failed: bool,
    next_actions: Mapping[str, Any],
) -> str:
    if verdict == "STATIC_FRONTIER_DOMINATES":
        return str(next_actions.get("static_frontier_dominates"))
    if tqqq_beta_only:
        return str(next_actions.get("tqqq_beta_only"))
    if same_risk_not_advantaged:
        return str(next_actions.get("same_risk_not_advantaged"))
    if verdict == "NO_MATERIAL_IMPROVEMENT":
        return str(next_actions.get("no_material_improvement"))
    if stress_risk_too_high:
        return str(next_actions.get("stress_risk_too_high"))
    if net_of_cost_failed:
        return str(next_actions.get("net_of_cost_failed"))
    if walk_forward_failed:
        return str(next_actions.get("walk_forward_failed"))
    return "KEEP_RESEARCH_ONLY_OWNER_REVIEW_REQUIRED"


def _weights_by_state_lookup(
    actual_metrics: pd.DataFrame,
    candidates: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, dict[str, dict[str, float]]]:
    lookup: dict[str, dict[str, dict[str, float]]] = {
        "limited_adjustment": _normalized_state_weights(
            _mapping(config.get("limited_adjustment_baseline"))
        )
    }
    for candidate in _records(candidates.get("candidates")):
        strategy_id = str(candidate.get("strategy_id"))
        if strategy_id:
            lookup[strategy_id] = _normalized_state_weights(
                _mapping(candidate.get("state_portfolios"))
            )
    for _, row in actual_metrics.iterrows():
        strategy_id = str(row["strategy_id"])
        if strategy_id not in lookup:
            lookup[strategy_id] = {"static": _weights_from_row(row.to_dict())}
    return lookup


def _normalized_state_weights(
    state_weights: Mapping[str, Any],
) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for state, weights in state_weights.items():
        output[str(state)] = {
            asset: round(_float(weight), GRID_ROUND_DIGITS)
            for asset, weight in _mapping(weights).items()
        }
    return output


def _tqqq_weight_profile(
    tqqq_row: Mapping[str, Any],
    weights_by_state: Mapping[str, Mapping[str, float]],
) -> dict[str, Any]:
    state_weights = {
        str(state): round(_float(_mapping(weights).get("TQQQ")), GRID_ROUND_DIGITS)
        for state, weights in weights_by_state.items()
    }
    return {
        "tqqq_max_weight": _float(tqqq_row.get("tqqq_max_weight")),
        "tqqq_avg_weight": _float(tqqq_row.get("tqqq_avg_weight")),
        "tqqq_days_positive": _int(tqqq_row.get("tqqq_days_positive")),
        "state_tqqq_weights": state_weights,
    }


def _write_failure_matrix_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    csv_rows = []
    for row in rows:
        csv_rows.append(
            {
                key: (
                    json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (Mapping, list))
                    else value
                )
                for key, value in row.items()
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(csv_rows).to_csv(path, index=False)


def _annual_return(equity: pd.Series, periods: int, annualization: int) -> float:
    if equity.empty or periods <= 0:
        return 0.0
    terminal = float(equity.iloc[-1])
    if terminal <= 0.0:
        return -1.0
    return terminal ** (float(annualization) / float(periods)) - 1.0


def _worst_window_return(returns: pd.Series, window: int) -> float:
    if len(returns) < window:
        return float(returns.min()) if len(returns) else 0.0
    rolled = (1.0 + returns).rolling(window).apply(lambda values: float(values.prod() - 1.0))
    return float(rolled.min()) if not rolled.dropna().empty else 0.0


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator)
    if abs(denom) <= 1e-12:
        return 0.0
    return _float(numerator) / denom


def _static_combo_id(weights: Mapping[str, float]) -> str:
    return "simplex_" + "_".join(
        f"{asset.lower()}{int(round(_float(weight) * 1000)):04d}"
        for asset, weight in sorted(weights.items())
    )


def _weights_from_row(row: Mapping[str, Any]) -> dict[str, float]:
    return {
        "QQQ": round(_float(row.get("qqq_weight")), GRID_ROUND_DIGITS),
        "SGOV": round(_float(row.get("sgov_weight")), GRID_ROUND_DIGITS),
        "TQQQ": round(_float(row.get("tqqq_weight")), GRID_ROUND_DIGITS),
    }


def _representative_row(
    reps: pd.DataFrame,
    *,
    bucket_id: str,
    selector_id: str,
) -> dict[str, Any] | None:
    selected = reps.loc[(reps["bucket_id"] == bucket_id) & (reps["selector_id"] == selector_id)]
    if selected.empty:
        return None
    return dict(selected.iloc[0].to_dict())


def _average_weight_dict(weights: pd.DataFrame) -> dict[str, float]:
    if weights.empty:
        return {"QQQ": 0.0, "SGOV": 0.0, "TQQQ": 0.0}
    return {
        str(column): round(float(weights[column].mean()), GRID_ROUND_DIGITS)
        for column in weights.columns
    }


def _candidate_family(strategy_id: str) -> str:
    if strategy_id.startswith("static_"):
        return "static_frontier_candidate"
    if strategy_id == "limited_adjustment":
        return "limited_adjustment_baseline"
    return "expanded_state_portfolio_candidate"


def _top_records(frame: pd.DataFrame, key: str, limit: int) -> list[dict[str, Any]]:
    if key not in frame.columns or frame.empty:
        return []
    return _json_records(frame.sort_values(key, ascending=False).head(limit).to_dict("records"))


def _value_counts(frame: pd.DataFrame, key: str) -> dict[str, int]:
    if key not in frame.columns:
        return {}
    return {str(item): int(value) for item, value in frame[key].value_counts().items()}


def _date_range_from_index(
    index: pd.DatetimeIndex,
    requested_start: date,
    requested_end: date | None,
) -> dict[str, str]:
    return {
        "start": index.min().date().isoformat(),
        "end": index.max().date().isoformat(),
        "requested_start": requested_start.isoformat(),
        "requested_end": (
            requested_end.isoformat()
            if requested_end
            else index.max().date().isoformat()
        ),
        "market_regime": "ai_after_chatgpt",
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


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


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _read_yaml_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_scalar(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _json_records(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(_json_scalar(row)) for row in rows]


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return str(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "UNKNOWN"
    return result.stdout.strip() or "UNKNOWN"


def _render_scope_doc(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Expanded QQQ / SGOV / TQQQ Universe Scope",
            "",
            f"- 状态：`{payload.get('status')}`",
            "- market_regime：`ai_after_chatgpt`",
            "- default_backtest_start：`2022-12-01`",
            "- TQQQ：`research_only`，promotion universe 仍为 `BLOCKED`",
            "- production_effect：`none`",
            "- broker_action：`none`",
            "",
            "本阶段只建立 research universe、static simplex frontier、state-to-portfolio "
            "candidate search 和 actual-path evidence，不恢复 dynamic promotion。",
        ]
    ) + "\n"


def _render_tqqq_review_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# TQQQ Data Quality Blocking Review",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- data_quality_status：`{summary.get('data_quality_status')}`",
            f"- promotion_universe_status：`{payload.get('promotion_universe_status')}`",
            "- TQQQ 使用真实 adjusted close；禁止 synthetic 3x QQQ return。",
            "- dynamic promotion：`BLOCKED`",
            "",
            "进入 promotion universe 前必须完成 primary adjusted close、secondary source、"
            "corporate action、sample date 和 strategy impact review。",
        ]
    ) + "\n"


def _render_static_index(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Static Simplex Grid Index",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- grid_step：`{summary.get('grid_step')}`",
            f"- static_grid_size：`{summary.get('static_grid_size')}`",
            f"- frontier_row_count：`{summary.get('frontier_row_count')}`",
            f"- data_quality_status：`{summary.get('data_quality_status')}`",
            "- promotion_status：`BLOCKED`",
        ]
    ) + "\n"


def _render_static_frontier_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Expanded Universe Static Frontier Review",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- static_grid_size：`{summary.get('static_grid_size')}`",
        f"- frontier_row_count：`{summary.get('frontier_row_count')}`",
        f"- best_static_candidate：`{summary.get('best_static_candidate')}`",
        "- dynamic promotion：`BLOCKED`",
        "",
        "## Top Static Candidates",
        "",
    ]
    for row in _records(payload.get("top_static_candidates")):
        lines.append(
            f"- `{row.get('strategy_id')}` annual_return=`{row.get('annual_return')}` "
            f"max_dd=`{row.get('max_drawdown_daily_equity')}` "
            f"exposure=`{row.get('qqq_equivalent_exposure')}`"
        )
    return "\n".join(lines) + "\n"


def _render_actual_path_owner_pack(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Expanded Universe Actual-Path Rebacktest",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- strategy_count：`{summary.get('strategy_count')}`",
            f"- best_actual_path_strategy：`{summary.get('best_actual_path_strategy')}`",
            f"- data_quality_status：`{summary.get('data_quality_status')}`",
            "- ranking_source：`leaderboard_actual_path.csv`",
            "- target_path_metrics_role：`diagnostic_only`",
            "- dynamic_promotion：`BLOCKED`",
        ]
    ) + "\n"


def _render_table_doc(title: str, payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("rows"))
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- row_count：`{len(rows)}`",
        "- dynamic_promotion：`BLOCKED`",
        "- production_effect：`none`",
        "",
    ]
    for row in rows[:10]:
        strategy = row.get("strategy_id", row.get("report_type", "row"))
        status = row.get("status", row.get("verdict", "READY"))
        lines.append(f"- `{strategy}`：`{status}`")
    return "\n".join(lines) + "\n"


def _render_failure_matrix_doc(payload: Mapping[str, Any]) -> str:
    rows = _records(payload.get("rows"))
    lines = [
        "# Expanded Actual-Path Candidate Failure Matrix",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- candidate_count：`{len(rows)}`",
        "- market_regime：`ai_after_chatgpt`",
        "- dynamic_promotion：`BLOCKED`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "",
        "|candidate_id|verdict|same_risk_baseline|failure_reason|next_action|",
        "|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "|"
            + "|".join(
                [
                    str(row.get("candidate_id")),
                    str(row.get("verdict")),
                    str(row.get("same_risk_baseline")),
                    str(row.get("failure_reason")).replace("|", ";"),
                    str(row.get("next_action")),
                ]
            )
            + "|"
        )
    return "\n".join(lines) + "\n"


def _render_owner_review_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Expanded QQQ / SGOV / TQQQ Allocation Owner Review Pack",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- best_static_candidate：`{summary.get('best_static_candidate')}`",
        f"- best_dynamic_candidate：`{summary.get('best_dynamic_candidate')}`",
        f"- surviving_candidate_count：`{summary.get('surviving_candidate_count')}`",
        f"- TQQQ data quality：`{summary.get('tqqq_data_quality_status')}`",
        "- promotion_status：`BLOCKED`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "",
        "## Owner Questions",
        "",
        "1. 放开三资产权重后是否发现明显更优候选：见 survival matrix。",
        "2. 更优是否只是 TQQQ beta：见 TQQQ risk attribution 和 same-risk comparison。",
        "3. 是否打败静态三资产 frontier：见 same-risk baseline comparison。",
        "4. 同风险基准下是否仍有优势：见 `annual_return_edge`、`sharpe_edge`、`calmar_edge`。",
        "5. 是否值得进入 watch-only forward：本批不自动批准，需 owner review。",
        "6. dynamic promotion 为什么仍 blocked：TQQQ promotion review、walk-forward、"
        "stress 和 owner review 未通过。",
        "",
        "## Remaining Blockers",
        "",
    ]
    lines.extend(f"- `{item}`" for item in payload.get("remaining_blockers", []))
    return "\n".join(lines) + "\n"


def _render_review_doc(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            f"# {payload.get('title', payload.get('report_type'))}",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- summary：`{summary}`",
            "- production_effect：`none`",
            "- broker_action：`none`",
        ]
    ) + "\n"
