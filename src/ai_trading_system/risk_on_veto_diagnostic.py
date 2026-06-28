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

from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_2022_SLICE_MATRIX_PATH,
    DEFAULT_2023_PLUS_MATRIX_PATH,
    DEFAULT_ACTUAL_PATH_MATRIX_PATH,
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    DEFAULT_FALSE_ADD_RISK_MATRIX_PATH,
    DEFAULT_FALSE_RISK_OFF_MATRIX_PATH,
    DEFAULT_FINAL_MATRIX_PATH,
    DEFAULT_SELECTION_RESULT_PATH,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _data_quality_gate,
    _load_price_matrix,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PREDICTIONS_PATH as DEFAULT_BASELINE_COMPOSER_PATH,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PRIMARY_WINDOW_ID = "exact_three_asset_validated"
PRIMARY_WINDOW_ALIAS = "EXACT_THREE_ASSET_VALIDATED_WINDOW"
REQUESTED_START = "2021-02-22"
MARKET_REGIME = "ai_after_chatgpt"
ANCHOR_EVENT = "ChatGPT public launch"
ANCHOR_DATE = "2022-11-30"
ASSETS = ["QQQ", "SGOV", "TQQQ"]

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "risk_on_veto_diagnostic"
DEFAULT_EPISODES_PATH = DEFAULT_OUTPUT_ROOT / "risk_on_veto_episodes.csv"
DEFAULT_SUMMARY_JSON_PATH = DEFAULT_OUTPUT_ROOT / "summary.json"
DEFAULT_DIAGNOSTIC_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_diagnostic_contract.yaml"
)
DEFAULT_METRIC_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_metric_policy.yaml"
)
DEFAULT_FORWARD_LOG_SPEC_PATH = (
    PROJECT_ROOT / "config" / "research" / "risk_on_veto_forward_log.yaml"
)
DEFAULT_RISK_VETO_LABELS_PATH = (
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT / "risk_on_veto_labels_v3.csv"
)
DEFAULT_CHANNEL_V3_COMPOSER_PATH = (
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT / "channel_composer_v3_predictions.csv"
)
DEFAULT_CHANNEL_PIT_MATRIX_PATH = (
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT / "channel_pit_feature_matrix_v3.csv"
)
DEFAULT_CLOSEOUT_RECLASSIFICATION_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_v3_closeout_reclassification.yaml"
)
DEFAULT_CLOSEOUT_RECLASSIFICATION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "channel_specific_v3_closeout_reclassification.md"
)
DEFAULT_DO_NOT_ARCHIVE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "do_not_de_risk_v3_archive.yaml"
)
DEFAULT_DO_NOT_ARCHIVE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "do_not_de_risk_v3_archive_review.md"
)
DEFAULT_DIAGNOSTIC_REVIEW_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "risk_on_veto_observe_only_diagnostic.yaml"
)
DEFAULT_DIAGNOSTIC_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_observe_only_diagnostic_review.md"
)
DEFAULT_BEHAVIOR_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "risk_on_veto_2022_2023_behavior.yaml"
)
DEFAULT_BEHAVIOR_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_2022_2023_behavior_review.md"
)
DEFAULT_TRADEOFF_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "risk_on_veto_tradeoff_matrix.yaml"
)
DEFAULT_TRADEOFF_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_tradeoff_review.md"
)
DEFAULT_COMPATIBILITY_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "risk_on_veto_return_seeking_diagnostic_compatibility.yaml"
)
DEFAULT_COMPATIBILITY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "risk_on_veto_return_seeking_diagnostic_compatibility.md"
)
DEFAULT_OWNER_BRIEF_PATH = (
    PROJECT_ROOT / "docs" / "research" / "risk_on_veto_observe_only_owner_brief.md"
)

EPISODE_COLUMNS = [
    "date",
    "research_window_id",
    "veto_active",
    "veto_reasons",
    "volatility_compression_features",
    "rates_liquidity_features",
    "would_have_add_risk_reference",
    "blocked_add_risk",
    "future_1d_return",
    "future_5d_return",
    "future_10d_return",
    "future_20d_return",
    "future_max_drawdown",
    "captured_upside_lost",
    "avoided_false_add_risk_cost",
    "net_veto_benefit",
]
FORBIDDEN_ALLOCATION_FIELDS = {
    "target_weights",
    "portfolio_weights",
    "trade_action",
    "paper_shadow_action",
    "broker_action",
    "recommended_allocation",
    "target_allocation",
    "qqq_weight",
    "sgov_weight",
    "tqqq_weight",
    "QQQ",
    "SGOV",
    "TQQQ",
}
VOLATILITY_FEATURES = [
    "realized_vol_20d",
    "realized_vol_decline_20d",
    "realized_vol_decline_60d",
    "downside_vol_20d",
    "downside_vol_decline_20d",
]
RATES_LIQUIDITY_FEATURES = ["yield_curve_10y2y", "usd_trend_20d"]
SAFETY_BOUNDARY = {
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


def run_risk_on_veto_diagnostic_pack(
    *,
    diagnostic_contract_path: Path = DEFAULT_DIAGNOSTIC_CONTRACT_PATH,
    metric_policy_path: Path = DEFAULT_METRIC_POLICY_PATH,
    forward_log_spec_path: Path = DEFAULT_FORWARD_LOG_SPEC_PATH,
    risk_veto_labels_path: Path = DEFAULT_RISK_VETO_LABELS_PATH,
    channel_v3_composer_path: Path = DEFAULT_CHANNEL_V3_COMPOSER_PATH,
    channel_pit_matrix_path: Path = DEFAULT_CHANNEL_PIT_MATRIX_PATH,
    baseline_composer_path: Path = DEFAULT_BASELINE_COMPOSER_PATH,
    final_matrix_path: Path = DEFAULT_FINAL_MATRIX_PATH,
    selection_result_path: Path = DEFAULT_SELECTION_RESULT_PATH,
    false_add_risk_path: Path = DEFAULT_FALSE_ADD_RISK_MATRIX_PATH,
    false_risk_off_path: Path = DEFAULT_FALSE_RISK_OFF_MATRIX_PATH,
    slice_2022_path: Path = DEFAULT_2022_SLICE_MATRIX_PATH,
    dependence_2023_path: Path = DEFAULT_2023_PLUS_MATRIX_PATH,
    actual_path_matrix_path: Path = DEFAULT_ACTUAL_PATH_MATRIX_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    contract = _load_mapping(diagnostic_contract_path)
    metric_policy = _load_mapping(metric_policy_path)
    forward_log_spec = _load_mapping(forward_log_spec_path)
    _validate_static_contracts(contract, metric_policy, forward_log_spec)

    expanded_config = _load_mapping(expanded_config_path)
    data_quality = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=expanded_config,
        as_of_date=as_of_date,
        expected_tickers=ASSETS,
    )
    if not data_quality.get("passed"):
        raise ValueError(
            f"Cached data quality gate failed for risk-on veto diagnostic: "
            f"{data_quality.get('status')}"
        )

    prices = _load_price_matrix(prices_path, ["QQQ"])
    labels = _load_risk_veto_labels(risk_veto_labels_path)
    channel_composer = _load_channel_v3_composer(channel_v3_composer_path)
    pit_matrix = _load_channel_pit_matrix(channel_pit_matrix_path)
    baseline = _load_baseline_reference(baseline_composer_path)

    episodes = _build_episode_rows(
        labels=labels,
        channel_composer=channel_composer,
        pit_matrix=pit_matrix,
        baseline=baseline,
        prices=prices,
    )
    episodes_path = output_root / "risk_on_veto_episodes.csv"
    summary_json_path = output_root / "summary.json"
    _write_csv(episodes_path, episodes, EPISODE_COLUMNS)

    final_matrix = _load_mapping(final_matrix_path)
    selection_result = _load_mapping(selection_result_path)
    false_add_risk = _load_mapping(false_add_risk_path)
    false_risk_off = _load_mapping(false_risk_off_path)
    slice_2022 = _load_mapping(slice_2022_path)
    dependence_2023 = _load_mapping(dependence_2023_path)
    actual_path = _load_mapping(actual_path_matrix_path)

    summary = _diagnostic_summary(
        episodes=episodes,
        labels=labels,
        selection_result=selection_result,
        false_add_risk=false_add_risk,
        actual_path=actual_path,
        data_quality=data_quality,
    )
    summary_payload = _payload(
        report_type="risk_on_veto_observe_only_diagnostic",
        title="Risk-On Veto Observe-Only Diagnostic",
        status="RISK_ON_VETO_V3_OBSERVE_ONLY_DIAGNOSTIC",
        summary=summary,
    )
    summary_payload.update(
        {
            "data_quality_gate": dict(data_quality),
            "diagnostic_contract": _mapping(contract.get("risk_on_veto_diagnostic_contract")),
            "metric_policy_id": metric_policy.get("policy_id"),
            "forward_log_policy_id": forward_log_spec.get("policy_id"),
            "episode_log_columns": EPISODE_COLUMNS,
            "forbidden_allocation_fields": sorted(FORBIDDEN_ALLOCATION_FIELDS),
            "artifact_paths": {
                "episodes_csv": str(episodes_path),
                "summary_json": str(summary_json_path),
            },
        }
    )
    _write_json(summary_json_path, summary_payload)
    _write_yaml(DEFAULT_DIAGNOSTIC_REVIEW_PATH, summary_payload)
    _write_markdown(
        DEFAULT_DIAGNOSTIC_REVIEW_DOC_PATH,
        _render_diagnostic_review(summary_payload),
    )

    closeout = _closeout_reclassification_payload(final_matrix, selection_result)
    _write_yaml(DEFAULT_CLOSEOUT_RECLASSIFICATION_PATH, closeout)
    _write_markdown(
        DEFAULT_CLOSEOUT_RECLASSIFICATION_DOC_PATH,
        _render_generic_review(closeout),
    )

    archive = _do_not_de_risk_archive_payload(
        false_risk_off=false_risk_off,
        slice_2022=slice_2022,
        actual_path=actual_path,
        selection_result=selection_result,
    )
    _write_yaml(DEFAULT_DO_NOT_ARCHIVE_PATH, archive)
    _write_markdown(DEFAULT_DO_NOT_ARCHIVE_DOC_PATH, _render_do_not_archive_review(archive))

    behavior = _behavior_payload(
        episodes=episodes,
        slice_2022=slice_2022,
        dependence_2023=dependence_2023,
    )
    _write_yaml(DEFAULT_BEHAVIOR_PATH, behavior)
    _write_markdown(DEFAULT_BEHAVIOR_DOC_PATH, _render_behavior_review(behavior))

    tradeoff = _tradeoff_payload(summary_payload, false_add_risk, actual_path)
    _write_yaml(DEFAULT_TRADEOFF_PATH, tradeoff)
    _write_markdown(DEFAULT_TRADEOFF_DOC_PATH, _render_tradeoff_review(tradeoff))

    compatibility = _compatibility_payload(summary_payload, tradeoff)
    _write_yaml(DEFAULT_COMPATIBILITY_PATH, compatibility)
    _write_markdown(DEFAULT_COMPATIBILITY_DOC_PATH, _render_generic_review(compatibility))

    _write_markdown(
        DEFAULT_OWNER_BRIEF_PATH,
        _render_owner_brief(
            closeout=closeout,
            archive=archive,
            diagnostic=summary_payload,
            tradeoff=tradeoff,
            compatibility=compatibility,
        ),
    )

    summary_payload["artifact_paths"].update(
        {
            "diagnostic_yaml": str(DEFAULT_DIAGNOSTIC_REVIEW_PATH),
            "diagnostic_review": str(DEFAULT_DIAGNOSTIC_REVIEW_DOC_PATH),
            "closeout_reclassification_yaml": str(DEFAULT_CLOSEOUT_RECLASSIFICATION_PATH),
            "do_not_de_risk_archive_yaml": str(DEFAULT_DO_NOT_ARCHIVE_PATH),
            "behavior_yaml": str(DEFAULT_BEHAVIOR_PATH),
            "tradeoff_yaml": str(DEFAULT_TRADEOFF_PATH),
            "compatibility_yaml": str(DEFAULT_COMPATIBILITY_PATH),
            "owner_brief": str(DEFAULT_OWNER_BRIEF_PATH),
        }
    )
    _write_json(summary_json_path, summary_payload)
    return summary_payload


def _validate_static_contracts(
    contract: Mapping[str, Any],
    metric_policy: Mapping[str, Any],
    forward_log_spec: Mapping[str, Any],
) -> None:
    diagnostic = _mapping(contract.get("risk_on_veto_diagnostic_contract"))
    if diagnostic.get("status") != "observe_only":
        raise ValueError("risk_on_veto diagnostic contract must be observe_only")
    forbidden_true_flags = [
        "can_emit_weights",
        "can_emit_trade_advice",
        "can_enable_growth_overlay",
        "can_enable_tqqq",
        "owner_review_allowed",
        "promotion_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
    ]
    enabled = [flag for flag in forbidden_true_flags if bool(diagnostic.get(flag))]
    if enabled:
        raise ValueError(f"risk_on_veto diagnostic contract enables forbidden flags: {enabled}")

    policy = _mapping(metric_policy.get("risk_on_veto_metric_policy"))
    required_metrics = {
        "raw_false_add_risk_cost_when_veto_active",
        "raw_false_add_risk_cost_when_veto_inactive",
        "avoided_false_add_risk_cost_due_to_veto",
        "captured_upside_lost_due_to_veto",
        "net_veto_benefit",
    }
    missing = required_metrics - set(policy)
    if missing:
        raise ValueError(f"risk_on_veto metric policy missing metrics: {sorted(missing)}")

    log_spec = _mapping(forward_log_spec.get("risk_on_veto_forward_log"))
    blocked = set(_records(log_spec.get("blocked_fields")))
    if not FORBIDDEN_ALLOCATION_FIELDS <= blocked:
        raise ValueError("risk_on_veto forward log must block all allocation fields")


def _load_risk_veto_labels(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    required = {
        "date",
        "research_window_id",
        "risk_on_veto_label_v3",
        "veto_reasons",
        "confidence",
        "false_add_risk_cost_proxy",
        "captured_upside_proxy",
        *VOLATILITY_FEATURES,
        *RATES_LIQUIDITY_FEATURES,
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"risk-on veto labels missing columns: {sorted(missing)}")
    frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    return frame.sort_values("date").drop_duplicates("date", keep="last")


def _load_channel_v3_composer(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    required = {"date", "growth_allowed", "veto_reasons", "confidence"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"channel v3 composer missing columns: {sorted(missing)}")
    return frame.sort_values("date").drop_duplicates("date", keep="last")


def _load_channel_pit_matrix(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    required = {"date", "research_window_id", *VOLATILITY_FEATURES, *RATES_LIQUIDITY_FEATURES}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"channel PIT matrix missing columns: {sorted(missing)}")
    frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    return frame.sort_values("date").drop_duplicates("date", keep="last")


def _load_baseline_reference(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    if "research_window_id" in frame.columns:
        frame = frame.loc[frame["research_window_id"].astype(str) == PRIMARY_WINDOW_ID].copy()
    if "trend_state" not in frame.columns:
        raise ValueError(f"baseline composer missing trend_state: {path}")
    add_risk_pred = _bool_series(frame, "add_risk_pred")
    high_confidence_pred = _bool_series(frame, "high_confidence_risk_on_pred")
    trend_add_risk = frame["trend_state"].astype(str).isin(["constructive", "risk_on"])
    frame["would_have_add_risk_reference"] = (
        trend_add_risk | add_risk_pred | high_confidence_pred
    )
    return frame[["date", "trend_state", "would_have_add_risk_reference"]].sort_values("date")


def _build_episode_rows(
    *,
    labels: pd.DataFrame,
    channel_composer: pd.DataFrame,
    pit_matrix: pd.DataFrame,
    baseline: pd.DataFrame,
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    frame = labels.merge(
        channel_composer[["date", "growth_allowed", "confidence", "veto_reasons"]],
        on="date",
        how="left",
        suffixes=("", "_composer"),
    )
    pit_columns = ["date", *VOLATILITY_FEATURES, *RATES_LIQUIDITY_FEATURES]
    frame = frame.merge(
        pit_matrix[pit_columns],
        on="date",
        how="left",
        suffixes=("", "_pit"),
    )
    frame = frame.merge(baseline, on="date", how="left")
    qqq = prices["QQQ"].dropna()

    rows: list[dict[str, Any]] = []
    for record in frame.to_dict("records"):
        ts = pd.Timestamp(record["date"])
        veto_active = _truthy(record.get("risk_on_veto_label_v3"))
        would_have_add_risk = _truthy(record.get("would_have_add_risk_reference"))
        blocked_add_risk = veto_active and would_have_add_risk
        false_cost = _float(record.get("false_add_risk_cost_proxy"))
        captured_upside = _float(record.get("captured_upside_proxy"))
        avoided = false_cost if blocked_add_risk else 0.0
        lost = captured_upside if blocked_add_risk else 0.0
        outcomes = _future_outcomes(qqq, ts)
        rows.append(
            {
                "date": ts.date().isoformat(),
                "research_window_id": PRIMARY_WINDOW_ID,
                "veto_active": veto_active,
                "veto_reasons": _coalesced_text(
                    record.get("veto_reasons_composer"),
                    record.get("veto_reasons"),
                ),
                "volatility_compression_features": _feature_json(
                    record,
                    VOLATILITY_FEATURES,
                ),
                "rates_liquidity_features": _feature_json(record, RATES_LIQUIDITY_FEATURES),
                "would_have_add_risk_reference": would_have_add_risk,
                "blocked_add_risk": blocked_add_risk,
                "future_1d_return": outcomes.get("future_1d_return"),
                "future_5d_return": outcomes.get("future_5d_return"),
                "future_10d_return": outcomes.get("future_10d_return"),
                "future_20d_return": outcomes.get("future_20d_return"),
                "future_max_drawdown": outcomes.get("future_max_drawdown"),
                "captured_upside_lost": _round(lost),
                "avoided_false_add_risk_cost": _round(avoided),
                "net_veto_benefit": _round(avoided - lost),
                "_false_add_risk_cost_proxy": false_cost,
                "_captured_upside_proxy": captured_upside,
            }
        )
    return rows


def _future_outcomes(prices: pd.Series, ts: pd.Timestamp) -> dict[str, float | None]:
    if ts not in prices.index:
        return {
            "future_1d_return": None,
            "future_5d_return": None,
            "future_10d_return": None,
            "future_20d_return": None,
            "future_max_drawdown": None,
        }
    pos = prices.index.get_loc(ts)
    if isinstance(pos, slice) or not isinstance(pos, int):
        return {}
    start_price = _float(prices.iloc[pos])
    outcomes: dict[str, float | None] = {}
    for horizon in [1, 5, 10, 20]:
        key = f"future_{horizon}d_return"
        if pos + horizon < len(prices):
            outcomes[key] = _round(_float(prices.iloc[pos + horizon]) / start_price - 1.0)
        else:
            outcomes[key] = None
    if pos + 20 < len(prices):
        future_path = prices.iloc[pos + 1 : pos + 21]
        outcomes["future_max_drawdown"] = _round((future_path / start_price - 1.0).min())
    else:
        outcomes["future_max_drawdown"] = None
    return outcomes


def _diagnostic_summary(
    *,
    episodes: Sequence[Mapping[str, Any]],
    labels: pd.DataFrame,
    selection_result: Mapping[str, Any],
    false_add_risk: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    active = labels["risk_on_veto_label_v3"].map(_truthy)
    blocked = [row for row in episodes if bool(row.get("blocked_add_risk"))]
    add_risk_reference_rows = [
        row for row in episodes if bool(row.get("would_have_add_risk_reference"))
    ]
    unblocked_reference = [
        row
        for row in add_risk_reference_rows
        if not bool(row.get("veto_active"))
    ]
    positive_cost = [
        row
        for row in blocked
        if _float(row.get("avoided_false_add_risk_cost")) > 0.0
    ]
    false_positive = [
        row
        for row in blocked
        if _float(row.get("avoided_false_add_risk_cost")) <= 0.0
        and _float(row.get("captured_upside_lost")) > 0.0
    ]
    false_negative = [
        row
        for row in unblocked_reference
        if _float(row.get("_false_add_risk_cost_proxy")) > 0.0
    ]
    avoided_total = _sum(row.get("avoided_false_add_risk_cost") for row in blocked)
    lost_total = _sum(row.get("captured_upside_lost") for row in blocked)
    net_total = _sum(row.get("net_veto_benefit") for row in blocked)
    false_add_summary = _mapping(false_add_risk.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    selection_summary = _mapping(selection_result.get("summary"))
    return {
        "data_quality_status": data_quality.get("status"),
        "data_quality_passed": data_quality.get("passed"),
        "episode_count": len(episodes),
        "veto_active_count": int(active.sum()),
        "veto_active_rate": _rate(int(active.sum()), len(labels)),
        "would_have_add_risk_reference_count": len(add_risk_reference_rows),
        "blocked_add_risk_count": len(blocked),
        "raw_false_add_risk_cost_when_veto_active": _mean(
            labels.loc[active, "false_add_risk_cost_proxy"]
        ),
        "raw_false_add_risk_cost_when_veto_inactive": _mean(
            labels.loc[~active, "false_add_risk_cost_proxy"]
        ),
        "prior_active_false_add_risk_cost_mean": false_add_summary.get(
            "active_false_add_risk_cost_mean"
        ),
        "prior_inactive_false_add_risk_cost_mean": false_add_summary.get(
            "inactive_false_add_risk_cost_mean"
        ),
        "avoided_false_add_risk_cost_due_to_veto_total": avoided_total,
        "avoided_false_add_risk_cost_due_to_veto_mean": _mean(
            [row.get("avoided_false_add_risk_cost") for row in blocked]
        ),
        "captured_upside_lost_due_to_veto_total": lost_total,
        "captured_upside_lost_due_to_veto_mean": _mean(
            [row.get("captured_upside_lost") for row in blocked]
        ),
        "net_veto_benefit_total": net_total,
        "net_veto_benefit_mean": _mean([row.get("net_veto_benefit") for row in blocked]),
        "net_veto_benefit_positive": net_total > 0.0,
        "veto_hit_rate": _rate(len(positive_cost), len(blocked)),
        "veto_false_positive_rate": _rate(len(false_positive), len(blocked)),
        "veto_false_negative_rate": _rate(len(false_negative), len(unblocked_reference)),
        "false_add_risk_cost_reduction": false_add_summary.get(
            "false_add_risk_cost_reduction"
        ),
        "defensive_probe_regression_count": actual_summary.get(
            "defensive_probe_regression_count"
        ),
        "risk_on_veto_defensive_regression_reduction": actual_summary.get(
            "risk_on_veto_defensive_regression_reduction"
        ),
        "selection_status": selection_summary.get("selection_status"),
        "final_status": selection_summary.get("final_status"),
        "candidate_count": 0,
        "owner_review_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "episode_log_forbidden_allocation_fields_absent": True,
    }


def _closeout_reclassification_payload(
    final_matrix: Mapping[str, Any],
    selection_result: Mapping[str, Any],
) -> dict[str, Any]:
    final_summary = _mapping(final_matrix.get("summary"))
    selection_summary = _mapping(selection_result.get("summary"))
    summary = {
        "final_status": final_summary.get("final_status"),
        "do_not_de_risk_pass": final_summary.get("do_not_de_risk_pass"),
        "risk_on_veto_pass": final_summary.get("risk_on_veto_pass"),
        "candidate_count": final_summary.get("candidate_count", 0),
        "owner_review_allowed": False,
        "promotion": "blocked",
        "paper_shadow": False,
        "production": False,
        "broker": "none",
        "allowed_statuses": [
            "DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT",
            "RISK_ON_VETO_V3_OBSERVE_ONLY_DIAGNOSTIC",
            "CHANNEL_V3_NO_ALLOCATION_CANDIDATE",
        ],
        "selection_status": selection_summary.get("selection_status"),
    }
    return _payload(
        report_type="channel_specific_v3_closeout_reclassification",
        title="Channel-Specific v3 Closeout Reclassification",
        status="CHANNEL_V3_NO_ALLOCATION_CANDIDATE",
        summary=summary,
    )


def _do_not_de_risk_archive_payload(
    *,
    false_risk_off: Mapping[str, Any],
    slice_2022: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    selection_result: Mapping[str, Any],
) -> dict[str, Any]:
    false_summary = _mapping(false_risk_off.get("summary"))
    slice_summary = _mapping(slice_2022.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    rows = _records(selection_result.get("rows"))
    do_not_row = next(
        (row for row in rows if row.get("channel") == "do_not_de_risk"),
        {},
    )
    summary = {
        "archive_status": "DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT",
        "do_not_de_risk_selection_status": do_not_row.get("status"),
        "false_risk_off_reduction": false_summary.get("false_risk_off_cost_reduction"),
        "missed_upside_reduction": false_summary.get("missed_upside_reduction"),
        "2022_slice_not_worse": slice_summary.get("2022_slice_not_worse"),
        "defensive_probe_regression_count": actual_summary.get(
            "defensive_probe_regression_count"
        ),
        "next_model_allowed": False,
        "allocation_allowed": False,
        "owner_review_allowed": False,
        "allowed_future_use": [
            "archived_research_evidence",
            "failure_attribution_reference",
            "future_reopen_only_with_new_feature_family_or_forward_evidence",
        ],
    }
    return _payload(
        report_type="do_not_de_risk_v3_archive",
        title="Do-Not-De-Risk v3 Archive Review",
        status="DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT",
        summary=summary,
    )


def _behavior_payload(
    *,
    episodes: Sequence[Mapping[str, Any]],
    slice_2022: Mapping[str, Any],
    dependence_2023: Mapping[str, Any],
) -> dict[str, Any]:
    rows_2022 = [row for row in episodes if str(row.get("date", "")).startswith("2022-")]
    rows_2023_plus = [row for row in episodes if str(row.get("date", "")) >= "2023-01-01"]
    slice_summary = _mapping(slice_2022.get("summary"))
    dep_summary = _mapping(dependence_2023.get("summary"))
    summary = {
        "2022_row_count": len(rows_2022),
        "2023_plus_row_count": len(rows_2023_plus),
        "risk_on_veto_2022_active_rate": slice_summary.get("risk_on_veto_2022_active_rate"),
        "risk_on_veto_2023_plus_active_rate": dep_summary.get(
            "risk_on_veto_2023_plus_active_rate"
        ),
        "risk_on_veto_2023_plus_only": dep_summary.get("risk_on_veto_2023_plus_only"),
        "2022_blocked_add_risk_count": _count(rows_2022, "blocked_add_risk"),
        "2023_plus_blocked_add_risk_count": _count(rows_2023_plus, "blocked_add_risk"),
        "2022_avoided_false_add_risk_cost_total": _sum(
            row.get("avoided_false_add_risk_cost") for row in rows_2022
        ),
        "2023_plus_avoided_false_add_risk_cost_total": _sum(
            row.get("avoided_false_add_risk_cost") for row in rows_2023_plus
        ),
        "2022_captured_upside_lost_total": _sum(
            row.get("captured_upside_lost") for row in rows_2022
        ),
        "2023_plus_captured_upside_lost_total": _sum(
            row.get("captured_upside_lost") for row in rows_2023_plus
        ),
        "2022_slice_not_worse": slice_summary.get("2022_slice_not_worse"),
    }
    summary["2023_plus_over_blocks_captured_upside"] = (
        _float(summary["2023_plus_captured_upside_lost_total"])
        > _float(summary["2023_plus_avoided_false_add_risk_cost_total"])
    )
    return _payload(
        report_type="risk_on_veto_2022_2023_behavior",
        title="Risk-On Veto 2022 / 2023+ Behavior Review",
        status="RISK_ON_VETO_2022_2023_BEHAVIOR_READY_OBSERVE_ONLY",
        summary=summary,
    )


def _tradeoff_payload(
    diagnostic: Mapping[str, Any],
    false_add_risk: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(diagnostic.get("summary"))
    false_summary = _mapping(false_add_risk.get("summary"))
    actual_summary = _mapping(actual_path.get("summary"))
    tradeoff = {
        "false_add_risk_cost_reduction": false_summary.get("false_add_risk_cost_reduction"),
        "raw_active_false_add_risk_cost_mean": summary.get(
            "raw_false_add_risk_cost_when_veto_active"
        ),
        "raw_inactive_false_add_risk_cost_mean": summary.get(
            "raw_false_add_risk_cost_when_veto_inactive"
        ),
        "avoided_false_add_risk_cost_total": summary.get(
            "avoided_false_add_risk_cost_due_to_veto_total"
        ),
        "captured_upside_lost_total": summary.get("captured_upside_lost_due_to_veto_total"),
        "net_veto_benefit_total": summary.get("net_veto_benefit_total"),
        "net_veto_benefit_positive": summary.get("net_veto_benefit_positive"),
        "veto_hit_rate": summary.get("veto_hit_rate"),
        "veto_false_positive_rate": summary.get("veto_false_positive_rate"),
        "veto_false_negative_rate": summary.get("veto_false_negative_rate"),
        "defensive_probe_regression_count": actual_summary.get(
            "defensive_probe_regression_count"
        ),
        "risk_on_veto_defensive_regression_reduction": actual_summary.get(
            "risk_on_veto_defensive_regression_reduction"
        ),
        "return_seeking_diagnostic_over_conservative": (
            _float(summary.get("captured_upside_lost_due_to_veto_total"))
            > _float(summary.get("avoided_false_add_risk_cost_due_to_veto_total"))
        ),
    }
    return _payload(
        report_type="risk_on_veto_tradeoff_matrix",
        title="Risk-On Veto Tradeoff Review",
        status="RISK_ON_VETO_TRADEOFF_READY_OBSERVE_ONLY",
        summary=tradeoff,
    )


def _compatibility_payload(
    diagnostic: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
) -> dict[str, Any]:
    diagnostic_summary = _mapping(diagnostic.get("summary"))
    tradeoff_summary = _mapping(tradeoff.get("summary"))
    compatible = bool(tradeoff_summary.get("false_add_risk_cost_reduction")) and bool(
        tradeoff_summary.get("net_veto_benefit_positive")
    )
    too_strict = bool(tradeoff_summary.get("return_seeking_diagnostic_over_conservative"))
    if compatible:
        status = "VETO_COMPATIBLE_WITH_RETURN_SEEKING_DIAGNOSTIC"
    elif too_strict:
        status = "VETO_TOO_STRICT_FOR_RETURN_SEEKING_DIAGNOSTIC"
    else:
        status = "VETO_DIAGNOSTIC_ONLY_NO_INTEGRATION"
    summary = {
        "compatibility_status": status,
        "can_block_return_seeking_diagnostic": compatible,
        "can_emit_allocation": False,
        "can_emit_weights": False,
        "owner_review_allowed": False,
        "promotion_allowed": False,
        "blocked_add_risk_count": diagnostic_summary.get("blocked_add_risk_count"),
        "net_veto_benefit_total": diagnostic_summary.get("net_veto_benefit_total"),
        "reason": (
            "即使 compatible，也只能作为 return-seeking diagnostic blocker，不能输出 allocation。"
        ),
    }
    return _payload(
        report_type="risk_on_veto_return_seeking_diagnostic_compatibility",
        title="Risk-On Veto Return-Seeking Diagnostic Compatibility",
        status=status,
        summary=summary,
    )


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
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
        "research_window_id": PRIMARY_WINDOW_ID,
        "research_window_alias": PRIMARY_WINDOW_ALIAS,
        "requested_start": REQUESTED_START,
        "actual_start": REQUESTED_START,
        "actual_portfolio_start": REQUESTED_START,
        "end": "latest",
        "window_role": "primary_validated",
        "data_quality_contract": "secondary_cross_checked",
        "exact_or_proxy": "exact",
        "summary": _clean_for_yaml(dict(summary)),
        "research_audit_metadata": _audit_metadata(),
        **SAFETY_BOUNDARY,
    }
    if rows is not None:
        payload["rows"] = _clean_for_yaml(list(rows))
    return payload


def _audit_metadata() -> dict[str, Any]:
    return {
        "modified_layer": "first_layer",
        "modified_channel": "risk_veto",
        "frozen_channels": ["defensive", "return_seeking_diagnostic", "risk_veto"],
        "frozen_first_layer_version": "first_layer_v2_return_seeking_diagnostic_only",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "channel_specific_labels_v3",
        "feature_set_version": "channel_specific_feature_set_v1_locked",
        "model_version": "risk_on_veto_v3_observe_only_diagnostic",
        "threshold_policy": (
            "risk_on_veto_metric_policy_v1+risk_on_veto_diagnostic_contract_v1"
        ),
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "signal_usage_matrix_version": "first_layer_signal_usage_matrix_v2",
        "boundary_contract_version": "two_layer_strategy_boundary_contract_v1",
        "selection_rule_version": "risk_on_veto_observe_only_diagnostic_v1",
        "candidate_count": 0,
        "pre_registered_selection_rule": (
            "risk_on_veto_v3_selection_rule_v1+risk_on_veto_metric_policy_v1"
        ),
    }


def _render_diagnostic_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Risk-On Veto Observe-Only Diagnostic Review",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            "本报告由 `aits research trends risk-on-veto-diagnostic` 生成。"
            "`risk_on_veto` 是 veto / blocker，不是 allocation、add-risk、buy 或 TQQQ signal。",
            "",
            "## 关键指标",
            "",
            f"- data_quality_status: `{summary.get('data_quality_status')}`",
            f"- episode_count: `{summary.get('episode_count')}`",
            f"- veto_active_rate: `{summary.get('veto_active_rate')}`",
            "- raw active false-add-risk cost: "
            f"`{summary.get('raw_false_add_risk_cost_when_veto_active')}`",
            "- raw inactive false-add-risk cost: "
            f"`{summary.get('raw_false_add_risk_cost_when_veto_inactive')}`",
            "- avoided false-add-risk cost total: "
            f"`{summary.get('avoided_false_add_risk_cost_due_to_veto_total')}`",
            "- captured upside lost total: "
            f"`{summary.get('captured_upside_lost_due_to_veto_total')}`",
            f"- net_veto_benefit_total: `{summary.get('net_veto_benefit_total')}`",
            f"- veto_hit_rate: `{summary.get('veto_hit_rate')}`",
            f"- veto_false_positive_rate: `{summary.get('veto_false_positive_rate')}`",
            f"- veto_false_negative_rate: `{summary.get('veto_false_negative_rate')}`",
            "",
            "## 解释边界",
            "",
            "active raw cost 高于 inactive mean 不能单独解释为 veto 失败；"
            "active 行本身可能处于更危险环境。只有当参考路径本来会 add-risk 且 veto "
            "阻断时，才计入 avoided cost 和 captured-upside lost。",
            "",
            "本报告不生成 weights、strategy candidate、trade action、paper-shadow action、"
            "production action 或 broker action。",
            "",
        ]
    )


def _render_do_not_archive_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Do-Not-De-Risk v3 Archive Review",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            "`do_not_de_risk v3` 未通过 false risk-off / missed upside / defensive "
            "regression gate，本批正式归档为 no-material-improvement。",
            "",
            "## 归档证据",
            "",
            f"- false_risk_off_reduction: `{summary.get('false_risk_off_reduction')}`",
            f"- missed_upside_reduction: `{summary.get('missed_upside_reduction')}`",
            f"- 2022_slice_not_worse: `{summary.get('2022_slice_not_worse')}`",
            "- defensive_probe_regression_count: "
            f"`{summary.get('defensive_probe_regression_count')}`",
            "",
            "后续只允许作为 archived research evidence、failure attribution reference，"
            "或在新增 feature family / forward evidence 后重新打开研究。",
            "",
        ]
    )


def _render_behavior_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Risk-On Veto 2022 / 2023+ Behavior Review",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            "## 2022 drawdown / transition slice",
            "",
            f"- active_rate: `{summary.get('risk_on_veto_2022_active_rate')}`",
            f"- blocked_add_risk_count: `{summary.get('2022_blocked_add_risk_count')}`",
            "- avoided_false_add_risk_cost_total: "
            f"`{summary.get('2022_avoided_false_add_risk_cost_total')}`",
            "",
            "## 2023+ AI / tech trend slice",
            "",
            f"- active_rate: `{summary.get('risk_on_veto_2023_plus_active_rate')}`",
            f"- blocked_add_risk_count: `{summary.get('2023_plus_blocked_add_risk_count')}`",
            "- captured_upside_lost_total: "
            f"`{summary.get('2023_plus_captured_upside_lost_total')}`",
            "- over_blocks_captured_upside: "
            f"`{summary.get('2023_plus_over_blocks_captured_upside')}`",
            "",
            f"`risk_on_veto_2023_plus_only={summary.get('risk_on_veto_2023_plus_only')}`。",
            "",
        ]
    )


def _render_tradeoff_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# Risk-On Veto Tradeoff Review",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            f"- false_add_risk_cost_reduction: `{summary.get('false_add_risk_cost_reduction')}`",
            "- avoided_false_add_risk_cost_total: "
            f"`{summary.get('avoided_false_add_risk_cost_total')}`",
            f"- captured_upside_lost_total: `{summary.get('captured_upside_lost_total')}`",
            f"- net_veto_benefit_total: `{summary.get('net_veto_benefit_total')}`",
            "- risk_on_veto_defensive_regression_reduction: "
            f"`{summary.get('risk_on_veto_defensive_regression_reduction')}`",
            "- return_seeking_diagnostic_over_conservative: "
            f"`{summary.get('return_seeking_diagnostic_over_conservative')}`",
            "",
            "该 tradeoff matrix 只用于 observe-only diagnostic。即使 net benefit 为正，"
            "也不能输出 allocation 或进入 promotion。",
            "",
        ]
    )


def _render_generic_review(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [f"# {payload.get('title')}", "", f"状态：`{payload.get('status')}`", "", "## 摘要", ""]
    for key, value in summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "所有结论均为 research-only / observe-only diagnostic，不构成 allocation、"
            "paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_owner_brief(
    *,
    closeout: Mapping[str, Any],
    archive: Mapping[str, Any],
    diagnostic: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
    compatibility: Mapping[str, Any],
) -> str:
    archive_summary = _mapping(archive.get("summary"))
    diagnostic_summary = _mapping(diagnostic.get("summary"))
    tradeoff_summary = _mapping(tradeoff.get("summary"))
    compatibility_summary = _mapping(compatibility.get("summary"))
    closeout_summary = _mapping(closeout.get("summary"))
    return "\n".join(
        [
            "# Risk-On Veto Observe-Only Owner Brief",
            "",
            "## 1. 为什么 do_not_de_risk v3 归档？",
            "",
            "`do_not_de_risk v3` 未通过 false risk-off / missed upside / defensive "
            "regression gate。归档状态为 "
            f"`{archive_summary.get('archive_status')}`；defensive_probe_regression_count="
            f"`{archive_summary.get('defensive_probe_regression_count')}`，"
            f"2022_slice_not_worse=`{archive_summary.get('2022_slice_not_worse')}`。",
            "",
            "## 2. 为什么 risk_on_veto 保留？",
            "",
            "`risk_on_veto` 是 TRADING-1976～2005 唯一通过的 channel，"
            f"final_status=`{closeout_summary.get('final_status')}`。它保留的理由是 "
            "false add-risk / compiler veto diagnostic 仍有解释价值。",
            "",
            "## 3. risk_on_veto 是什么，不是什么？",
            "",
            "它是 veto / blocker，用于说明何时不应轻易 add-risk、growth overlay "
            "或 TQQQ exposure。它不是 allocation signal、risk-on signal、buy signal "
            "或 TQQQ signal，也不能进入 owner review、promotion、paper-shadow、production "
            "或 broker。",
            "",
            "## 4. active false-add-risk cost 为什么不能单独解释？",
            "",
            "Veto active 行本来就可能处在更危险环境，所以 raw active cost 高于 inactive "
            "mean 不等于 veto 失败。必须同时看 blocked add-risk reference 下的 avoided cost、"
            "captured-upside lost 和 net veto benefit。",
            "",
            "## 当前诊断摘要",
            "",
            "- raw active false-add-risk cost: "
            f"`{diagnostic_summary.get('raw_false_add_risk_cost_when_veto_active')}`",
            "- raw inactive false-add-risk cost: "
            f"`{diagnostic_summary.get('raw_false_add_risk_cost_when_veto_inactive')}`",
            "- avoided false-add-risk cost total: "
            f"`{diagnostic_summary.get('avoided_false_add_risk_cost_due_to_veto_total')}`",
            f"- captured upside lost total: `{tradeoff_summary.get('captured_upside_lost_total')}`",
            f"- net veto benefit total: `{tradeoff_summary.get('net_veto_benefit_total')}`",
            f"- compatibility status: `{compatibility_summary.get('compatibility_status')}`",
            "",
            "结论：本批只建立 observe-only diagnostic 能力。candidate_count=0，"
            "promotion / paper-shadow / production / broker 全部 disabled。",
            "",
        ]
    )


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].map(_truthy)


def _feature_json(record: Mapping[str, Any], features: Sequence[str]) -> str:
    payload = {feature: _round(_float(record.get(feature))) for feature in features}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _coalesced_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        text = str(value)
        if text and text.lower() != "nan":
            return text
    return ""


def _count(rows: Sequence[Mapping[str, Any]], key: str) -> int:
    return sum(1 for row in rows if bool(row.get(key)))


def _mean(values: Any) -> float:
    if isinstance(values, pd.Series):
        values = values.dropna().tolist()
    cleaned = [_float(value) for value in values if value is not None and not _is_nan(value)]
    if not cleaned:
        return 0.0
    return _round(sum(cleaned) / len(cleaned))


def _sum(values: Any) -> float:
    return _round(
        sum(_float(value) for value in values if value is not None and not _is_nan(value))
    )


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return _round(numerator / denominator)


def _round(value: Any, digits: int = 6) -> float:
    return round(_float(value), digits)


def _float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(number) or math.isinf(number):
        return 0.0
    return number


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_clean_for_yaml(dict(payload)), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_clean_for_yaml(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fieldnames: Sequence[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_clean_for_yaml(list(rows)))


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _clean_for_yaml(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clean_for_yaml(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, tuple):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        return _clean_for_yaml(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value
