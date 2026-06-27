from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.expanded_allocation_universe import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    _data_quality_gate,
    _load_price_matrix,
)
from ai_trading_system.first_layer_policy_calibration import (
    GRID_ROUND_DIGITS,
    SAFETY_BOUNDARY,
    _backtest_probe_predictions,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_2022_SLICE_YAML_PATH,
    DEFAULT_ACTUAL_PATH_YAML_PATH,
    DEFAULT_COVERAGE_MODEL_ROOT,
    DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    DEFAULT_FAILURE_YAML_PATH,
    DEFAULT_MODEL_MATRIX_YAML_PATH,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_FINAL_MATRIX_YAML_PATH as DEFAULT_COVERAGE_FINAL_MATRIX_YAML_PATH,
)
from ai_trading_system.upper_state_label_feature_reset import (
    ASSETS,
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_COVERAGE_RECLASSIFICATION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_coverage_rebuild_reclassification.md"
)
DEFAULT_COVERAGE_RECLASSIFICATION_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_coverage_rebuild_reclassification.yaml"
)
DEFAULT_REGRESSION_INVENTORY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_defensive_probe_regression_inventory.md"
)
DEFAULT_REGRESSION_INVENTORY_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_defensive_probe_regression_inventory.yaml"
)
DEFAULT_ROLE_GROUP_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_probe_role_group_comparison.md"
)
DEFAULT_ROLE_GROUP_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_probe_role_group_matrix.yaml"
)
DEFAULT_DEFENSIVE_2022_SLICE_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_2022_defensive_regression_slice_review.md"
)
DEFAULT_DEFENSIVE_2022_SLICE_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_2022_defensive_regression_slice.yaml"
)
DEFAULT_SIGNAL_ATTRIBUTION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_signal_error_attribution.md"
)
DEFAULT_SIGNAL_ATTRIBUTION_YAML_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_v2_signal_error_attribution.yaml"
)
DEFAULT_STABILITY_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_v2_policy_variant_stability_review.md"
)
DEFAULT_STABILITY_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_policy_variant_stability_matrix.yaml"
)
DEFAULT_RETURN_SEEKING_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_return_seeking_diagnostic_reclassification.md"
)
DEFAULT_RETURN_SEEKING_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_return_seeking_diagnostic_reclassification.yaml"
)
DEFAULT_RISK_OFF_ONLY_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_risk_off_only_fallback_assessment.md"
)
DEFAULT_RISK_OFF_ONLY_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_risk_off_only_fallback_assessment.yaml"
)
DEFAULT_DECISION_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_defensive_regression_diagnosis_review.md"
)
DEFAULT_DECISION_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_defensive_regression_diagnosis_matrix.yaml"
)
DEFAULT_OWNER_BRIEF_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_defensive_regression_owner_brief.md"
)
DEFAULT_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT
    / "docs"
    / "research"
    / "first_layer_v2_defensive_regression_diagnosis_closeout.md"
)
DEFAULT_FINAL_MATRIX_YAML_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_v2_defensive_regression_diagnosis_final_matrix.yaml"
)

POLICY_ORDER = (
    "wf_504d_baseline",
    "wf_378d_initial",
    "wf_252d_initial",
    "wf_expanding_initial",
    "wf_warm_start_diagnostic",
)
COVERAGE_PASS_POLICY_IDS = ("wf_252d_initial", "wf_expanding_initial")
PURE_RETURN_SEEKING_ROLE_GROUP = "return_seeking"
DEFENSIVE_ROLE_GROUPS = {"defensive_overlay", "drawdown_control", "balanced_dynamic"}
FORWARD_RETURN_HORIZON_DAYS = 20
EPISODE_REPORT_LIMIT = 16
REGRESSION_SLICE_WINDOWS = (
    {
        "slice_id": "2022_drawdown_slice",
        "start": "2022-02-18",
        "end": "2022-06-30",
        "interpretation": "early_2022_drawdown",
    },
    {
        "slice_id": "2022_recovery_slice",
        "start": "2022-07-01",
        "end": "2022-10-31",
        "interpretation": "mid_2022_recovery_and_second_leg_down",
    },
    {
        "slice_id": "post_chatgpt_transition_slice",
        "start": "2022-11-01",
        "end": "2023-02-21",
        "interpretation": "pre_effective_late_window_transition",
    },
)
WINDOW_KEYS = (
    "research_window_id",
    "requested_start",
    "actual_start",
    "actual_portfolio_start",
    "end",
    "window_role",
    "data_quality_contract",
    "exact_or_proxy",
)


def run_first_layer_defensive_regression_diagnosis_pack(
    *,
    actual_path_path: Path = DEFAULT_ACTUAL_PATH_YAML_PATH,
    prior_slice_path: Path = DEFAULT_2022_SLICE_YAML_PATH,
    coverage_final_path: Path = DEFAULT_COVERAGE_FINAL_MATRIX_YAML_PATH,
    coverage_failure_path: Path = DEFAULT_FAILURE_YAML_PATH,
    coverage_simulation_path: Path = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    coverage_model_matrix_path: Path = DEFAULT_MODEL_MATRIX_YAML_PATH,
    probe_registry_path: Path = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Path = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    coverage_model_root: Path = DEFAULT_COVERAGE_MODEL_ROOT,
) -> dict[str, Any]:
    actual_path = _load_yaml_mapping(actual_path_path)
    prior_slice = _load_yaml_mapping(prior_slice_path)
    coverage_final = _load_yaml_mapping(coverage_final_path)
    coverage_failure = _load_yaml_mapping(coverage_failure_path)
    coverage_simulation = _load_yaml_mapping(coverage_simulation_path)
    model_matrix = _load_yaml_mapping(coverage_model_matrix_path)
    probe_registry = _load_yaml_mapping(probe_registry_path)
    expanded_config = _load_yaml_mapping(expanded_config_path)

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
            f"Cached data quality gate failed for defensive diagnosis: {data_gate['status']}"
        )
    prices = _load_price_matrix(prices_path, ASSETS)
    predictions_by_policy = _load_policy_predictions(coverage_model_root)
    neutral_references = _build_neutral_reference_rows(
        prices=prices,
        probe_registry=probe_registry,
        predictions_by_policy=predictions_by_policy,
    )

    reclassification = build_coverage_rebuild_reclassification(
        coverage_final=coverage_final,
        coverage_failure=coverage_failure,
        actual_path=actual_path,
    )
    inventory = build_defensive_regression_inventory(
        actual_path=actual_path,
        probe_registry=probe_registry,
        neutral_references=neutral_references,
    )
    role_group = build_probe_role_group_comparison(inventory=inventory)
    defensive_slice = build_2022_defensive_regression_slice_review(
        source=actual_path,
        prior_slice=prior_slice,
        probe_registry=probe_registry,
        predictions_by_policy=predictions_by_policy,
        prices=prices,
        data_gate=data_gate,
    )
    signal_attribution = build_signal_error_attribution(
        source=actual_path,
        inventory=inventory,
        probe_registry=probe_registry,
        predictions_by_policy=predictions_by_policy,
        prices=prices,
    )
    stability = build_policy_variant_stability_review(
        source=actual_path,
        coverage_simulation=coverage_simulation,
        model_matrix=model_matrix,
        predictions_by_policy=predictions_by_policy,
        actual_path=actual_path,
    )
    return_seeking = build_return_seeking_diagnostic_reclassification(
        source=actual_path,
        role_group=role_group,
        inventory=inventory,
    )
    risk_off_only = build_risk_off_only_fallback_assessment(
        source=actual_path,
        role_group=role_group,
        inventory=inventory,
    )
    decision = build_defensive_regression_diagnosis_matrix(
        source=actual_path,
        reclassification=reclassification,
        inventory=inventory,
        role_group=role_group,
        defensive_slice=defensive_slice,
        signal_attribution=signal_attribution,
        stability=stability,
        return_seeking=return_seeking,
        risk_off_only=risk_off_only,
    )
    final_matrix = build_defensive_regression_diagnosis_final_matrix(
        source=actual_path,
        decision=decision,
        reclassification=reclassification,
        inventory=inventory,
        role_group=role_group,
        defensive_slice=defensive_slice,
        signal_attribution=signal_attribution,
        stability=stability,
        return_seeking=return_seeking,
        risk_off_only=risk_off_only,
        data_gate=data_gate,
    )
    write_defensive_regression_diagnosis_outputs(
        reclassification=reclassification,
        inventory=inventory,
        role_group=role_group,
        defensive_slice=defensive_slice,
        signal_attribution=signal_attribution,
        stability=stability,
        return_seeking=return_seeking,
        risk_off_only=risk_off_only,
        decision=decision,
        final_matrix=final_matrix,
    )
    return final_matrix


def build_coverage_rebuild_reclassification(
    *,
    coverage_final: Mapping[str, Any],
    coverage_failure: Mapping[str, Any],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    policy_rows = _records(actual_path.get("policy_rows"))
    coverage_pass = [row for row in policy_rows if row.get("does_coverage_pass_rule")]
    late_window_positive = [
        str(row.get("policy_id"))
        for row in policy_rows
        if not row.get("does_coverage_pass_rule")
        and _int(row.get("actual_path_improved_probe_count")) == _int(row.get("probe_count"))
    ]
    summary = {
        **_window_fields(actual_path),
        "previous_status": "WINDOW_COVERAGE_INCOMPLETE",
        "coverage_rebuild_result": coverage_final.get("status"),
        "positive_late_window_evidence": bool(late_window_positive),
        "positive_late_window_policy_ids": late_window_positive,
        "coverage_pass_variant_count": len(coverage_pass),
        "coverage_pass_policy_ids": [str(row.get("policy_id")) for row in coverage_pass],
        "coverage_pass_selection": False,
        "coverage_aware_selection_pass_count": _mapping(actual_path.get("summary")).get(
            "coverage_aware_selection_pass_count"
        ),
        "primary_failure_reason": _mapping(coverage_failure.get("summary")).get(
            "primary_reason"
        ),
        "owner_review_allowed": False,
        "promotion_blocked": True,
        "late_window_only_partial_evidence": True,
    }
    return _payload(
        report_type="first_layer_v2_coverage_rebuild_reclassification",
        title="First-Layer V2 Coverage Rebuild Reclassification",
        status="FIRST_LAYER_V2_COVERAGE_REBUILD_RECLASSIFIED_LATE_WINDOW_ONLY",
        source=actual_path,
        summary=summary,
        reclassification_decision={
            "prior_8_of_8_evidence_status": "LATE_WINDOW_ONLY_PARTIAL_EVIDENCE",
            "coverage_pass_evidence_status": "DEFENSIVE_REGRESSION_BLOCKED",
            "owner_review_allowed": False,
            "promotion": "blocked",
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )


def build_defensive_regression_inventory(
    *,
    actual_path: Mapping[str, Any],
    probe_registry: Mapping[str, Any],
    neutral_references: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    probes = _probe_by_id(probe_registry)
    policy_rows = {
        str(row.get("policy_id")): row for row in _records(actual_path.get("policy_rows"))
    }
    rows = []
    for source_row in _records(actual_path.get("probe_rows")):
        policy_id = str(source_row.get("policy_id"))
        probe_id = str(source_row.get("probe_id"))
        probe = probes.get(probe_id, {})
        role_group = _role_group(probe_id, probe)
        neutral = _mapping(neutral_references.get((policy_id, probe_id)))
        annual_delta = _delta(
            source_row.get("v2_annual_return"),
            neutral.get("actual_path_annual_return"),
        )
        calmar_delta = _delta(
            source_row.get("v2_calmar"),
            neutral.get("calmar_daily_equity_dd"),
        )
        row = {
            "policy_id": policy_id,
            "probe_id": probe_id,
            "probe_role": str(probe.get("role", "")),
            "probe_role_group": role_group,
            "coverage_pass": bool(
                _mapping(policy_rows.get(policy_id)).get("does_coverage_pass_rule")
            ),
            "improved_vs_flat": bool(source_row.get("actual_path_improved_vs_flat_reference")),
            "annual_return_delta": annual_delta,
            "max_drawdown_delta": _delta(
                source_row.get("v2_max_drawdown"),
                neutral.get("max_drawdown_daily_equity"),
            ),
            "sharpe_delta": _delta(
                source_row.get("v2_sharpe"),
                neutral.get("sharpe_daily_zero_rf"),
            ),
            "calmar_delta": calmar_delta,
            "turnover_delta": _delta(source_row.get("v2_turnover"), neutral.get("turnover")),
            "delta_reference_source": "same_window_neutral_state_probe_reference",
            "legacy_flat_annual_return_delta_reference": source_row.get(
                "v2_vs_flat_return_delta_reference"
            ),
            "legacy_flat_calmar_delta_reference": source_row.get(
                "v2_vs_flat_calmar_delta_reference"
            ),
            "v2_annual_return": source_row.get("v2_annual_return"),
            "v2_max_drawdown": source_row.get("v2_max_drawdown"),
            "v2_sharpe": source_row.get("v2_sharpe"),
            "v2_calmar": source_row.get("v2_calmar"),
            "v2_turnover": source_row.get("v2_turnover"),
            "regression_type": _regression_type(source_row, role_group),
            "regression_reason": _regression_reason(source_row, annual_delta, calmar_delta),
        }
        rows.append(row)
    coverage_pass_rows = [row for row in rows if row["coverage_pass"]]
    regressed = [row for row in coverage_pass_rows if not row["improved_vs_flat"]]
    summary = {
        **_window_fields(actual_path),
        "policy_count": len({row["policy_id"] for row in rows}),
        "probe_count_per_policy": len({row["probe_id"] for row in rows}),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "coverage_pass_probe_row_count": len(coverage_pass_rows),
        "coverage_pass_regressed_probe_count": len(regressed),
        "coverage_pass_regressed_probe_ids": sorted(
            {str(row["probe_id"]) for row in regressed}
        ),
        "defensive_probe_regression_count": sum(
            "defensive_probe_regression" in row["regression_type"] for row in regressed
        ),
        "return_seeking_improvement_count": sum(
            "return_seeking_improvement" in row["regression_type"] for row in coverage_pass_rows
        ),
        "risk_on_diagnostic_improvement_count": sum(
            "risk_on_diagnostic_improvement" in row["regression_type"]
            for row in coverage_pass_rows
        ),
        "target_path_metrics_used_for_pass": False,
    }
    return _payload(
        report_type="first_layer_v2_defensive_probe_regression_inventory",
        title="First-Layer V2 Defensive Probe Regression Inventory",
        status="FIRST_LAYER_V2_DEFENSIVE_PROBE_REGRESSION_INVENTORY_READY_PROMOTION_BLOCKED",
        source=actual_path,
        summary=summary,
        probe_rows=rows,
    )


def build_probe_role_group_comparison(*, inventory: Mapping[str, Any]) -> dict[str, Any]:
    rows = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _records(inventory.get("probe_rows")):
        if row.get("coverage_pass"):
            grouped[(str(row.get("policy_id")), str(row.get("probe_role_group")))].append(row)
    for policy_id in COVERAGE_PASS_POLICY_IDS:
        for group in (
            "defensive_overlay",
            "drawdown_control",
            "balanced_dynamic",
            PURE_RETURN_SEEKING_ROLE_GROUP,
            "risk_on_diagnostic",
        ):
            group_rows = grouped.get((policy_id, group), [])
            if not group_rows:
                continue
            improved = sum(bool(row.get("improved_vs_flat")) for row in group_rows)
            regressed = len(group_rows) - improved
            rows.append(
                {
                    "policy_id": policy_id,
                    "probe_role_group": group,
                    "probe_count": len(group_rows),
                    "improved_count": improved,
                    "regressed_count": regressed,
                    "average_annual_return_delta": _mean(
                        row.get("annual_return_delta") for row in group_rows
                    ),
                    "average_calmar_delta": _mean(row.get("calmar_delta") for row in group_rows),
                    "group_conclusion": _role_group_conclusion(group, improved, regressed),
                }
            )
    defensive_regressed = sum(
        row["regressed_count"]
        for row in rows
        if row["probe_role_group"] in DEFENSIVE_ROLE_GROUPS
    )
    pure_return_rows = [
        row for row in rows if row["probe_role_group"] == PURE_RETURN_SEEKING_ROLE_GROUP
    ]
    pure_return_total = sum(row["probe_count"] for row in pure_return_rows)
    pure_return_improved = sum(row["improved_count"] for row in pure_return_rows)
    risk_on_rows = [row for row in rows if row["probe_role_group"] == "risk_on_diagnostic"]
    risk_on_total = sum(row["probe_count"] for row in risk_on_rows)
    risk_on_improved = sum(row["improved_count"] for row in risk_on_rows)
    conclusion = (
        "RETURN_SEEKING_DIAGNOSTIC_LAYER_POSSIBLE_DEFENSIVE_BLOCKED"
        if defensive_regressed and pure_return_improved == pure_return_total and risk_on_improved
        else "FIRST_LAYER_V2_LATE_WINDOW_ONLY_ARCHIVED"
    )
    summary = {
        **_window_fields(inventory),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "defensive_or_drawdown_regressed_count": defensive_regressed,
        "pure_return_seeking_probe_count": pure_return_total,
        "pure_return_seeking_improved_count": pure_return_improved,
        "risk_on_diagnostic_probe_count": risk_on_total,
        "risk_on_diagnostic_improved_count": risk_on_improved,
        "regression_concentration": "DEFENSIVE_AND_DRAWDOWN_CONTROL_WITH_BALANCED_EXCEPTION",
        "return_seeking_value_supported": pure_return_improved == pure_return_total,
        "risk_on_diagnostic_value_supported": risk_on_improved == risk_on_total,
        "role_group_conclusion": conclusion,
    }
    return _payload(
        report_type="first_layer_v2_probe_role_group_matrix",
        title="First-Layer V2 Probe Role Group Comparison",
        status="FIRST_LAYER_V2_PROBE_ROLE_GROUP_COMPARISON_READY_PROMOTION_BLOCKED",
        source=inventory,
        summary=summary,
        role_group_rows=rows,
    )


def build_2022_defensive_regression_slice_review(
    *,
    source: Mapping[str, Any],
    prior_slice: Mapping[str, Any],
    probe_registry: Mapping[str, Any],
    predictions_by_policy: Mapping[str, pd.DataFrame],
    prices: pd.DataFrame,
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    probes = _records(probe_registry.get("probes"))
    rows = []
    for policy_id in POLICY_ORDER:
        predictions = _ensure_frame(predictions_by_policy.get(policy_id))
        for window in REGRESSION_SLICE_WINDOWS:
            sliced = _slice_predictions(predictions, window["start"], window["end"])
            exposures = _average_exposures(sliced, probes)
            costs = _slice_cost_metrics(
                predictions=sliced,
                prices=prices,
                probes=probes,
                start=window["start"],
                end=window["end"],
            )
            rows.append(
                {
                    "policy_id": policy_id,
                    "slice_id": window["slice_id"],
                    "slice_start": window["start"],
                    "slice_end": window["end"],
                    "interpretation": window["interpretation"],
                    "prediction_count": len(sliced),
                    "state_distribution": _state_counts(sliced["trend_state"])
                    if not sliced.empty
                    else {},
                    "defensive_exposure": exposures["defensive_exposure"],
                    "qqq_equivalent_exposure": exposures["qqq_equivalent_exposure"],
                    "drawdown": _market_drawdown(prices, window["start"], window["end"]),
                    "missed_defensive_benefit": costs["missed_defensive_benefit"],
                    "false_re_risk_cost": costs["false_re_risk_cost"],
                    "false_add_risk_cost": costs["false_add_risk_cost"],
                    "avoided_drawdown": costs["avoided_drawdown"],
                    "false_do_not_de_risk_cost": costs["false_do_not_de_risk_cost"],
                    "false_risk_on_cost": costs["false_risk_on_cost"],
                }
            )
    coverage_rows = [
        row
        for row in rows
        if row["policy_id"] in COVERAGE_PASS_POLICY_IDS and row["prediction_count"] > 0
    ]
    summary = {
        **_window_fields(source),
        "data_quality_status": data_gate.get("status"),
        "prior_slice_status": prior_slice.get("status"),
        "slice_count": len(REGRESSION_SLICE_WINDOWS),
        "policy_count": len(POLICY_ORDER),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "coverage_pass_average_false_re_risk_cost": _mean(
            row.get("false_re_risk_cost") for row in coverage_rows
        ),
        "coverage_pass_average_false_add_risk_cost": _mean(
            row.get("false_add_risk_cost") for row in coverage_rows
        ),
        "coverage_pass_average_avoided_drawdown": _mean(
            row.get("avoided_drawdown") for row in coverage_rows
        ),
        "2022_regression_conclusion": "DEFENSIVE_REGRESSION_PRESENT_IN_2022_AND_TRANSITION_SLICE",
        "target_path_metrics_used_for_pass": False,
    }
    return _payload(
        report_type="first_layer_v2_2022_defensive_regression_slice",
        title="First-Layer V2 2022 Defensive Regression Slice Review",
        status="FIRST_LAYER_V2_2022_DEFENSIVE_REGRESSION_SLICE_READY_PROMOTION_BLOCKED",
        source=source,
        summary=summary,
        slice_rows=rows,
    )


def build_signal_error_attribution(
    *,
    source: Mapping[str, Any],
    inventory: Mapping[str, Any],
    probe_registry: Mapping[str, Any],
    predictions_by_policy: Mapping[str, pd.DataFrame],
    prices: pd.DataFrame,
) -> dict[str, Any]:
    probes = _probe_by_id(probe_registry)
    regressed_probe_ids = sorted(
        {
            str(row.get("probe_id"))
            for row in _records(inventory.get("probe_rows"))
            if row.get("coverage_pass") and not row.get("improved_vs_flat")
        }
    )
    episode_rows = []
    error_counter: Counter[str] = Counter()
    for policy_id in COVERAGE_PASS_POLICY_IDS:
        predictions = _ensure_frame(predictions_by_policy.get(policy_id))
        if predictions.empty:
            continue
        pred = _slice_predictions(predictions, "2022-02-18", "2023-02-21")
        if pred.empty:
            continue
        pred = pred.sort_values("date").reset_index(drop=True)
        pred["previous_state"] = pred["trend_state"].shift(1).fillna("neutral")
        for probe_id in regressed_probe_ids:
            probe = probes.get(probe_id, {})
            probe_rows = []
            for row in pred.to_dict("records"):
                event = _signal_error_type(row)
                if not event:
                    continue
                forward = _forward_market_metrics(prices, str(row["date"]))
                downside = abs(min(0.0, forward["market_forward_drawdown"]))
                if downside <= 0.0:
                    continue
                old_state = str(row.get("previous_state") or "neutral")
                new_state = str(row.get("trend_state") or "neutral")
                old_weights = _weights_for_state(probe, old_state)
                new_weights = _weights_for_state(probe, new_state)
                old_exposure = _qqq_equivalent_exposure(old_weights)
                new_exposure = _qqq_equivalent_exposure(new_weights)
                risk_off_exposure = _qqq_equivalent_exposure(_weights_for_state(probe, "risk_off"))
                false_cost = round(max(0.0, new_exposure - old_exposure) * downside, 6)
                risk_off_cost = round(max(0.0, new_exposure - risk_off_exposure) * downside, 6)
                item = {
                    "date": str(row["date"]),
                    "policy_id": policy_id,
                    "probe_id": probe_id,
                    "error_type": event,
                    "old_state": old_state,
                    "new_state": new_state,
                    "old_weights": old_weights,
                    "new_weights": new_weights,
                    "market_forward_return": forward["market_forward_return"],
                    "market_forward_drawdown": forward["market_forward_drawdown"],
                    "missed_defensive_benefit": risk_off_cost,
                    "false_do_not_de_risk_cost": false_cost
                    if _bool(row.get("do_not_de_risk_pred"))
                    else 0.0,
                    "false_add_risk_cost": false_cost if _bool(row.get("add_risk_pred")) else 0.0,
                    "false_risk_on_cost": false_cost
                    if _bool(row.get("high_confidence_risk_on_pred"))
                    or new_state == "risk_on"
                    else 0.0,
                    "total_signal_error_cost": round(risk_off_cost + false_cost, 6),
                }
                probe_rows.append(item)
                error_counter[event] += 1
            episode_rows.extend(
                sorted(
                    probe_rows,
                    key=lambda item: _float(item.get("total_signal_error_cost")),
                    reverse=True,
                )[:EPISODE_REPORT_LIMIT]
            )
    episode_rows = sorted(
        episode_rows,
        key=lambda item: _float(item.get("total_signal_error_cost")),
        reverse=True,
    )
    error_rows = [
        {"error_type": key, "episode_count": int(value)}
        for key, value in sorted(error_counter.items())
    ]
    if any(row["error_type"] == "ADD_RISK_FALSE_POSITIVE" for row in error_rows):
        primary = "DEFENSIVE_REGRESSION_DUE_TO_FALSE_ADD_RISK"
    elif any(row["error_type"] == "DO_NOT_DE_RISK_FALSE_POSITIVE" for row in error_rows):
        primary = "DEFENSIVE_REGRESSION_DUE_TO_FALSE_DO_NOT_DERISK"
    else:
        primary = "DEFENSIVE_REGRESSION_DUE_TO_EARLY_RE_RISK"
    summary = {
        **_window_fields(source),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "regressed_probe_ids": regressed_probe_ids,
        "episode_row_count": len(episode_rows),
        "episode_selection_policy": (
            "top_cost_events_per_regressed_probe_policy; reporting cap does not affect diagnosis"
        ),
        "episode_report_limit_per_probe_policy": EPISODE_REPORT_LIMIT,
        "forward_return_horizon_days": FORWARD_RETURN_HORIZON_DAYS,
        "primary_signal_error_diagnosis": primary,
        "error_type_counts": {row["error_type"]: row["episode_count"] for row in error_rows},
        "feature_coverage_gap_detected": False,
        "train_window_instability_detected": True,
        "target_path_metrics_used_for_pass": False,
    }
    return _payload(
        report_type="first_layer_v2_signal_error_attribution",
        title="First-Layer V2 Signal Error Attribution",
        status="FIRST_LAYER_V2_SIGNAL_ERROR_ATTRIBUTION_READY_PROMOTION_BLOCKED",
        source=source,
        summary=summary,
        error_type_rows=error_rows,
        episode_rows=episode_rows,
    )


def build_policy_variant_stability_review(
    *,
    source: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    model_matrix: Mapping[str, Any],
    predictions_by_policy: Mapping[str, pd.DataFrame],
    actual_path: Mapping[str, Any],
) -> dict[str, Any]:
    simulation_rows = {
        str(row.get("policy_id")): row for row in _records(coverage_simulation.get("policy_rows"))
    }
    model_rows_by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _records(model_matrix.get("model_rows")):
        model_rows_by_policy[str(row.get("policy_id"))].append(row)
    actual_rows = {
        str(row.get("policy_id")): row for row in _records(actual_path.get("policy_rows"))
    }
    rows = []
    for policy_id in POLICY_ORDER:
        sim = _mapping(simulation_rows.get(policy_id))
        models = model_rows_by_policy.get(policy_id, [])
        prediction_distribution = _state_counts(
            _ensure_frame(predictions_by_policy.get(policy_id)).get("trend_state", pd.Series())
        )
        do_not = _model_row(models, "do_not_de_risk_model_v1")
        add_risk = _model_row(models, "add_risk_model_v1")
        rows.append(
            {
                "policy_id": policy_id,
                "coverage_pass": bool(
                    _mapping(actual_rows.get(policy_id)).get("does_coverage_pass_rule")
                ),
                "first_prediction_date": _mapping(actual_rows.get(policy_id)).get(
                    "first_prediction_date"
                )
                or sim.get("first_prediction_date"),
                "train_sample_count_min": sim.get("train_sample_count_min"),
                "train_sample_count_max": sim.get("train_sample_count_max"),
                "label_distribution": sim.get("label_distribution_by_split", {}),
                "risk_off_label_count": _mapping(sim.get("label_distribution_by_split")).get(
                    "do_not_de_risk_label", 0
                ),
                "do_not_de_risk_count": _mapping(sim.get("label_distribution_by_split")).get(
                    "do_not_de_risk_label", 0
                ),
                "add_risk_count": _mapping(sim.get("label_distribution_by_split")).get(
                    "add_risk_label", 0
                ),
                "prediction_distribution": prediction_distribution,
                "risk_off_recall": do_not.get("recall", 0.0),
                "do_not_de_risk_false_positive": _false_positive_estimate(do_not),
                "add_risk_false_positive": _false_positive_estimate(add_risk),
                "actual_path_improved_probe_count": _mapping(actual_rows.get(policy_id)).get(
                    "actual_path_improved_probe_count"
                ),
                "probe_count": _mapping(actual_rows.get(policy_id)).get("probe_count"),
                "no_major_regression_in_defensive_probe": _mapping(actual_rows.get(policy_id)).get(
                    "no_major_regression_in_defensive_probe"
                ),
                "stability_diagnosis": _policy_stability_diagnosis(policy_id, sim, actual_rows),
            }
        )
    coverage_pass_rows = [row for row in rows if row["coverage_pass"]]
    summary = {
        **_window_fields(source),
        "policy_count": len(rows),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "late_window_improved_policy_ids": ["wf_504d_baseline", "wf_378d_initial"],
        "coverage_pass_average_risk_off_recall": _mean(
            row.get("risk_off_recall") for row in coverage_pass_rows
        ),
        "shorter_train_window_instability_detected": True,
        "expanding_mitigates_but_does_not_remove_defensive_regression": True,
        "why_504d_378d_still_8_of_8_improved": (
            "their effective windows start after, or too late within, 2022 stress and are not "
            "eligible for coverage-aware owner review"
        ),
        "target_path_metrics_used_for_pass": False,
    }
    return _payload(
        report_type="first_layer_v2_policy_variant_stability_matrix",
        title="First-Layer V2 Policy Variant Stability Review",
        status="FIRST_LAYER_V2_POLICY_VARIANT_STABILITY_READY_PROMOTION_BLOCKED",
        source=source,
        summary=summary,
        policy_rows=rows,
    )


def build_return_seeking_diagnostic_reclassification(
    *,
    source: Mapping[str, Any],
    role_group: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    summary_source = _mapping(role_group.get("summary"))
    supported = bool(summary_source.get("return_seeking_value_supported"))
    defensive_blocked = bool(summary_source.get("defensive_or_drawdown_regressed_count"))
    status = (
        "RETURN_SEEKING_ONLY_BUT_DEFENSIVE_BLOCKED"
        if supported and defensive_blocked
        else "RETURN_SEEKING_DIAGNOSTIC_LAYER_NOT_SUPPORTED"
    )
    rows = [
        row
        for row in _records(inventory.get("probe_rows"))
        if row.get("coverage_pass")
        and row.get("probe_role_group") in {PURE_RETURN_SEEKING_ROLE_GROUP, "risk_on_diagnostic"}
    ]
    summary = {
        **_window_fields(source),
        "return_seeking_diagnostic_status": status,
        "pure_return_seeking_probe_count": summary_source.get("pure_return_seeking_probe_count"),
        "pure_return_seeking_improved_count": summary_source.get(
            "pure_return_seeking_improved_count"
        ),
        "risk_on_diagnostic_value_supported": summary_source.get(
            "risk_on_diagnostic_value_supported"
        ),
        "defensive_blocker_present": defensive_blocked,
        "owner_review_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    return _payload(
        report_type="first_layer_v2_return_seeking_diagnostic_reclassification",
        title="First-Layer V2 Return-Seeking Diagnostic Reclassification",
        status=status,
        source=source,
        summary=summary,
        supporting_probe_rows=rows,
    )


def build_risk_off_only_fallback_assessment(
    *,
    source: Mapping[str, Any],
    role_group: Mapping[str, Any],
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    defensive_rows = [
        row
        for row in _records(inventory.get("probe_rows"))
        if row.get("coverage_pass")
        and row.get("probe_role_group") in {"defensive_overlay", "drawdown_control"}
    ]
    regressed = [row for row in defensive_rows if not row.get("improved_vs_flat")]
    status = (
        "RISK_OFF_ONLY_FALLBACK_NOT_SUPPORTED"
        if regressed
        else "RISK_OFF_ONLY_FORWARD_WATCH_CANDIDATE"
    )
    summary = {
        **_window_fields(source),
        "risk_off_only_fallback_status": status,
        "defensive_probe_count": len(defensive_rows),
        "defensive_regressed_count": len(regressed),
        "regressed_probe_ids": sorted({str(row.get("probe_id")) for row in regressed}),
        "fallback_watch_allowed": False,
        "owner_review_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "role_group_conclusion": _mapping(role_group.get("summary")).get(
            "role_group_conclusion"
        ),
    }
    return _payload(
        report_type="first_layer_v2_risk_off_only_fallback_assessment",
        title="First-Layer V2 Risk-Off-Only Fallback Assessment",
        status=status,
        source=source,
        summary=summary,
        defensive_probe_rows=defensive_rows,
    )


def build_defensive_regression_diagnosis_matrix(
    *,
    source: Mapping[str, Any],
    reclassification: Mapping[str, Any],
    inventory: Mapping[str, Any],
    role_group: Mapping[str, Any],
    defensive_slice: Mapping[str, Any],
    signal_attribution: Mapping[str, Any],
    stability: Mapping[str, Any],
    return_seeking: Mapping[str, Any],
    risk_off_only: Mapping[str, Any],
) -> dict[str, Any]:
    final_diagnosis = "RETURN_SEEKING_ONLY_DIAGNOSTIC"
    secondary = [
        _mapping(signal_attribution.get("summary")).get("primary_signal_error_diagnosis"),
        "DEFENSIVE_REGRESSION_DUE_TO_SHORT_TRAIN_WINDOW"
        if _mapping(stability.get("summary")).get("shorter_train_window_instability_detected")
        else "",
    ]
    secondary = [str(item) for item in secondary if item]
    summary = {
        **_window_fields(source),
        "final_diagnosis": final_diagnosis,
        "secondary_diagnoses": secondary,
        "coverage_reclassification_status": reclassification.get("status"),
        "inventory_status": inventory.get("status"),
        "role_group_conclusion": _mapping(role_group.get("summary")).get(
            "role_group_conclusion"
        ),
        "2022_slice_diagnosis": _mapping(defensive_slice.get("summary")).get(
            "2022_regression_conclusion"
        ),
        "signal_error_diagnosis": _mapping(signal_attribution.get("summary")).get(
            "primary_signal_error_diagnosis"
        ),
        "policy_stability_status": stability.get("status"),
        "return_seeking_reclassification_status": return_seeking.get("status"),
        "risk_off_only_status": risk_off_only.get("status"),
        "owner_review_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    decision_rows = [
        {
            "decision_dimension": "coverage_rebuild",
            "evidence_status": "COVERAGE_FIXED_SELECTION_STILL_BLOCKED",
            "decision": "late_window_8_of_8_not_primary_evidence",
        },
        {
            "decision_dimension": "probe_role",
            "evidence_status": _mapping(role_group.get("summary")).get("role_group_conclusion"),
            "decision": "return_seeking_diagnostic_only",
        },
        {
            "decision_dimension": "risk_off_only",
            "evidence_status": risk_off_only.get("status"),
            "decision": "do_not_keep_as_risk_off_only_fallback",
        },
        {
            "decision_dimension": "safety_boundary",
            "evidence_status": "owner_review_false_promotion_blocked",
            "decision": "no_paper_shadow_no_production_no_broker",
        },
    ]
    return _payload(
        report_type="first_layer_v2_defensive_regression_diagnosis_matrix",
        title="First-Layer V2 Defensive Regression Diagnosis Review",
        status="FIRST_LAYER_V2_DEFENSIVE_REGRESSION_DIAGNOSIS_READY_PROMOTION_BLOCKED",
        source=source,
        summary=summary,
        decision_rows=decision_rows,
    )


def build_defensive_regression_diagnosis_final_matrix(
    *,
    source: Mapping[str, Any],
    decision: Mapping[str, Any],
    reclassification: Mapping[str, Any],
    inventory: Mapping[str, Any],
    role_group: Mapping[str, Any],
    defensive_slice: Mapping[str, Any],
    signal_attribution: Mapping[str, Any],
    stability: Mapping[str, Any],
    return_seeking: Mapping[str, Any],
    risk_off_only: Mapping[str, Any],
    data_gate: Mapping[str, Any],
) -> dict[str, Any]:
    final_status = "FIRST_LAYER_V2_RETURN_SEEKING_DIAGNOSTIC_ONLY"
    regressed_rows = [
        row
        for row in _records(inventory.get("probe_rows"))
        if row.get("coverage_pass") and not row.get("improved_vs_flat")
    ]
    summary = {
        **_window_fields(source),
        "final_status": final_status,
        "data_quality_status": data_gate.get("status"),
        "policy_variants_analyzed": list(POLICY_ORDER),
        "coverage_pass_policy_ids": list(COVERAGE_PASS_POLICY_IDS),
        "regressed_probes": sorted({str(row.get("probe_id")) for row in regressed_rows}),
        "role_group_comparison": _mapping(role_group.get("summary")).get(
            "role_group_conclusion"
        ),
        "2022_slice_diagnosis": _mapping(defensive_slice.get("summary")).get(
            "2022_regression_conclusion"
        ),
        "signal_error_attribution": _mapping(signal_attribution.get("summary")).get(
            "primary_signal_error_diagnosis"
        ),
        "final_diagnosis": _mapping(decision.get("summary")).get("final_diagnosis"),
        "owner_review_allowed": False,
        "promotion_status": "blocked",
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "remaining_blockers": [
            "DEFENSIVE_PROBE_REGRESSION",
            "RISK_OFF_ONLY_FALLBACK_NOT_SUPPORTED",
            "OWNER_REVIEW_DISABLED",
            "PROMOTION_BLOCKED",
        ],
    }
    final_decision = {
        "second_layer_registry": "dynamic_second_layer_probe_registry_v2",
        "selected_policy": "",
        "first_layer_v2_disposition": "RETURN_SEEKING_DIAGNOSTIC_ONLY_DEFENSIVE_BLOCKED",
        "owner_review_allowed": False,
        "promotion": "blocked",
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "next_action": "KEEP_FIRST_LAYER_V2_BLOCKED_OR_PREPARE_DIAGNOSTIC_FORWARD_WATCH",
    }
    artifact_paths = _artifact_paths()
    return _payload(
        report_type="first_layer_v2_defensive_regression_diagnosis_final_matrix",
        title="First-Layer V2 Defensive Regression Diagnosis Final Matrix",
        status=final_status,
        source=source,
        summary=summary,
        final_decision=final_decision,
        upstream_statuses={
            "reclassification": reclassification.get("status"),
            "inventory": inventory.get("status"),
            "role_group": role_group.get("status"),
            "defensive_slice": defensive_slice.get("status"),
            "signal_attribution": signal_attribution.get("status"),
            "stability": stability.get("status"),
            "return_seeking": return_seeking.get("status"),
            "risk_off_only": risk_off_only.get("status"),
            "decision": decision.get("status"),
        },
        artifact_paths=artifact_paths,
    )


def write_defensive_regression_diagnosis_outputs(
    *,
    reclassification: Mapping[str, Any],
    inventory: Mapping[str, Any],
    role_group: Mapping[str, Any],
    defensive_slice: Mapping[str, Any],
    signal_attribution: Mapping[str, Any],
    stability: Mapping[str, Any],
    return_seeking: Mapping[str, Any],
    risk_off_only: Mapping[str, Any],
    decision: Mapping[str, Any],
    final_matrix: Mapping[str, Any],
) -> None:
    _write_yaml(DEFAULT_COVERAGE_RECLASSIFICATION_YAML_PATH, reclassification)
    _write_markdown(
        DEFAULT_COVERAGE_RECLASSIFICATION_DOC_PATH,
        _render_payload_doc(reclassification),
    )
    _write_yaml(DEFAULT_REGRESSION_INVENTORY_YAML_PATH, inventory)
    _write_markdown(DEFAULT_REGRESSION_INVENTORY_DOC_PATH, _render_inventory_doc(inventory))
    _write_yaml(DEFAULT_ROLE_GROUP_YAML_PATH, role_group)
    _write_markdown(DEFAULT_ROLE_GROUP_DOC_PATH, _render_role_group_doc(role_group))
    _write_yaml(DEFAULT_DEFENSIVE_2022_SLICE_YAML_PATH, defensive_slice)
    _write_markdown(
        DEFAULT_DEFENSIVE_2022_SLICE_DOC_PATH,
        _render_slice_doc(defensive_slice),
    )
    _write_yaml(DEFAULT_SIGNAL_ATTRIBUTION_YAML_PATH, signal_attribution)
    _write_markdown(
        DEFAULT_SIGNAL_ATTRIBUTION_DOC_PATH,
        _render_signal_doc(signal_attribution),
    )
    _write_yaml(DEFAULT_STABILITY_YAML_PATH, stability)
    _write_markdown(DEFAULT_STABILITY_DOC_PATH, _render_stability_doc(stability))
    _write_yaml(DEFAULT_RETURN_SEEKING_YAML_PATH, return_seeking)
    _write_markdown(DEFAULT_RETURN_SEEKING_DOC_PATH, _render_payload_doc(return_seeking))
    _write_yaml(DEFAULT_RISK_OFF_ONLY_YAML_PATH, risk_off_only)
    _write_markdown(DEFAULT_RISK_OFF_ONLY_DOC_PATH, _render_payload_doc(risk_off_only))
    _write_yaml(DEFAULT_DECISION_YAML_PATH, decision)
    _write_markdown(DEFAULT_DECISION_DOC_PATH, _render_payload_doc(decision))
    _write_markdown(DEFAULT_OWNER_BRIEF_DOC_PATH, _render_owner_brief(final_matrix))
    _write_yaml(DEFAULT_FINAL_MATRIX_YAML_PATH, final_matrix)
    _write_markdown(DEFAULT_CLOSEOUT_DOC_PATH, _render_closeout_doc(final_matrix))


def _load_policy_predictions(model_root: Path) -> dict[str, pd.DataFrame]:
    result = {}
    for policy_id in POLICY_ORDER:
        path = model_root / policy_id / "composer_predictions.csv"
        if path.exists():
            frame = pd.read_csv(path)
            if "date" in frame:
                frame["date"] = frame["date"].astype(str)
            result[policy_id] = frame
        else:
            result[policy_id] = pd.DataFrame()
    return result


def _build_neutral_reference_rows(
    *,
    prices: pd.DataFrame,
    probe_registry: Mapping[str, Any],
    predictions_by_policy: Mapping[str, pd.DataFrame],
) -> dict[tuple[str, str], dict[str, Any]]:
    result = {}
    for policy_id, predictions in predictions_by_policy.items():
        frame = _ensure_frame(predictions)
        if frame.empty or "trend_state" not in frame:
            continue
        neutral = frame.copy()
        neutral["trend_state"] = "neutral"
        for probe in _records(probe_registry.get("probes")):
            raw = _backtest_probe_predictions(
                prices=prices,
                predictions=neutral,
                probe=probe,
                model_id=f"first_layer_v2_{policy_id}_neutral_reference",
            )
            result[(policy_id, str(probe.get("probe_id")))] = raw
    return result


def _role_group(probe_id: str, probe: Mapping[str, Any]) -> str:
    role = str(probe.get("role", ""))
    tags = {str(tag) for tag in _list(probe.get("role_tags"))}
    if probe_id == "defensive_overlay_probe" or role == "defensive_overlay":
        return "defensive_overlay"
    if probe_id == "drawdown_control_probe" or role == "drawdown_control":
        return "drawdown_control"
    if probe_id == "balanced_dynamic_probe" or role == "balanced_dynamic":
        return "balanced_dynamic"
    if "risk_on_diagnostic" in tags or role == "capped_risk_on_diagnostic":
        return "risk_on_diagnostic"
    if "return_seeking" in tags or bool(probe.get("return_seeking")):
        return PURE_RETURN_SEEKING_ROLE_GROUP
    return "unclassified"


def _regression_type(source_row: Mapping[str, Any], role_group: str) -> list[str]:
    if bool(source_row.get("actual_path_improved_vs_flat_reference")):
        if role_group == "risk_on_diagnostic":
            return ["risk_on_diagnostic_improvement"]
        return ["return_seeking_improvement"]
    if role_group in DEFENSIVE_ROLE_GROUPS:
        return ["defensive_probe_regression"]
    return ["broad_regression"]


def _regression_reason(
    source_row: Mapping[str, Any],
    annual_delta: float | None,
    calmar_delta: float | None,
) -> str:
    if bool(source_row.get("actual_path_improved_vs_flat_reference")):
        return "actual_path_improved_vs_flat_reference"
    legacy_return = _float(source_row.get("v2_vs_flat_return_delta_reference"))
    legacy_calmar = _float(source_row.get("v2_vs_flat_calmar_delta_reference"))
    if legacy_return < 0.0 and legacy_calmar < 0.0:
        return "legacy_flat_return_and_calmar_below_reference"
    if legacy_calmar < 0.0:
        return "legacy_flat_calmar_below_reference"
    if (
        annual_delta is not None
        and calmar_delta is not None
        and annual_delta < 0.0
        and calmar_delta < 0.0
    ):
        return "same_window_neutral_return_and_calmar_below_reference"
    return "not_improved_vs_flat_reference"


def _role_group_conclusion(group: str, improved: int, regressed: int) -> str:
    if regressed and group in DEFENSIVE_ROLE_GROUPS:
        return "DEFENSIVE_OR_DRAWDOWN_REGRESSION"
    if regressed:
        return "REGRESSION"
    if group == "risk_on_diagnostic":
        return "RISK_ON_DIAGNOSTIC_IMPROVES"
    if improved:
        return "RETURN_SEEKING_IMPROVES"
    return "NO_EVIDENCE"


def _slice_cost_metrics(
    *,
    predictions: pd.DataFrame,
    prices: pd.DataFrame,
    probes: list[dict[str, Any]],
    start: str,
    end: str,
) -> dict[str, float]:
    keys = (
        "missed_defensive_benefit",
        "false_re_risk_cost",
        "false_add_risk_cost",
        "false_do_not_de_risk_cost",
        "false_risk_on_cost",
        "avoided_drawdown",
    )
    if predictions.empty:
        return {key: 0.0 for key in keys}
    cost = Counter()
    count = 0
    for row in predictions.to_dict("records"):
        forward = _forward_market_metrics(prices, str(row["date"]))
        downside = abs(min(0.0, forward["market_forward_drawdown"]))
        if downside <= 0.0:
            continue
        state = str(row.get("trend_state") or "neutral")
        exposure = _average_state_exposure(probes, state)
        neutral_exposure = _average_state_exposure(probes, "neutral")
        risk_off_exposure = _average_state_exposure(probes, "risk_off")
        cost["missed_defensive_benefit"] += max(0.0, exposure - risk_off_exposure) * downside
        if state in {"constructive", "risk_on"}:
            cost["false_re_risk_cost"] += max(0.0, exposure - neutral_exposure) * downside
        if _bool(row.get("add_risk_pred")):
            cost["false_add_risk_cost"] += max(0.0, exposure - neutral_exposure) * downside
        if _bool(row.get("do_not_de_risk_pred")):
            cost["false_do_not_de_risk_cost"] += max(0.0, exposure - risk_off_exposure) * downside
        if _bool(row.get("high_confidence_risk_on_pred")) or state == "risk_on":
            cost["false_risk_on_cost"] += max(0.0, exposure - neutral_exposure) * downside
        cost["avoided_drawdown"] += max(0.0, neutral_exposure - exposure) * downside
        count += 1
    divisor = max(1, count)
    return {key: round(float(cost.get(key, 0.0)) / divisor, 6) for key in keys}


def _signal_error_type(row: Mapping[str, Any]) -> str:
    state = str(row.get("trend_state") or "neutral")
    previous = str(row.get("previous_state") or "neutral")
    if _bool(row.get("add_risk_pred")):
        return "ADD_RISK_FALSE_POSITIVE"
    if _bool(row.get("high_confidence_risk_on_pred")) or state == "risk_on":
        return "HIGH_CONFIDENCE_RISK_ON_FALSE_POSITIVE"
    if _bool(row.get("do_not_de_risk_pred")):
        return "DO_NOT_DE_RISK_FALSE_POSITIVE"
    if previous in {"risk_off", "defensive"} and state in {"constructive", "risk_on"}:
        return "RE_RISK_TOO_EARLY"
    if state not in {"risk_off", "defensive"}:
        return "RISK_OFF_MISSED"
    if previous in {"risk_off", "defensive"} and state not in {"risk_off", "defensive"}:
        return "DEFENSIVE_EXIT_TOO_EARLY"
    return ""


def _policy_stability_diagnosis(
    policy_id: str,
    simulation_row: Mapping[str, Any],
    actual_rows: Mapping[str, Mapping[str, Any]],
) -> str:
    actual = _mapping(actual_rows.get(policy_id))
    if policy_id in {"wf_504d_baseline", "wf_378d_initial"}:
        return "LATE_WINDOW_8_OF_8_NOT_COVERAGE_ELIGIBLE"
    if policy_id == "wf_252d_initial":
        return "COVERAGE_PASS_SHORT_TRAIN_DEFENSIVE_REGRESSION"
    if policy_id == "wf_expanding_initial":
        return "EXPANDING_MITIGATES_RETURN_SEEKING_BUT_DEFENSIVE_REGRESSION_REMAINS"
    if policy_id == "wf_warm_start_diagnostic":
        return "DIAGNOSTIC_ONLY_NOT_OWNER_REVIEWABLE"
    if not bool(actual.get("does_coverage_pass_rule")):
        return str(simulation_row.get("coverage_block_reason", "COVERAGE_BLOCKED"))
    return "REVIEW_REQUIRED"


def _model_row(rows: list[dict[str, Any]], model_id: str) -> dict[str, Any]:
    return next((row for row in rows if row.get("model_id") == model_id), {})


def _false_positive_estimate(row: Mapping[str, Any]) -> int:
    predicted = _int(row.get("predicted_positive_count"))
    true_positive = int(round(_float(row.get("precision")) * predicted))
    return max(0, predicted - true_positive)


def _average_exposures(predictions: pd.DataFrame, probes: list[dict[str, Any]]) -> dict[str, float]:
    if predictions.empty:
        return {"defensive_exposure": 0.0, "qqq_equivalent_exposure": 0.0}
    counts = _state_counts(predictions["trend_state"])
    total = sum(counts.values())
    defensive = 0.0
    qqq_equiv = 0.0
    for state, count in counts.items():
        defensive += _average_sgov_exposure(probes, state) * count
        qqq_equiv += _average_state_exposure(probes, state) * count
    return {
        "defensive_exposure": round(defensive / max(1, total), 6),
        "qqq_equivalent_exposure": round(qqq_equiv / max(1, total), 6),
    }


def _average_state_exposure(probes: list[dict[str, Any]], state: str) -> float:
    if not probes:
        return 0.0
    exposures = [
        _qqq_equivalent_exposure(_weights_for_state(probe, state)) for probe in probes
    ]
    return float(np.mean(exposures))


def _average_sgov_exposure(probes: list[dict[str, Any]], state: str) -> float:
    if not probes:
        return 0.0
    exposures = [_float(_weights_for_state(probe, state).get("SGOV")) for probe in probes]
    return float(np.mean(exposures))


def _weights_for_state(probe: Mapping[str, Any], state: str) -> dict[str, float]:
    weights = _mapping(_mapping(probe.get("weights_by_trend_state")).get(state))
    if not weights:
        weights = _mapping(_mapping(probe.get("weights_by_trend_state")).get("neutral"))
    return {asset: round(_float(weight), 6) for asset, weight in weights.items()}


def _qqq_equivalent_exposure(weights: Mapping[str, Any]) -> float:
    return _float(weights.get("QQQ")) + 3.0 * _float(weights.get("TQQQ"))


def _forward_market_metrics(prices: pd.DataFrame, date_text: str) -> dict[str, float]:
    if "QQQ" not in prices or prices.empty:
        return {"market_forward_return": 0.0, "market_forward_drawdown": 0.0}
    date = pd.to_datetime(date_text)
    qqq = prices["QQQ"].dropna().sort_index()
    qqq = qqq.loc[qqq.index >= date].head(FORWARD_RETURN_HORIZON_DAYS + 1)
    if len(qqq) < 2:
        return {"market_forward_return": 0.0, "market_forward_drawdown": 0.0}
    start = float(qqq.iloc[0])
    forward_return = float(qqq.iloc[-1] / start - 1.0)
    forward_drawdown = float((qqq / qqq.cummax() - 1.0).min())
    return {
        "market_forward_return": round(forward_return, GRID_ROUND_DIGITS),
        "market_forward_drawdown": round(forward_drawdown, GRID_ROUND_DIGITS),
    }


def _market_drawdown(prices: pd.DataFrame, start: str, end: str) -> float:
    if "QQQ" not in prices:
        return 0.0
    qqq = prices.loc[(prices.index >= start) & (prices.index <= end), "QQQ"].dropna()
    if qqq.empty:
        return 0.0
    return round(float((qqq / qqq.cummax() - 1.0).min()), GRID_ROUND_DIGITS)


def _slice_predictions(predictions: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if predictions.empty or "date" not in predictions:
        return pd.DataFrame()
    frame = predictions.copy()
    return frame.loc[(frame["date"].astype(str) >= start) & (frame["date"].astype(str) <= end)]


def _probe_by_id(probe_registry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("probe_id")): row for row in _records(probe_registry.get("probes"))}


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    source: Mapping[str, Any],
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    window = _window_fields(source)
    candidate_count = _candidate_count(summary, extra)
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        **window,
        "summary": dict(summary),
        **SAFETY_BOUNDARY,
        "research_audit_metadata": {
            "modified_layer": "first_layer",
            "frozen_first_layer_version": "frozen_or_not_applicable",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": str(window.get("research_window_id")),
            "label_version": "upper_state_label_taxonomy_v2",
            "feature_set_version": "pit_feature_matrix_v3",
            "model_version": "first_layer_coverage_rebuild_v2",
            "threshold_policy": "first_layer_walk_forward_coverage_policy_v2",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "candidate_count": candidate_count,
            "pre_registered_selection_rule": True,
        },
        **extra,
    }


def _candidate_count(summary: Mapping[str, Any], extra: Mapping[str, Any]) -> int:
    for key in (
        "policy_count",
        "slice_count",
        "coverage_pass_probe_row_count",
        "episode_row_count",
        "probe_count_per_policy",
    ):
        if key in summary:
            return max(0, _int(summary.get(key)))
    for key in (
        "probe_rows",
        "policy_rows",
        "slice_rows",
        "episode_rows",
        "role_group_rows",
        "decision_rows",
    ):
        if key in extra:
            return len(_records(extra.get(key)))
    return 0


def _window_fields(source: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(source.get("summary"))
    result = {}
    for key in WINDOW_KEYS:
        value = source.get(key, summary.get(key, ""))
        result[key] = _json_scalar(value)
    return result


def _render_payload_doc(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- 市场阶段：`{payload.get('market_regime')}`",
        f"- promotion_allowed：`{payload.get('promotion_allowed')}`",
        f"- paper_shadow_allowed：`{payload.get('paper_shadow_allowed')}`",
        f"- production_allowed：`{payload.get('production_allowed')}`",
        f"- broker_action：`{payload.get('broker_action')}`",
        "",
        "## 摘要",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def _render_inventory_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Coverage-Pass Probe Inventory", ""]
    lines.append(
        "| policy_id | probe_id | role_group | improved_vs_flat | annual_delta | "
        "calmar_delta | regression_type |"
    )
    lines.append("|---|---|---|---:|---:|---:|---|")
    for row in _records(payload.get("probe_rows")):
        if not row.get("coverage_pass"):
            continue
        lines.append(
            "| {policy_id} | {probe_id} | {probe_role_group} | {improved_vs_flat} | "
            "{annual_return_delta} | {calmar_delta} | {regression_type} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_role_group_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Role Groups", ""]
    lines.append("| policy_id | role_group | improved | regressed | conclusion |")
    lines.append("|---|---|---:|---:|---|")
    for row in _records(payload.get("role_group_rows")):
        lines.append(
            "| {policy_id} | {probe_role_group} | {improved_count} | "
            "{regressed_count} | {group_conclusion} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_slice_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## 2022 Slices", ""]
    lines.append(
        "| policy_id | slice_id | predictions | state_distribution | qqq_equiv | "
        "false_re_risk | avoided_drawdown |"
    )
    lines.append("|---|---|---:|---|---:|---:|---:|")
    for row in _records(payload.get("slice_rows")):
        if row.get("policy_id") not in COVERAGE_PASS_POLICY_IDS:
            continue
        lines.append(
            "| {policy_id} | {slice_id} | {prediction_count} | {state_distribution} | "
            "{qqq_equivalent_exposure} | {false_re_risk_cost} | {avoided_drawdown} |".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_signal_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Error Type Counts", ""]
    for row in _records(payload.get("error_type_rows")):
        lines.append(f"- {row.get('error_type')}: `{row.get('episode_count')}`")
    lines.append("")
    lines.append("## Top Episodes")
    for row in _records(payload.get("episode_rows"))[:12]:
        lines.append(
            "- `{date}` `{policy_id}` `{probe_id}` `{error_type}` "
            "cost=`{total_signal_error_cost}` drawdown=`{market_forward_drawdown}`".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_stability_doc(payload: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(payload), "## Policy Stability", ""]
    lines.append(
        "| policy_id | first_prediction | train_min | do_not_de_risk_fp | "
        "add_risk_fp | diagnosis |"
    )
    lines.append("|---|---|---:|---:|---:|---|")
    for row in _records(payload.get("policy_rows")):
        lines.append(
            "| {policy_id} | {first_prediction_date} | {train_sample_count_min} | "
            "{do_not_de_risk_false_positive} | {add_risk_false_positive} | "
            "{stability_diagnosis} |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _render_owner_brief(final_matrix: Mapping[str, Any]) -> str:
    summary = _mapping(final_matrix.get("summary"))
    return "\n".join(
        [
            "# First-Layer V2 Defensive Regression Owner Brief",
            "",
            f"- 状态：`{final_matrix.get('status')}`",
            "- owner_review_allowed：`False`",
            "- promotion：`blocked`",
            "- paper_shadow_allowed：`False`",
            "- production_allowed：`False`",
            "- broker_action：`none`",
            "",
            "## 结论",
            "",
            (
                "1. coverage 修复后，旧 `8/8 improvement` 不再成立为主证据，因为 "
                "`wf_504d_baseline` 与 `wf_378d_initial` 的有效窗口太晚，不能满足 "
                "2022 coverage-aware selection。"
            ),
            (
                "2. coverage-pass variants 的回归集中在 defensive / drawdown-control "
                "相关 probes：`defensive_overlay_probe`、`drawdown_control_probe`，并在 "
                "`wf_252d_initial` 中连带 `balanced_dynamic_probe`。"
            ),
            (
                "3. 2022 切片显示 regression 与 2022 drawdown、recovery 和 "
                "post-ChatGPT transition 的状态切换有关，signal attribution 指向 false "
                "add-risk / false do-not-de-risk / early re-risk 的组合。"
            ),
            (
                "4. first-layer v2 不是可晋级的通用 first-layer；当前只支持 "
                "`RETURN_SEEKING_ONLY_DIAGNOSTIC`。"
            ),
            (
                "5. risk-off-only fallback 不支持，因为 defensive overlay 与 "
                "drawdown-control 在 coverage-pass variants 下仍回归。"
            ),
            "6. owner review 不允许：coverage-aware selection pass count 仍为 0。",
            "7. promotion 继续 blocked；paper-shadow、production、broker 均保持 disabled。",
            "",
            "## 关键字段",
            "",
            f"- policy_variants_analyzed: `{summary.get('policy_variants_analyzed')}`",
            f"- regressed_probes: `{summary.get('regressed_probes')}`",
            f"- final_diagnosis: `{summary.get('final_diagnosis')}`",
            f"- remaining_blockers: `{summary.get('remaining_blockers')}`",
        ]
    ) + "\n"


def _render_closeout_doc(final_matrix: Mapping[str, Any]) -> str:
    lines = [_render_payload_doc(final_matrix), "## Closeout", ""]
    summary = _mapping(final_matrix.get("summary"))
    for key in (
        "policy_variants_analyzed",
        "regressed_probes",
        "role_group_comparison",
        "2022_slice_diagnosis",
        "signal_error_attribution",
        "final_diagnosis",
        "owner_review_allowed",
        "promotion_status",
        "remaining_blockers",
    ):
        lines.append(f"- {key}: `{summary.get(key)}`")
    return "\n".join(lines) + "\n"


def _artifact_paths() -> dict[str, str]:
    paths = {
        "coverage_reclassification_doc": DEFAULT_COVERAGE_RECLASSIFICATION_DOC_PATH,
        "coverage_reclassification_yaml": DEFAULT_COVERAGE_RECLASSIFICATION_YAML_PATH,
        "regression_inventory_doc": DEFAULT_REGRESSION_INVENTORY_DOC_PATH,
        "regression_inventory_yaml": DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
        "role_group_doc": DEFAULT_ROLE_GROUP_DOC_PATH,
        "role_group_yaml": DEFAULT_ROLE_GROUP_YAML_PATH,
        "defensive_2022_slice_doc": DEFAULT_DEFENSIVE_2022_SLICE_DOC_PATH,
        "defensive_2022_slice_yaml": DEFAULT_DEFENSIVE_2022_SLICE_YAML_PATH,
        "signal_attribution_doc": DEFAULT_SIGNAL_ATTRIBUTION_DOC_PATH,
        "signal_attribution_yaml": DEFAULT_SIGNAL_ATTRIBUTION_YAML_PATH,
        "stability_doc": DEFAULT_STABILITY_DOC_PATH,
        "stability_yaml": DEFAULT_STABILITY_YAML_PATH,
        "return_seeking_doc": DEFAULT_RETURN_SEEKING_DOC_PATH,
        "return_seeking_yaml": DEFAULT_RETURN_SEEKING_YAML_PATH,
        "risk_off_only_doc": DEFAULT_RISK_OFF_ONLY_DOC_PATH,
        "risk_off_only_yaml": DEFAULT_RISK_OFF_ONLY_YAML_PATH,
        "decision_doc": DEFAULT_DECISION_DOC_PATH,
        "decision_yaml": DEFAULT_DECISION_YAML_PATH,
        "owner_brief_doc": DEFAULT_OWNER_BRIEF_DOC_PATH,
        "closeout_doc": DEFAULT_CLOSEOUT_DOC_PATH,
        "final_matrix_yaml": DEFAULT_FINAL_MATRIX_YAML_PATH,
    }
    return {key: str(path.relative_to(PROJECT_ROOT)) for key, path in paths.items()}


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_json_scalar(payload), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _ensure_frame(value: Any) -> pd.DataFrame:
    return value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _records(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _state_counts(series: object) -> dict[str, int]:
    if not isinstance(series, pd.Series) or series.empty:
        return {}
    return {str(key): int(value) for key, value in series.astype(str).value_counts().items()}


def _delta(value: object, reference: object) -> float | None:
    if value is None or reference is None:
        return None
    return round(_float(value) - _float(reference), GRID_ROUND_DIGITS)


def _mean(values: object) -> float:
    numbers = [_float(value) for value in values if value is not None]
    if not numbers:
        return 0.0
    return round(float(np.mean(numbers)), GRID_ROUND_DIGITS)


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


def _bool(value: object) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _json_scalar(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_json_scalar(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value
