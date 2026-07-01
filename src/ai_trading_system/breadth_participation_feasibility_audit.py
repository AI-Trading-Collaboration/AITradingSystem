from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    write_csv_rows,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "breadth_participation_candidate_family_feasibility_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2302_BREADTH_PARTICIPATION_CANDIDATE_FAMILY_DATA_FEASIBILITY_AUDIT"
TASK_REGISTER_ID = "TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC"
REPORT_TYPE = "breadth_participation_candidate_family_feasibility_audit"
MODE = "feasibility_audit"
STATUS = "BREADTH_FEASIBILITY_AUDIT_READY_PROXY_ONLY"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"
ARTIFACT_ROLE = "breadth_participation_feasibility_audit"

PIT_STATUSES = {
    "STRICT_PIT_READY",
    "PIT_APPROXIMATION_READY",
    "CURRENT_CONSTITUENTS_PROXY_ONLY",
    "FORWARD_OBSERVE_ONLY",
    "BLOCKED_NO_RELIABLE_DATA",
}
BIAS_RISKS = {
    "LOW_BIAS",
    "MODERATE_BIAS",
    "HIGH_SURVIVORSHIP_BIAS",
    "HIGH_LOOKAHEAD_BIAS",
    "UNACCEPTABLE_FOR_VALIDATION",
}
USAGE_ROLES = {
    "confirmation_only",
    "trend_fragility_warning",
    "risk_cap_modifier",
    "diagnostic_only",
}
HORIZONS = ("5d", "10d", "20d")
PRIORITY_HORIZONS = ("10d", "20d")

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "generator_implemented": False,
    "actual_path_validation_executed": False,
    "candidate_artifact_generated": False,
    "candidate_signal_series_generated": False,
    "prediction_artifact_generated": False,
    "owner_review_required": False,
}

SOURCE_EVIDENCE_PATHS = {
    "true_breadth_contract": "config/research/true_breadth_data_contract.yaml",
    "participation_proxy_registry": "config/research/participation_proxy_free_registry.yaml",
    "paid_breadth_vendor_registry": "config/data/paid_breadth_data_vendor_registry.yaml",
    "fmp_holdings_gate": "inputs/research_reviews/fmp_etf_holdings_trial_gate.yaml",
    "norgate_trial_summary": "inputs/research_reviews/norgate_breadth_prototype_2y.yaml",
    "equal_weight_proxy_fix": "docs/research/equal_weight_proxy_data_fix_report.md",
}


class BreadthParticipationFeasibilityAuditError(ValueError):
    pass


def run_breadth_participation_feasibility_audit(
    *,
    target_etfs: str | Sequence[str],
    target_assets: str | Sequence[str],
    candidate_family: str,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    request = _validated_request(
        target_etfs=target_etfs,
        target_assets=target_assets,
        candidate_family=candidate_family,
        mode=mode,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_breadth_participation_feasibility_artifacts(
        target_etfs=request["target_etfs"],
        target_assets=request["target_assets"],
        candidate_family=request["candidate_family"],
        generated_at=generated_at,
    )
    artifact_paths = write_breadth_participation_feasibility_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def build_breadth_participation_feasibility_artifacts(
    *,
    target_etfs: Sequence[str],
    target_assets: Sequence[str],
    candidate_family: str,
    generated_at: datetime,
) -> dict[str, Any]:
    common = _common_payload(
        target_etfs=target_etfs,
        target_assets=target_assets,
        candidate_family=candidate_family,
        generated_at=generated_at,
    )
    inventory_rows = build_breadth_input_data_inventory(target_etfs=target_etfs)
    pit_gap_rows = build_historical_constituent_pit_gap_matrix(target_etfs=target_etfs)
    proxy_risk_rows = build_current_constituents_proxy_risk_matrix(target_etfs=target_etfs)
    design_sketch = build_candidate_family_design_sketch(
        target_etfs=target_etfs,
        target_assets=target_assets,
    )
    signal_rows = build_candidate_signal_concept_matrix(target_assets=target_assets)
    validation_rows = build_candidate_validation_route_matrix()
    recommendation_rows = build_data_feasibility_recommendation_matrix()
    task_route = build_2303_task_route(
        strict_pit_feasibility=False,
        pit_approximation_feasibility=False,
        current_constituents_proxy_feasibility=True,
    )
    safety_boundary = build_breadth_safety_boundary()
    summary = build_feasibility_summary(
        common=common,
        inventory_rows=inventory_rows,
        pit_gap_rows=pit_gap_rows,
        proxy_risk_rows=proxy_risk_rows,
        task_route=task_route,
    )
    docs = build_research_docs(
        summary=summary,
        inventory_rows=inventory_rows,
        pit_gap_rows=pit_gap_rows,
        proxy_risk_rows=proxy_risk_rows,
        design_sketch=design_sketch,
        signal_rows=signal_rows,
        validation_rows=validation_rows,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return {
        "summary": summary,
        "data_inventory": {**common, "rows": inventory_rows},
        "pit_gap_matrix": {**common, "rows": pit_gap_rows},
        "proxy_risk_matrix": {**common, "rows": proxy_risk_rows},
        "design_sketch": {**common, **design_sketch},
        "signal_concept_matrix": {**common, "rows": signal_rows},
        "validation_route_matrix": {**common, "rows": validation_rows},
        "recommendation_matrix": {**common, "rows": recommendation_rows},
        "task_route": {**common, **task_route},
        "safety_boundary": {**common, **safety_boundary},
        "docs": docs,
    }


def build_breadth_input_data_inventory(*, target_etfs: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for etf in target_etfs:
        rows.extend(
            [
                _inventory_row(
                    input_name=f"{etf}_etf_price_history",
                    input_category="etf_index_price_history",
                    target_etf=etf,
                    source_available=True,
                    source_path="data/raw/prices_daily.csv (validation required before use)",
                    history_start="not_read_by_static_feasibility_audit",
                    history_end="not_read_by_static_feasibility_audit",
                    coverage_ratio="not_evaluated",
                    frequency="daily",
                    pit_status="PIT_APPROXIMATION_READY",
                    bias_risk="LOW_BIAS",
                    data_cost="existing_cache_if_validated",
                    manual_action_required=False,
                    license_note="existing project cache policy applies",
                    recommended_usage="benchmark_anchor_after_aits_validate_data_pass",
                ),
                _inventory_row(
                    input_name=f"{etf}_current_constituents_snapshot",
                    input_category="etf_current_constituents",
                    target_etf=etf,
                    source_available=False,
                    source_path="owner_or_provider_current_snapshot_required",
                    history_start="snapshot_only",
                    history_end="snapshot_only",
                    coverage_ratio="not_available_until_snapshot_ingested",
                    frequency="as_reported_current_snapshot",
                    pit_status="CURRENT_CONSTITUENTS_PROXY_ONLY",
                    bias_risk="HIGH_SURVIVORSHIP_BIAS",
                    data_cost="manual_or_provider_source_dependent",
                    manual_action_required=True,
                    license_note="issuer/vendor terms must be recorded before caching",
                    recommended_usage="diagnostics_only_after_snapshot_freeze",
                ),
                _inventory_row(
                    input_name=f"{etf}_historical_constituents",
                    input_category="historical_etf_constituents",
                    target_etf=etf,
                    source_available=False,
                    source_path="no_owner_approved_local_pit_source",
                    history_start="missing",
                    history_end="missing",
                    coverage_ratio="0",
                    frequency="rebalance_or_daily_membership_required",
                    pit_status="BLOCKED_NO_RELIABLE_DATA",
                    bias_risk="UNACCEPTABLE_FOR_VALIDATION",
                    data_cost="paid_vendor_or_owner_source_decision_required",
                    manual_action_required=True,
                    license_note="true breadth contract requires license/local-cache review",
                    recommended_usage="data_source_decision_required_before_validation",
                ),
                _inventory_row(
                    input_name=f"{etf}_constituent_price_history",
                    input_category="constituent_price_history",
                    target_etf=etf,
                    source_available=False,
                    source_path="blocked_until_constituent_universe_defined",
                    history_start="missing",
                    history_end="missing",
                    coverage_ratio="not_computable_without_membership",
                    frequency="daily_adjusted_close_required",
                    pit_status="CURRENT_CONSTITUENTS_PROXY_ONLY",
                    bias_risk="HIGH_SURVIVORSHIP_BIAS",
                    data_cost="depends_on_constituent_universe_and_price_vendor",
                    manual_action_required=True,
                    license_note="delisted ticker and corporate-action coverage must be audited",
                    recommended_usage="coverage_audit_only_until_membership_source_exists",
                ),
            ]
        )

    rows.extend(_alternative_proxy_inventory_rows())
    _validate_enum_rows(rows, "pit_status", PIT_STATUSES)
    _validate_enum_rows(rows, "bias_risk", BIAS_RISKS)
    return rows


def build_historical_constituent_pit_gap_matrix(
    *,
    target_etfs: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for etf in target_etfs:
        rows.append(
            {
                "target_etf": etf,
                "historical_constituents_available": False,
                "historical_weights_available": False,
                "rebalance_history_available": False,
                "delisted_constituent_coverage": False,
                "symbol_mapping_available": False,
                "strict_pit_blockers": _joined(
                    [
                        "no owner-approved local historical constituent source",
                        "daily membership and rebalance effective dates missing",
                        "delisted constituent coverage missing",
                        "known_at / reported_at semantics missing",
                        "current constituents backfill forbidden",
                    ]
                ),
                "pit_approximation_possible": False,
                "current_constituents_proxy_only": True,
                "survivorship_bias_risk": "HIGH_SURVIVORSHIP_BIAS",
                "lookahead_bias_risk": "HIGH_LOOKAHEAD_BIAS",
                "recommendation": (
                    "CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY"
                ),
                "historical_constituent_gap": True,
                "strict_pit_breadth_blocked": True,
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_current_constituents_proxy_risk_matrix(
    *,
    target_etfs: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for etf in target_etfs:
        rows.append(
            {
                "target_etf": etf,
                "current_constituent_count": "source_not_cached",
                "weight_available": "source_not_cached",
                "constituent_price_coverage": "not_audited_until_snapshot_exists",
                "survivorship_bias_risk": "HIGH_SURVIVORSHIP_BIAS",
                "lookahead_bias_risk": "HIGH_LOOKAHEAD_BIAS",
                "expected_bias_direction": (
                    "overstates historical participation by retaining current survivors "
                    "and current mega-cap winners"
                ),
                "acceptable_for_candidate_generator_poc": True,
                "acceptable_for_actual_path_validation": False,
                "acceptable_for_forward_observe": (
                    "conditional_forward_only_after_snapshot_freeze"
                ),
                "acceptable_for_promotion": False,
                "notes": (
                    "Diagnostics-only proxy. It must not be relabeled as strict PIT, "
                    "actual-path validation evidence, promotion evidence, or broker input."
                ),
                **SAFETY_FIELDS,
            }
        )
    return rows


def build_candidate_family_design_sketch(
    *,
    target_etfs: Sequence[str],
    target_assets: Sequence[str],
) -> dict[str, Any]:
    return {
        "candidate_family": "breadth_participation",
        "target_etfs": list(target_etfs),
        "target_assets": list(target_assets),
        "candidate_role": [
            "trend_quality_filter",
            "trend_fragility_warning",
            "participation_confirmation",
            "concentration_risk_warning",
        ],
        "not_recommended_as": [
            "primary_directional_signal",
            "standalone_return_predictor",
            "broker_action_signal",
        ],
        "potential_candidate_ids": [
            "qqq_breadth_participation_v1",
            "smh_breadth_participation_v1",
            "cross_asset_breadth_quality_v1",
            "mega_cap_concentration_fragility_v1",
        ],
        "signal_concepts": [
            "breadth_participation_score",
            "advance_decline_participation_score",
            "constituent_momentum_breadth_score",
            "new_high_new_low_proxy_score",
            "mega_cap_concentration_risk_score",
            "sector_leadership_diffusion_score",
            "trend_fragility_score",
        ],
        "signal_direction_mapping": {
            "breadth_participation_score_high": {
                "direction": "trend_confirming",
                "usage_role": "confirmation_only",
            },
            "breadth_participation_score_low": {
                "direction": "trend_weakening",
                "usage_role": "confirmation_only",
            },
            "trend_fragility_score_high": {
                "direction": "risk_off",
                "usage_role": "risk_cap_modifier",
            },
            "mega_cap_concentration_risk_high": {
                "direction": "trend_weakening",
                "usage_role": "warning_only",
            },
        },
        "horizons": list(HORIZONS),
        "priority_horizons": list(PRIORITY_HORIZONS),
        "horizon_rationale": (
            "Breadth / participation reflects trend quality; 10d and 20d are "
            "preferred over 1d noise."
        ),
        "data_feasibility_conclusion": (
            "Strict PIT breadth is blocked by historical constituent gaps. Current "
            "constituents proxy is acceptable only for diagnostics / POC."
        ),
        **SAFETY_FIELDS,
    }


def build_candidate_signal_concept_matrix(
    *,
    target_assets: Sequence[str],
) -> list[dict[str, Any]]:
    rows = [
        _signal_row(
            signal_name="breadth_participation_score",
            description="Share of ETF constituents participating in the target asset trend.",
            required_inputs=["historical_or_current_constituents", "constituent_adjusted_close"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="confirmation_only",
            expected_direction="high_confirms_trend_low_weakens_trend",
            pit_requirement="STRICT_PIT_READY_OR_PIT_APPROXIMATION_READY",
            data_feasibility="CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW",
            bias_risk="HIGH_SURVIVORSHIP_BIAS",
            validation_method="strict_pit_required_for_actual_path_validation",
            recommended_priority="P1",
        ),
        _signal_row(
            signal_name="advance_decline_participation_score",
            description="Advance/decline style constituent participation proxy.",
            required_inputs=["constituent_return_direction", "membership_snapshot"],
            target_assets=target_assets,
            horizons=["5d", "10d", "20d"],
            usage_role="confirmation_only",
            expected_direction="more_advancers_confirm_trend",
            pit_requirement="PIT_APPROXIMATION_READY",
            data_feasibility="PROXY_ONLY_UNTIL_HISTORICAL_MEMBERSHIP_SOURCE",
            bias_risk="HIGH_SURVIVORSHIP_BIAS",
            validation_method="offline_diagnostics_until_pit_source_exists",
            recommended_priority="P1",
        ),
        _signal_row(
            signal_name="constituent_momentum_breadth_score",
            description="Constituent share with positive medium-horizon momentum.",
            required_inputs=["constituent_adjusted_close", "membership_snapshot"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="confirmation_only",
            expected_direction="broad_positive_momentum_confirms_trend",
            pit_requirement="STRICT_PIT_READY",
            data_feasibility="BLOCKED_NO_RELIABLE_DATA_FOR_STRICT_PIT",
            bias_risk="UNACCEPTABLE_FOR_VALIDATION",
            validation_method="blocked_pending_data_source",
            recommended_priority="P1",
        ),
        _signal_row(
            signal_name="new_high_new_low_proxy_score",
            description="Proxy for new-high / new-low diffusion inside ETF members.",
            required_inputs=["constituent_adjusted_close", "rolling_high_low_windows"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="trend_fragility_warning",
            expected_direction="new_low_pressure_warns_trend_fragility",
            pit_requirement="STRICT_PIT_READY",
            data_feasibility="BLOCKED_NO_RELIABLE_DATA_FOR_STRICT_PIT",
            bias_risk="UNACCEPTABLE_FOR_VALIDATION",
            validation_method="blocked_pending_data_source",
            recommended_priority="P2",
        ),
        _signal_row(
            signal_name="mega_cap_concentration_risk_score",
            description="Warning when cap-weight index gains rely on narrow mega-cap leadership.",
            required_inputs=["current_or_historical_weights", "top_weight_return_contribution"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="trend_fragility_warning",
            expected_direction="high_concentration_warns_weak_participation",
            pit_requirement="PIT_APPROXIMATION_READY",
            data_feasibility="CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW",
            bias_risk="HIGH_LOOKAHEAD_BIAS",
            validation_method="diagnostics_only_until_weight_history_is_pit",
            recommended_priority="P1",
        ),
        _signal_row(
            signal_name="sector_leadership_diffusion_score",
            description="Participation diffusion across semiconductor and AI-adjacent groups.",
            required_inputs=["sector_metadata", "constituent_returns", "membership_snapshot"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="diagnostic_only",
            expected_direction="diffuse_leadership_supports_trend_quality",
            pit_requirement="PIT_APPROXIMATION_READY",
            data_feasibility="PROXY_ONLY_UNTIL_SECTOR_METADATA_AND_MEMBERSHIP_AUDIT",
            bias_risk="MODERATE_BIAS",
            validation_method="offline_diagnostics",
            recommended_priority="P2",
        ),
        _signal_row(
            signal_name="trend_fragility_score",
            description="Composite warning from low participation and high concentration.",
            required_inputs=["breadth_score", "concentration_score", "price_trend_state"],
            target_assets=target_assets,
            horizons=["10d", "20d"],
            usage_role="risk_cap_modifier",
            expected_direction="high_fragility_is_risk_off_warning",
            pit_requirement="PIT_APPROXIMATION_READY",
            data_feasibility="CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW",
            bias_risk="HIGH_SURVIVORSHIP_BIAS",
            validation_method="diagnostics_only_until_pit_source_exists",
            recommended_priority="P1",
        ),
    ]
    _validate_enum_rows(rows, "usage_role", USAGE_ROLES)
    return rows


def build_candidate_validation_route_matrix() -> list[dict[str, Any]]:
    return [
        _validation_route_row(
            candidate_id="qqq_breadth_participation_v1",
            data_mode="strict_pit_historical_constituents",
            pit_status="BLOCKED_NO_RELIABLE_DATA",
            allowed_validation="research_design_only",
            blocked_validation="candidate_generator_poc; actual_path_validation; promotion",
            required_artifacts=[
                "historical_constituent_membership",
                "daily_adjusted_constituent_prices",
                "source_hash_and_known_at_manifest",
            ],
            next_task="TRADING-2303_Breadth_Data_Source_Decision",
        ),
        _validation_route_row(
            candidate_id="smh_breadth_participation_v1",
            data_mode="current_constituents_proxy",
            pit_status="CURRENT_CONSTITUENTS_PROXY_ONLY",
            allowed_validation="candidate_generator_poc",
            blocked_validation="actual_path_validation; promotion; paper_shadow; production",
            required_artifacts=[
                "frozen_current_constituent_snapshot",
                "constituent_price_coverage_audit",
                "proxy_bias_disclosure",
            ],
            next_task="TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only",
        ),
        _validation_route_row(
            candidate_id="cross_asset_breadth_quality_v1",
            data_mode="pit_approximation_historical_constituents",
            pit_status="BLOCKED_NO_RELIABLE_DATA",
            allowed_validation="offline_diagnostics",
            blocked_validation="promotion; paper_shadow; production; broker_action",
            required_artifacts=[
                "approximate_membership_policy",
                "known_at_caveat_report",
                "bias_risk_matrix",
            ],
            next_task="TRADING-2303_Breadth_Data_Source_Decision",
        ),
        _validation_route_row(
            candidate_id="mega_cap_concentration_fragility_v1",
            data_mode="forward_only",
            pit_status="FORWARD_OBSERVE_ONLY",
            allowed_validation="forward_observe_only",
            blocked_validation="historical_actual_path_validation; promotion",
            required_artifacts=[
                "frozen_snapshot_from_start_date",
                "forward_evidence_log",
                "no_backfill_attestation",
            ],
            next_task="TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only",
        ),
    ]


def build_data_feasibility_recommendation_matrix() -> list[dict[str, Any]]:
    return [
        {
            "route": "strict_pit_historical_constituents",
            "strict_pit_feasibility": False,
            "pit_approximation_feasibility": False,
            "current_constituents_proxy_feasibility": False,
            "recommendation": "BLOCKED_PENDING_DATA_SOURCE",
            "next_task": "TRADING-2303_Breadth_Data_Source_Decision",
            "reason": "Historical membership, weights, delisted coverage and known_at are missing.",
            **SAFETY_FIELDS,
        },
        {
            "route": "current_constituents_proxy",
            "strict_pit_feasibility": False,
            "pit_approximation_feasibility": False,
            "current_constituents_proxy_feasibility": True,
            "recommendation": "CURRENT_CONSTITUENTS_PROXY_ALLOWED_FOR_DIAGNOSTICS_ONLY",
            "next_task": "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only",
            "reason": (
                "A frozen current snapshot can support diagnostics / POC, but creates "
                "survivorship and lookahead bias for historical validation."
            ),
            **SAFETY_FIELDS,
        },
    ]


def build_2303_task_route(
    *,
    strict_pit_feasibility: bool,
    pit_approximation_feasibility: bool,
    current_constituents_proxy_feasibility: bool,
) -> dict[str, Any]:
    if strict_pit_feasibility:
        next_task = "TRADING-2303_Breadth_Participation_Executable_Generator_POC"
        caveat = "STRICT_PIT_READY"
    elif pit_approximation_feasibility:
        next_task = "TRADING-2303_Breadth_Participation_Executable_Generator_POC"
        caveat = "PIT_APPROXIMATION_ONLY"
    elif current_constituents_proxy_feasibility:
        next_task = "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only"
        caveat = "SURVIVORSHIP_BIAS"
    else:
        next_task = "TRADING-2303_Breadth_Data_Source_Decision"
        caveat = "NO_RELIABLE_DATA"
    return {
        "route_status": next_task,
        "next_task": next_task,
        "strict_pit_feasibility": strict_pit_feasibility,
        "pit_approximation_feasibility": pit_approximation_feasibility,
        "current_constituents_proxy_feasibility": current_constituents_proxy_feasibility,
        "caveat": caveat,
        "generator_implementation_allowed_now": (
            strict_pit_feasibility or pit_approximation_feasibility
        ),
        "diagnostics_only_allowed_now": current_constituents_proxy_feasibility,
        **SAFETY_FIELDS,
    }


def build_breadth_safety_boundary() -> dict[str, Any]:
    return {
        "boundary_status": "RESEARCH_ONLY_GENERATOR_VALIDATION_PROMOTION_BLOCKED",
        "research_only": True,
        "generator_implemented": False,
        "candidate_signal_series_generated": False,
        "prediction_artifact_generated": False,
        "actual_path_validation_executed": False,
        "candidate_artifact_generated": False,
        "forbidden_output_statuses": [
            "PROMOTION_READY",
            "PAPER_SHADOW_READY",
            "PRODUCTION_READY",
            "BROKER_READY",
        ],
        "data_quality_status": DATA_QUALITY_STATUS,
        **SAFETY_FIELDS,
    }


def build_feasibility_summary(
    *,
    common: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_gap_rows: Sequence[Mapping[str, Any]],
    proxy_risk_rows: Sequence[Mapping[str, Any]],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    summary = {
        **dict(common),
        "summary": {
            "target_etf_count": len(common["target_etfs"]),
            "inventory_row_count": len(inventory_rows),
            "pit_gap_row_count": len(pit_gap_rows),
            "proxy_risk_row_count": len(proxy_risk_rows),
            "data_quality_status": DATA_QUALITY_STATUS,
            "recommended_next_action": task_route["next_task"],
        },
        "strict_pit_feasibility": False,
        "pit_approximation_feasibility": False,
        "current_constituents_proxy_feasibility": True,
        "recommended_next_action": task_route["next_task"],
        "recommendation_caveat": task_route["caveat"],
        "historical_constituent_gap": True,
        "strict_pit_breadth_blocked": True,
        "current_constituents_proxy_usage": "diagnostics_only",
        "data_quality_status": DATA_QUALITY_STATUS,
        **SAFETY_FIELDS,
    }
    return summary


def write_breadth_participation_feasibility_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "feasibility_summary": output_dir / "breadth_participation_feasibility_summary.json",
        "data_inventory_json": output_dir / "breadth_input_data_inventory.json",
        "data_inventory_csv": output_dir / "breadth_input_data_inventory.csv",
        "pit_gap_json": output_dir / "historical_constituent_pit_gap_matrix.json",
        "pit_gap_csv": output_dir / "historical_constituent_pit_gap_matrix.csv",
        "proxy_risk_json": output_dir / "current_constituents_proxy_risk_matrix.json",
        "proxy_risk_csv": output_dir / "current_constituents_proxy_risk_matrix.csv",
        "design_sketch": output_dir / "breadth_candidate_family_design_sketch.json",
        "signal_concepts_json": output_dir / "breadth_candidate_signal_concept_matrix.json",
        "signal_concepts_csv": output_dir / "breadth_candidate_signal_concept_matrix.csv",
        "validation_route_json": output_dir / "breadth_candidate_validation_route_matrix.json",
        "validation_route_csv": output_dir / "breadth_candidate_validation_route_matrix.csv",
        "recommendation_matrix": output_dir / "breadth_data_feasibility_recommendation_matrix.json",
        "task_route": output_dir / "breadth_2303_task_route.json",
        "safety_boundary": output_dir / "breadth_safety_boundary.json",
        "audit_doc": docs_root / "breadth_participation_candidate_family_feasibility_audit.md",
        "inventory_doc": docs_root / "breadth_participation_data_inventory.md",
        "pit_bias_doc": docs_root / "breadth_participation_pit_and_bias_risk.md",
        "design_doc": docs_root / "breadth_participation_candidate_family_design_sketch.md",
        "route_doc": docs_root / "breadth_participation_2303_task_route.md",
    }

    write_json(paths["feasibility_summary"], artifacts["summary"])
    write_json(paths["data_inventory_json"], artifacts["data_inventory"])
    write_csv_rows(paths["data_inventory_csv"], artifacts["data_inventory"]["rows"])
    write_json(paths["pit_gap_json"], artifacts["pit_gap_matrix"])
    write_csv_rows(paths["pit_gap_csv"], artifacts["pit_gap_matrix"]["rows"])
    write_json(paths["proxy_risk_json"], artifacts["proxy_risk_matrix"])
    write_csv_rows(paths["proxy_risk_csv"], artifacts["proxy_risk_matrix"]["rows"])
    write_json(paths["design_sketch"], artifacts["design_sketch"])
    write_json(paths["signal_concepts_json"], artifacts["signal_concept_matrix"])
    write_csv_rows(paths["signal_concepts_csv"], artifacts["signal_concept_matrix"]["rows"])
    write_json(paths["validation_route_json"], artifacts["validation_route_matrix"])
    write_csv_rows(paths["validation_route_csv"], artifacts["validation_route_matrix"]["rows"])
    write_json(paths["recommendation_matrix"], artifacts["recommendation_matrix"])
    write_json(paths["task_route"], artifacts["task_route"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])

    docs = artifacts["docs"]
    write_markdown(paths["audit_doc"], docs["audit"])
    write_markdown(paths["inventory_doc"], docs["inventory"])
    write_markdown(paths["pit_bias_doc"], docs["pit_bias"])
    write_markdown(paths["design_doc"], docs["design"])
    write_markdown(paths["route_doc"], docs["route"])
    return {key: str(path) for key, path in paths.items()}


def build_research_docs(
    *,
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_gap_rows: Sequence[Mapping[str, Any]],
    proxy_risk_rows: Sequence[Mapping[str, Any]],
    design_sketch: Mapping[str, Any],
    signal_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    audit = "\n".join(
        [
            "# Breadth Participation Candidate Family Feasibility Audit",
            "",
            "TRADING-2302 只做 breadth / participation data feasibility audit。",
            "",
            f"- status: `{summary['status']}`",
            "- selected_market_regime: `ai_after_chatgpt`",
            "- actual_requested_date_range: `static_feasibility_audit`",
            f"- candidate_family: `{summary['candidate_family']}`",
            f"- target_etfs: `{', '.join(summary['target_etfs'])}`",
            f"- strict_pit_feasibility: `{summary['strict_pit_feasibility']}`",
            (
                "- current_constituents_proxy_feasibility: "
                f"`{summary['current_constituents_proxy_feasibility']}`"
            ),
            f"- recommended_next_action: `{summary['recommended_next_action']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            "",
            "## 结论",
            "",
            "Breadth / participation 是 P1-1，因为它最直接补足 "
            "`baseline_plus_trend_structure` 当前形式失败后的趋势确认缺口。"
            "但 QQQ / SPY / SMH strict PIT breadth 当前被 historical constituents、"
            "rebalance history、delisted coverage 和 known-at 语义缺口阻断。",
            "",
            "Current constituents proxy 可以作为 diagnostics / POC，但会引入 "
            "survivorship bias 和 lookahead bias，不能进入 actual-path validation、"
            "promotion、paper-shadow、production 或 broker。",
            "",
            "## Safety",
            "",
            _safety_sentence(safety_boundary),
            "",
        ]
    )
    inventory = "\n".join(
        [
            "# Breadth Participation Data Inventory",
            "",
            "TRADING-2302 不读取 market cache；下表是静态 feasibility inventory。",
            "",
            "|input_name|category|target_etf|pit_status|bias_risk|recommended_usage|",
            "|---|---|---|---|---|---|",
            *[
                (
                    f"|`{row['input_name']}`|{row['input_category']}|`{row['target_etf']}`|"
                    f"`{row['pit_status']}`|`{row['bias_risk']}`|"
                    f"{row['recommended_usage']}|"
                )
                for row in inventory_rows
            ],
            "",
            "ETF / index price history 只可在后续 cached-data quality gate 通过后使用；"
            "current constituent snapshots 只能 diagnostics；historical constituents 缺口仍是"
            " strict PIT blocker。",
            "",
        ]
    )
    pit_bias = "\n".join(
        [
            "# Breadth Participation PIT and Bias Risk",
            "",
            "|target_etf|historical_constituents_available|proxy_only|survivorship|lookahead|recommendation|",
            "|---|---:|---:|---|---|---|",
            *[
                (
                    f"|`{row['target_etf']}`|{row['historical_constituents_available']}|"
                    f"{row['current_constituents_proxy_only']}|"
                    f"`{row['survivorship_bias_risk']}`|"
                    f"`{row['lookahead_bias_risk']}`|`{row['recommendation']}`|"
                )
                for row in pit_gap_rows
            ],
            "",
            "|target_etf|acceptable_for_poc|actual_path_validation|promotion|notes|",
            "|---|---:|---:|---:|---|",
            *[
                (
                    f"|`{row['target_etf']}`|"
                    f"{row['acceptable_for_candidate_generator_poc']}|"
                    f"{row['acceptable_for_actual_path_validation']}|"
                    f"{row['acceptable_for_promotion']}|{row['notes']}|"
                )
                for row in proxy_risk_rows
            ],
            "",
        ]
    )
    design = "\n".join(
        [
            "# Breadth Participation Candidate Family Design Sketch",
            "",
            "本设计草案不生成 candidate-bound artifacts。",
            "",
            "## Candidate IDs",
            "",
            *[f"- `{item}`" for item in design_sketch["potential_candidate_ids"]],
            "",
            "## Signal Concepts",
            "",
            "|signal_name|usage_role|horizons|data_feasibility|bias_risk|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['signal_name']}`|`{row['usage_role']}`|"
                    f"{', '.join(row['horizons'])}|{row['data_feasibility']}|"
                    f"`{row['bias_risk']}`|"
                )
                for row in signal_rows
            ],
            "",
            "优先 horizon 是 `10d` / `20d`；breadth 不应作为 1d 噪音信号。",
            "",
        ]
    )
    route = "\n".join(
        [
            "# Breadth Participation 2303 Task Route",
            "",
            f"- next_task: `{task_route['next_task']}`",
            f"- caveat: `{task_route['caveat']}`",
            (
                "- generator_implementation_allowed_now: "
                f"`{task_route['generator_implementation_allowed_now']}`"
            ),
            f"- diagnostics_only_allowed_now: `{task_route['diagnostics_only_allowed_now']}`",
            "",
            "|candidate_id|data_mode|pit_status|allowed_validation|next_task|",
            "|---|---|---|---|---|",
            *[
                (
                    f"|`{row['candidate_id']}`|`{row['data_mode']}`|"
                    f"`{row['pit_status']}`|`{row['allowed_validation']}`|"
                    f"`{row['next_task']}`|"
                )
                for row in validation_rows
            ],
            "",
            "当前推荐进入 current constituents proxy diagnostics-only 路线。"
            "这不是 generator implementation approval，也不是 actual-path validation approval。",
            "",
        ]
    )
    return {
        "audit": audit,
        "inventory": inventory,
        "pit_bias": pit_bias,
        "design": design,
        "route": route,
    }


def _validated_request(
    *,
    target_etfs: str | Sequence[str],
    target_assets: str | Sequence[str],
    candidate_family: str,
    mode: str,
) -> dict[str, Any]:
    if mode != MODE:
        raise BreadthParticipationFeasibilityAuditError(
            f"breadth participation feasibility audit only supports {MODE}"
        )
    if candidate_family != "breadth_participation":
        raise BreadthParticipationFeasibilityAuditError(
            "candidate-family must be breadth_participation"
        )
    etfs = _parse_list(target_etfs)
    assets = _parse_list(target_assets)
    if not etfs:
        raise BreadthParticipationFeasibilityAuditError("--target-etfs is required")
    if not assets:
        raise BreadthParticipationFeasibilityAuditError("--target-assets is required")
    return {
        "target_etfs": etfs,
        "target_assets": assets,
        "candidate_family": candidate_family,
        "mode": mode,
    }


def _common_payload(
    *,
    target_etfs: Sequence[str],
    target_assets: Sequence[str],
    candidate_family: str,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "Breadth Participation Candidate Family Feasibility Audit",
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "candidate_family": candidate_family,
        "target_etfs": list(target_etfs),
        "target_assets": list(target_assets),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "static_feasibility_audit",
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_requirement": (
            "This static audit does not consume cached market/macro data. Future "
            "generator, scoring, backtest, daily report, or actual-path validation "
            "must run aits validate-data or the same validation code path first."
        ),
        "source_evidence_paths": dict(SOURCE_EVIDENCE_PATHS),
        **SAFETY_FIELDS,
    }


def _alternative_proxy_inventory_rows() -> list[dict[str, Any]]:
    specs = [
        (
            "current_constituents_proxy_breadth",
            "alternative_proxy_inputs",
            "ALL",
            False,
            "owner_or_provider_current_snapshot_required",
            "CURRENT_CONSTITUENTS_PROXY_ONLY",
            "HIGH_SURVIVORSHIP_BIAS",
            "diagnostics_only_after_snapshot_freeze",
        ),
        (
            "equal_weight_etf_proxy",
            "alternative_proxy_inputs",
            "QQQ/SPY",
            True,
            "participation_proxy_free_registry / equal-weight ETF ratio artifacts",
            "PIT_APPROXIMATION_READY",
            "MODERATE_BIAS",
            "offline_diagnostics_only_not_true_breadth",
        ),
        (
            "sector_etf_proxy",
            "alternative_proxy_inputs",
            "QQQ/SMH",
            True,
            "config/research/participation_proxy_free_registry.yaml",
            "PIT_APPROXIMATION_READY",
            "MODERATE_BIAS",
            "diagnostic_leadership_proxy_only",
        ),
        (
            "mega_cap_concentration_proxy",
            "alternative_proxy_inputs",
            "QQQ/SPY/SMH",
            False,
            "historical_weights_or_snapshot_required",
            "CURRENT_CONSTITUENTS_PROXY_ONLY",
            "HIGH_LOOKAHEAD_BIAS",
            "warning_only_after_weight_source_audit",
        ),
        (
            "qqq_vs_equal_weight_nasdaq_proxy",
            "alternative_proxy_inputs",
            "QQQ",
            True,
            "QQQE/QQQ price proxy if cache validation passes",
            "PIT_APPROXIMATION_READY",
            "MODERATE_BIAS",
            "diagnostics_only_not_constituent_membership",
        ),
        (
            "smh_internal_leadership_proxy",
            "alternative_proxy_inputs",
            "SMH",
            True,
            "SMH/SOXX/QQQ ETF ratio proxy registry",
            "PIT_APPROXIMATION_READY",
            "MODERATE_BIAS",
            "diagnostics_only_not_internal_membership",
        ),
    ]
    return [
        _inventory_row(
            input_name=name,
            input_category=category,
            target_etf=target,
            source_available=available,
            source_path=source_path,
            history_start="source_dependent",
            history_end="source_dependent",
            coverage_ratio="source_dependent",
            frequency="daily_or_snapshot_dependent",
            pit_status=pit_status,
            bias_risk=bias_risk,
            data_cost="existing_proxy_or_manual_source_dependent",
            manual_action_required=not available,
            license_note="proxy is not true PIT breadth",
            recommended_usage=usage,
        )
        for (
            name,
            category,
            target,
            available,
            source_path,
            pit_status,
            bias_risk,
            usage,
        ) in specs
    ]


def _inventory_row(
    *,
    input_name: str,
    input_category: str,
    target_etf: str,
    source_available: bool,
    source_path: str,
    history_start: str,
    history_end: str,
    coverage_ratio: str,
    frequency: str,
    pit_status: str,
    bias_risk: str,
    data_cost: str,
    manual_action_required: bool,
    license_note: str,
    recommended_usage: str,
) -> dict[str, Any]:
    return {
        "input_name": input_name,
        "input_category": input_category,
        "target_etf": target_etf,
        "source_available": source_available,
        "source_path": source_path,
        "history_start": history_start,
        "history_end": history_end,
        "coverage_ratio": coverage_ratio,
        "frequency": frequency,
        "pit_status": pit_status,
        "bias_risk": bias_risk,
        "data_cost": data_cost,
        "manual_action_required": manual_action_required,
        "license_note": license_note,
        "recommended_usage": recommended_usage,
        **SAFETY_FIELDS,
    }


def _signal_row(
    *,
    signal_name: str,
    description: str,
    required_inputs: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    usage_role: str,
    expected_direction: str,
    pit_requirement: str,
    data_feasibility: str,
    bias_risk: str,
    validation_method: str,
    recommended_priority: str,
) -> dict[str, Any]:
    return {
        "signal_name": signal_name,
        "description": description,
        "required_inputs": list(required_inputs),
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "usage_role": usage_role,
        "expected_direction": expected_direction,
        "pit_requirement": pit_requirement,
        "data_feasibility": data_feasibility,
        "bias_risk": bias_risk,
        "validation_method": validation_method,
        "recommended_priority": recommended_priority,
        **SAFETY_FIELDS,
    }


def _validation_route_row(
    *,
    candidate_id: str,
    data_mode: str,
    pit_status: str,
    allowed_validation: str,
    blocked_validation: str,
    required_artifacts: Sequence[str],
    next_task: str,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "candidate_family": "breadth_participation",
        "data_mode": data_mode,
        "pit_status": pit_status,
        "allowed_validation": allowed_validation,
        "blocked_validation": blocked_validation,
        "required_artifacts": list(required_artifacts),
        "next_task": next_task,
        **SAFETY_FIELDS,
    }


def _parse_list(value: str | Sequence[str]) -> list[str]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    return [part.strip().upper() for part in parts if part.strip()]


def _validate_enum_rows(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    allowed: set[str],
) -> None:
    invalid = sorted({str(row.get(field)) for row in rows if str(row.get(field)) not in allowed})
    if invalid:
        raise BreadthParticipationFeasibilityAuditError(
            f"{field} contains unsupported values: {invalid}"
        )


def _joined(values: Sequence[str]) -> str:
    return "; ".join(values)


def _safety_sentence(payload: Mapping[str, Any]) -> str:
    return (
        f"promotion_allowed=`{payload['promotion_allowed']}`, "
        f"paper_shadow_allowed=`{payload['paper_shadow_allowed']}`, "
        f"production_allowed=`{payload['production_allowed']}`, "
        f"broker_action=`{payload['broker_action']}`, "
        f"generator_implemented=`{payload['generator_implemented']}`, "
        f"actual_path_validation_executed=`{payload['actual_path_validation_executed']}`."
    )


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "MODE",
    "STATUS",
    "BreadthParticipationFeasibilityAuditError",
    "build_2303_task_route",
    "build_breadth_input_data_inventory",
    "build_breadth_safety_boundary",
    "build_candidate_family_design_sketch",
    "build_candidate_signal_concept_matrix",
    "build_candidate_validation_route_matrix",
    "build_current_constituents_proxy_risk_matrix",
    "build_historical_constituent_pit_gap_matrix",
    "run_breadth_participation_feasibility_audit",
]
