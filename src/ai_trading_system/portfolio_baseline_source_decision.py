from __future__ import annotations

import hashlib
import json
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
    clean_for_yaml,
    mapping,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2325_PORTFOLIO_BASELINE_SOURCE_DECISION"
REPORT_TYPE = "portfolio_baseline_source_decision"
ARTIFACT_ROLE = "portfolio_baseline_source_decision"
MODE = "baseline_source_decision"
STATUS = "PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_DECISION_ONLY"

DEFAULT_SOURCE_BINDING_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_simulation_source_binding"
)
DEFAULT_SIMULATION_POLICY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_mechanics_simulation"
)
DEFAULT_PORTFOLIO_CONFIG_ROOT = PROJECT_ROOT / "config" / "etf_portfolio"
DEFAULT_PAPER_PORTFOLIO_CONFIG = (
    DEFAULT_PORTFOLIO_CONFIG_ROOT / "dynamic_v3_rescue" / "paper_portfolio_v1.yaml"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

BASELINE_IDS = (
    "synthetic_observe_only_baseline",
    "static_etf_allocation_baseline",
    "dynamic_strategy_target_exposure_baseline",
    "paper_portfolio_advisory_baseline",
    "actual_holdings_derived_baseline",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "source_decision_only": True,
    "simulation_executed": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_only": True,
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}


class PortfolioBaselineSourceDecisionError(ValueError):
    pass


def run_portfolio_baseline_source_decision(
    *,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    portfolio_config_dir: Path | None = DEFAULT_PORTFOLIO_CONFIG_ROOT,
    paper_portfolio_config: Path | None = DEFAULT_PAPER_PORTFOLIO_CONFIG,
    actual_holdings_source: Path | None = None,
    allow_synthetic_baseline: bool = True,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise PortfolioBaselineSourceDecisionError(
            f"portfolio baseline source decision only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    source_binding = load_source_binding_outputs(source_binding_dir)
    simulation_policy = load_simulation_policy_outputs(simulation_policy_dir)
    target_assets = _target_assets(source_binding)

    candidate_rows = build_portfolio_baseline_candidate_matrix(
        source_binding=source_binding,
        simulation_policy=simulation_policy,
        portfolio_config_dir=portfolio_config_dir,
        paper_portfolio_config=paper_portfolio_config,
        actual_holdings_source=actual_holdings_source,
        allow_synthetic_baseline=allow_synthetic_baseline,
        target_assets=target_assets,
    )
    feasibility_rows = build_portfolio_baseline_source_feasibility_matrix(candidate_rows)
    pit_rows = build_portfolio_baseline_pit_reproducibility_audit(candidate_rows)
    risk_rows = build_portfolio_baseline_risk_matrix(candidate_rows)
    recommendation = build_portfolio_baseline_recommendation(
        feasibility_rows=feasibility_rows,
        pit_rows=pit_rows,
    )
    field_rows = build_portfolio_baseline_field_requirement_matrix(
        recommended_baseline=str(recommendation["selected_for_2326"])
    )
    task_route = build_exposure_cap_2326_task_route(
        recommendation=recommendation,
        feasibility_rows=feasibility_rows,
    )
    safety_boundary = build_portfolio_baseline_source_safety_boundary(
        generated_at=generated_at,
        recommendation=recommendation,
    )
    summary = build_portfolio_baseline_source_decision_summary(
        generated_at=generated_at,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        portfolio_config_dir=portfolio_config_dir,
        paper_portfolio_config=paper_portfolio_config,
        actual_holdings_source=actual_holdings_source,
        allow_synthetic_baseline=allow_synthetic_baseline,
        source_binding=source_binding,
        simulation_policy=simulation_policy,
        target_assets=target_assets,
        candidate_rows=candidate_rows,
        feasibility_rows=feasibility_rows,
        pit_rows=pit_rows,
        risk_rows=risk_rows,
        recommendation=recommendation,
        task_route=task_route,
    )
    paths = write_portfolio_baseline_source_decision_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        candidate_rows=candidate_rows,
        feasibility_rows=feasibility_rows,
        pit_rows=pit_rows,
        risk_rows=risk_rows,
        field_rows=field_rows,
        recommendation=recommendation,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_source_binding_outputs(source_binding_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": source_binding_dir / "exposure_cap_source_binding_summary.json",
        "source_inventory": source_binding_dir / "exposure_cap_source_inventory.json",
        "source_gap_matrix": source_binding_dir / "exposure_cap_source_gap_matrix.json",
        "risk_cap_trigger_binding": source_binding_dir
        / "risk_cap_trigger_series_binding_report.json",
        "market_data_binding": source_binding_dir / "market_data_binding_report.json",
        "portfolio_baseline_binding": source_binding_dir
        / "portfolio_baseline_binding_report.json",
        "turnover_rebalance_assumption": source_binding_dir
        / "turnover_rebalance_assumption_report.json",
        "dry_run_readiness": source_binding_dir
        / "source_bound_dry_run_simulation_readiness.json",
        "safety_boundary": source_binding_dir / "source_bound_dry_run_safety_boundary.json",
        "next_task_route": source_binding_dir / "exposure_cap_simulation_next_task_route.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        if str(paths["summary"]) in missing:
            raise PortfolioBaselineSourceDecisionError(
                "TRADING-2325 requires TRADING-2324 source binding summary: "
                f"{paths['summary']}"
            )
        raise PortfolioBaselineSourceDecisionError(
            "TRADING-2325 requires TRADING-2324 source binding artifacts: "
            + ", ".join(missing)
        )
    payloads = {key: _load_json(path) for key, path in paths.items()}
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2324 {key}", payload)

    optional_paths = {
        "dry_run_result": source_binding_dir / "source_bound_exposure_cap_dry_run_result.json",
        "dry_run_result_csv": source_binding_dir
        / "source_bound_exposure_cap_dry_run_result.csv",
        "cap_comparison": source_binding_dir / "exposure_cap_vs_no_cap_comparison.json",
        "turnover_impact": source_binding_dir / "exposure_cap_turnover_impact_report.json",
        "cooldown_impact": source_binding_dir / "exposure_cap_cooldown_impact_report.json",
    }
    optional_payloads: dict[str, Any] = {}
    for key, path in optional_paths.items():
        if not path.exists():
            continue
        optional_payloads[key] = (
            {
                "path": str(path),
                "row_count": _csv_row_count(path),
                "source_hash": _file_hash(path),
            }
            if path.suffix.lower() == ".csv"
            else _load_json(path)
        )
        if isinstance(optional_payloads[key], Mapping):
            _validate_no_unsafe_fields(
                f"TRADING-2324 optional {key}",
                optional_payloads[key],
            )

    return {
        "source_dir": str(source_binding_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        "optional_paths": {key: str(path) for key, path in optional_paths.items()},
        "optional_payloads": optional_payloads,
        **payloads,
    }


def load_simulation_policy_outputs(simulation_policy_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": simulation_policy_dir / "exposure_cap_mechanics_simulation_summary.json",
        "readiness": simulation_policy_dir / "exposure_cap_simulation_readiness_matrix.json",
        "metric_contract": simulation_policy_dir / "exposure_cap_simulation_metric_contract.json",
        "input_requirements": simulation_policy_dir
        / "exposure_cap_simulation_input_requirement_matrix.json",
        "blocker_report": simulation_policy_dir / "exposure_cap_simulation_blocker_report.json",
        "safety_boundary": simulation_policy_dir
        / "exposure_cap_simulation_safety_boundary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise PortfolioBaselineSourceDecisionError(
            "TRADING-2325 requires TRADING-2323 simulation policy readiness artifacts: "
            + ", ".join(missing)
        )
    payloads = {key: _load_json(path) for key, path in paths.items()}
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)

    optional_policy_path = simulation_policy_dir / "exposure_cap_policy.json"
    optional_policy = _load_json(optional_policy_path) if optional_policy_path.exists() else {}
    if optional_policy:
        _validate_no_unsafe_fields("TRADING-2323 exposure_cap_policy", optional_policy)
    return {
        "source_dir": str(simulation_policy_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        "optional_policy_path": str(optional_policy_path),
        "optional_policy": optional_policy,
        **payloads,
    }


def build_portfolio_baseline_candidate_matrix(
    *,
    source_binding: Mapping[str, Any],
    simulation_policy: Mapping[str, Any],
    portfolio_config_dir: Path | None,
    paper_portfolio_config: Path | None,
    actual_holdings_source: Path | None,
    allow_synthetic_baseline: bool,
    target_assets: Sequence[str],
) -> list[dict[str, Any]]:
    del simulation_policy
    coverage_start, coverage_end = _source_binding_date_range(source_binding)
    source_summary = mapping(source_binding.get("summary"))
    portfolio_binding = mapping(source_binding.get("portfolio_baseline_binding"))
    synthetic_available = allow_synthetic_baseline and (
        portfolio_binding.get("synthetic_observe_only") is True
        or source_summary.get("portfolio_source_mode") == "synthetic_observe_only"
    )

    static_config_path = _static_config_path(portfolio_config_dir)
    static_config = _load_yaml_if_exists(static_config_path)
    static_assets = _static_supported_assets(static_config)
    static_available = static_config_path is not None and static_config_path.exists()
    static_supports_targets = set(target_assets).issubset(static_assets)

    dynamic_artifact = (
        Path(str(source_summary.get("dynamic_target_exposure_source")))
        if source_summary.get("dynamic_target_exposure_source")
        else PROJECT_ROOT
        / "outputs"
        / "research_trends"
        / "dynamic_strategy_target_exposure"
        / "target_exposure_series.csv"
    )
    paper_available = bool(paper_portfolio_config and paper_portfolio_config.exists())
    paper_config = _load_yaml_if_exists(paper_portfolio_config)
    actual_available = bool(actual_holdings_source and actual_holdings_source.exists())

    rows = [
        _candidate_row(
            baseline_id="synthetic_observe_only_baseline",
            baseline_type="synthetic_observe_only",
            source_path=str(portfolio_binding.get("source_path") or "system_generated"),
            source_available=synthetic_available,
            history_start=coverage_start if synthetic_available else "",
            history_end=coverage_end if synthetic_available else "",
            coverage_ratio=1.0 if synthetic_available else 0.0,
            target_assets_supported=list(target_assets) if synthetic_available else [],
            exposure_fields_available=[
                "date",
                "target_asset",
                "baseline_exposure",
                "risk_asset_exposure",
            ],
            rebalance_calendar_available=True,
            turnover_fields_available=True,
            pit_status="SYNTHETIC_OBSERVE_ONLY",
            reproducibility_status="REPRODUCIBLE_SYSTEM_RULE",
            maintenance_cost="LOW",
            implementation_cost="LOW",
            interpretation_quality="LOW",
            research_stage_fit="early dry-run mechanics verification",
            privacy_or_account_risk="LOW",
            recommended_usage=(
                "early dry-run, source binding smoke test, mechanics verification"
            ),
        ),
        _candidate_row(
            baseline_id="static_etf_allocation_baseline",
            baseline_type="static_etf_allocation",
            source_path=str(static_config_path or ""),
            source_available=static_available,
            history_start=DEFAULT_BACKTEST_START if static_available else "",
            history_end=coverage_end if static_available else "",
            coverage_ratio=1.0 if static_available and static_supports_targets else 0.0,
            target_assets_supported=sorted(static_assets),
            exposure_fields_available=[
                "date",
                "target_asset",
                "asset_weight",
                "baseline_exposure",
                "cash_weight",
                "source_artifact_hash",
            ]
            if static_available
            else [],
            rebalance_calendar_available=True,
            turnover_fields_available=True,
            pit_status="PIT_APPROXIMATION_READY" if static_available else "BLOCKED",
            reproducibility_status=(
                "REPLAYABLE_CONFIG_VERSIONED" if static_available else "SOURCE_MISSING"
            ),
            maintenance_cost="LOW",
            implementation_cost="LOW",
            interpretation_quality="MEDIUM_HIGH",
            research_stage_fit="near-term simulation baseline",
            privacy_or_account_risk="LOW",
            recommended_usage=(
                "first source-bound dry-run simulation and exposure-cap vs no-cap comparison"
            ),
        ),
        _candidate_row(
            baseline_id="dynamic_strategy_target_exposure_baseline",
            baseline_type="dynamic_strategy_target_exposure",
            source_path=str(dynamic_artifact),
            source_available=dynamic_artifact.exists(),
            history_start="",
            history_end="",
            coverage_ratio=0.0,
            target_assets_supported=list(target_assets) if dynamic_artifact.exists() else [],
            exposure_fields_available=[],
            rebalance_calendar_available=False,
            turnover_fields_available=False,
            pit_status="BLOCKED",
            reproducibility_status="PIT_TARGET_EXPOSURE_ARTIFACT_MISSING",
            maintenance_cost="MEDIUM_HIGH",
            implementation_cost="MEDIUM_HIGH",
            interpretation_quality="HIGH",
            research_stage_fit="medium-term simulation baseline",
            privacy_or_account_risk="LOW",
            recommended_usage=(
                "post-static-baseline validation after PIT target exposure artifacts exist"
            ),
        ),
        _candidate_row(
            baseline_id="paper_portfolio_advisory_baseline",
            baseline_type="paper_portfolio_advisory",
            source_path=str(paper_portfolio_config or ""),
            source_available=paper_available,
            history_start=_config_history_value(paper_config, "history_start"),
            history_end=_config_history_value(paper_config, "history_end"),
            coverage_ratio=0.25 if paper_available else 0.0,
            target_assets_supported=list(target_assets) if paper_available else [],
            exposure_fields_available=["advisory_state", "target_exposure"]
            if paper_available
            else [],
            rebalance_calendar_available=False,
            turnover_fields_available=False,
            pit_status=(
                "REPLAYABLE_BUT_NOT_STRICT_PIT" if paper_available else "BLOCKED"
            ),
            reproducibility_status=(
                "CONFIG_EXISTS_HISTORY_CONTINUITY_UNPROVEN"
                if paper_available
                else "SOURCE_MISSING"
            ),
            maintenance_cost="MEDIUM",
            implementation_cost="MEDIUM",
            interpretation_quality="MEDIUM_HIGH",
            research_stage_fit="forward observe simulation",
            privacy_or_account_risk="MEDIUM",
            recommended_usage="forward observe simulation and paper advisory comparison",
        ),
        _candidate_row(
            baseline_id="actual_holdings_derived_baseline",
            baseline_type="actual_holdings_derived",
            source_path=str(actual_holdings_source or ""),
            source_available=actual_available,
            history_start="",
            history_end="",
            coverage_ratio=0.0,
            target_assets_supported=[],
            exposure_fields_available=[],
            rebalance_calendar_available=False,
            turnover_fields_available=False,
            pit_status="MANUAL_REFERENCE_ONLY" if actual_available else "BLOCKED",
            reproducibility_status="MANUAL_OWNER_ONLY_REFERENCE",
            maintenance_cost="HIGH",
            implementation_cost="HIGH",
            interpretation_quality="HIGH_BUT_NOT_CURRENT_STAGE",
            research_stage_fit="future optional owner review only",
            privacy_or_account_risk="HIGH",
            recommended_usage="not recommended for current stage",
        ),
    ]
    return rows


def build_portfolio_baseline_source_feasibility_matrix(
    candidate_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    provisional: list[dict[str, Any]] = []
    for row in candidate_rows:
        baseline_id = str(row["baseline_id"])
        source_available = bool(row["source_available"])
        source_complete = _source_complete(row)
        source_pit_compatible = str(row["pit_status"]) in {
            "STRICT_PIT_READY",
            "PIT_APPROXIMATION_READY",
            "REPLAYABLE_BUT_NOT_STRICT_PIT",
            "SYNTHETIC_OBSERVE_ONLY",
        }
        source_reproducible = str(row["reproducibility_status"]) not in {
            "SOURCE_MISSING",
            "PIT_TARGET_EXPOSURE_ARTIFACT_MISSING",
        }
        source_maintainable = str(row["maintenance_cost"]) in {"LOW", "MEDIUM"}
        source_privacy_safe = str(row["privacy_or_account_risk"]) != "HIGH"
        dry_run_eligible = (
            source_available
            and source_reproducible
            and source_privacy_safe
            and baseline_id
            in {
                "synthetic_observe_only_baseline",
                "static_etf_allocation_baseline",
                "dynamic_strategy_target_exposure_baseline",
                "paper_portfolio_advisory_baseline",
            }
        )
        if baseline_id == "dynamic_strategy_target_exposure_baseline":
            dry_run_eligible = dry_run_eligible and source_complete
        if baseline_id == "paper_portfolio_advisory_baseline":
            dry_run_eligible = False
        full_simulation_eligible = (
            source_available
            and source_complete
            and source_pit_compatible
            and source_reproducible
            and bool(row["rebalance_calendar_available"])
            and bool(row["turnover_fields_available"])
            and baseline_id != "synthetic_observe_only_baseline"
            and source_privacy_safe
        )
        blockers: list[str] = []
        warnings: list[str] = []
        if not source_available:
            blockers.append("source_missing")
        if baseline_id == "dynamic_strategy_target_exposure_baseline":
            blockers.append("pit_target_exposure_artifact_missing")
        if baseline_id == "actual_holdings_derived_baseline":
            blockers.append("privacy_and_account_boundary_not_current_stage")
        if not source_complete:
            warnings.append("source_completeness_not_enough_for_full_simulation")
        if baseline_id == "synthetic_observe_only_baseline":
            warnings.append("proxy_diagnostics_only_not_strategy_level_conclusion")
        if baseline_id == "static_etf_allocation_baseline":
            warnings.append("pit_approximation_current_config_applied_to_history")
        provisional.append(
            {
                "baseline_id": baseline_id,
                "source_available": source_available,
                "source_complete": source_complete,
                "source_pit_compatible": source_pit_compatible,
                "source_reproducible": source_reproducible,
                "source_maintainable": source_maintainable,
                "source_privacy_safe": source_privacy_safe,
                "dry_run_eligible": dry_run_eligible,
                "full_simulation_eligible": full_simulation_eligible,
                "recommended_for_2326": False,
                "blockers": blockers,
                "warnings": warnings,
            }
        )
    selected = _select_baseline_for_2326(provisional)
    for row in provisional:
        row["recommended_for_2326"] = row["baseline_id"] == selected
    return provisional


def build_portfolio_baseline_pit_reproducibility_audit(
    candidate_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in candidate_rows:
        baseline_id = str(row["baseline_id"])
        source_path = Path(str(row.get("source_path", "")))
        source_exists = source_path.exists()
        if baseline_id == "synthetic_observe_only_baseline":
            audit = {
                "pit_status": "SYNTHETIC_OBSERVE_ONLY",
                "as_of_timestamp_available": True,
                "decision_timestamp_available": False,
                "rebalance_timestamp_available": False,
                "artifact_hash_available": False,
                "source_version_available": True,
                "replayable": bool(row["source_available"]),
                "known_at_semantics": "system rule known at run time only",
                "lookahead_risk": "LOW_FOR_MECHANICS_HIGH_FOR_STRATEGY_INTERPRETATION",
                "revision_risk": "LOW",
                "recommendation": "Use only as fallback proxy diagnostics.",
            }
        elif baseline_id == "static_etf_allocation_baseline":
            audit = {
                "pit_status": "PIT_APPROXIMATION_READY" if source_exists else "BLOCKED",
                "as_of_timestamp_available": False,
                "decision_timestamp_available": False,
                "rebalance_timestamp_available": False,
                "artifact_hash_available": source_exists,
                "source_version_available": bool(_source_version(source_path)),
                "replayable": source_exists,
                "known_at_semantics": (
                    "versioned config replay, not strict historical target decision"
                ),
                "lookahead_risk": "MEDIUM",
                "revision_risk": "LOW_WITH_HASHED_CONFIG",
                "recommendation": "Use for TRADING-2326 first source-bound dry-run.",
            }
        elif baseline_id == "dynamic_strategy_target_exposure_baseline":
            audit = {
                "pit_status": "BLOCKED",
                "as_of_timestamp_available": False,
                "decision_timestamp_available": False,
                "rebalance_timestamp_available": False,
                "artifact_hash_available": source_exists,
                "source_version_available": False,
                "replayable": False,
                "known_at_semantics": "requires PIT target exposure artifact",
                "lookahead_risk": "BLOCKING_UNTIL_PIT_ARTIFACT_EXISTS",
                "revision_risk": "HIGH",
                "recommendation": "Keep as medium-term baseline after artifact remediation.",
            }
        elif baseline_id == "paper_portfolio_advisory_baseline":
            audit = {
                "pit_status": "REPLAYABLE_BUT_NOT_STRICT_PIT"
                if source_exists
                else "BLOCKED",
                "as_of_timestamp_available": False,
                "decision_timestamp_available": False,
                "rebalance_timestamp_available": False,
                "artifact_hash_available": source_exists,
                "source_version_available": bool(_source_version(source_path)),
                "replayable": source_exists,
                "known_at_semantics": (
                    "paper advisory config exists, historical continuity unproven"
                ),
                "lookahead_risk": "MEDIUM_HIGH",
                "revision_risk": "MEDIUM",
                "recommendation": "Reserve for forward observe comparison after continuity audit.",
            }
        else:
            audit = {
                "pit_status": "MANUAL_REFERENCE_ONLY"
                if source_exists
                else "BLOCKED",
                "as_of_timestamp_available": False,
                "decision_timestamp_available": False,
                "rebalance_timestamp_available": False,
                "artifact_hash_available": source_exists,
                "source_version_available": False,
                "replayable": False,
                "known_at_semantics": "owner-only actual holdings reference",
                "lookahead_risk": "HIGH_PRIVACY_AND_ACCOUNT_BOUNDARY",
                "revision_risk": "HIGH",
                "recommendation": "Do not use for current research-layer simulation.",
            }
        rows.append({"baseline_id": baseline_id, **audit})
    return rows


def build_portfolio_baseline_risk_matrix(
    candidate_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    treatment = {
        "synthetic_observe_only_baseline": (
            "Use only as fallback proxy diagnostics; disclose non-strategy interpretation."
        ),
        "static_etf_allocation_baseline": (
            "Use for first dry-run with PIT approximation warning and config hash."
        ),
        "dynamic_strategy_target_exposure_baseline": (
            "Block until PIT target exposure and rebalance timing artifacts exist."
        ),
        "paper_portfolio_advisory_baseline": (
            "Audit continuity before using outside forward observe comparison."
        ),
        "actual_holdings_derived_baseline": (
            "Keep owner-only manual reference; do not bind to current simulation."
        ),
    }
    levels = {
        "synthetic_observe_only_baseline": ("LOW", "HIGH", "LOW", "LOW", "LOW", "MEDIUM"),
        "static_etf_allocation_baseline": (
            "LOW",
            "MEDIUM",
            "LOW",
            "LOW",
            "LOW",
            "LOW",
        ),
        "dynamic_strategy_target_exposure_baseline": (
            "MEDIUM",
            "LOW",
            "LOW",
            "MEDIUM",
            "MEDIUM",
            "BLOCKING",
        ),
        "paper_portfolio_advisory_baseline": (
            "MEDIUM",
            "MEDIUM",
            "MEDIUM",
            "MEDIUM",
            "MEDIUM",
            "HIGH",
        ),
        "actual_holdings_derived_baseline": (
            "HIGH",
            "MEDIUM",
            "HIGH",
            "HIGH",
            "MEDIUM",
            "HIGH",
        ),
    }
    rows = []
    for candidate in candidate_rows:
        baseline_id = str(candidate["baseline_id"])
        (
            research_risk,
            interpretation_risk,
            privacy_risk,
            maintenance_risk,
            overfitting_risk,
            source_gap_risk,
        ) = levels[baseline_id]
        rows.append(
            {
                "baseline_id": baseline_id,
                "research_risk": research_risk,
                "interpretation_risk": interpretation_risk,
                "privacy_risk": privacy_risk,
                "maintenance_risk": maintenance_risk,
                "overfitting_risk": overfitting_risk,
                "source_gap_risk": source_gap_risk,
                "recommended_risk_treatment": treatment[baseline_id],
            }
        )
    return rows


def build_portfolio_baseline_field_requirement_matrix(
    *, recommended_baseline: str
) -> list[dict[str, Any]]:
    availability = {
        "date": True,
        "target_asset": True,
        "baseline_exposure": True,
        "asset_weight": recommended_baseline == "static_etf_allocation_baseline",
        "risk_asset_exposure": True,
        "cash_weight": recommended_baseline == "static_etf_allocation_baseline",
        "rebalance_flag": recommended_baseline == "static_etf_allocation_baseline",
        "turnover_proxy": recommended_baseline == "static_etf_allocation_baseline",
        "decision_timestamp": False,
        "as_of_timestamp": False,
        "source_artifact_hash": recommended_baseline == "static_etf_allocation_baseline",
    }
    candidates = {
        "date": ["source_binding_calendar", recommended_baseline],
        "target_asset": ["source_binding_target_assets", recommended_baseline],
        "baseline_exposure": [recommended_baseline],
        "asset_weight": ["static_etf_allocation_baseline"],
        "risk_asset_exposure": [
            "static_etf_allocation_baseline",
            "synthetic_observe_only_baseline",
        ],
        "cash_weight": ["static_etf_allocation_baseline"],
        "rebalance_flag": ["static_etf_allocation_baseline", "turnover_assumption_report"],
        "turnover_proxy": ["turnover_assumption_report"],
        "decision_timestamp": ["dynamic_strategy_target_exposure_baseline"],
        "as_of_timestamp": ["dynamic_strategy_target_exposure_baseline"],
        "source_artifact_hash": [recommended_baseline],
    }
    dry_run_required = {
        "date",
        "target_asset",
        "baseline_exposure",
        "risk_asset_exposure",
        "cash_weight",
        "rebalance_flag",
        "turnover_proxy",
        "source_artifact_hash",
    }
    full_required = set(availability)
    rows = []
    for field_name in availability:
        rows.append(
            {
                "field_name": field_name,
                "required_for_dry_run": field_name in dry_run_required,
                "required_for_full_simulation": field_name in full_required,
                "source_candidates": candidates[field_name],
                "available_in_recommended_baseline": availability[field_name],
                "fallback_allowed": field_name not in {"decision_timestamp", "as_of_timestamp"},
                "fallback_policy": (
                    "allow policy-governed static approximation for dry-run only"
                    if availability[field_name]
                    else "required before full simulation or dynamic baseline route"
                ),
            }
        )
    return rows


def build_portfolio_baseline_recommendation(
    *,
    feasibility_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected = _select_baseline_for_2326(feasibility_rows)
    pit_by_id = {str(row["baseline_id"]): row for row in pit_rows}
    pit_status = str(pit_by_id.get(selected, {}).get("pit_status", "BLOCKED"))
    reproducibility = _reproducibility_for(selected, feasibility_rows)
    return {
        "recommended_short_term_baseline": [
            "static_etf_allocation_baseline",
            "synthetic_observe_only_baseline",
        ],
        "recommended_medium_term_baseline": [
            "dynamic_strategy_target_exposure_baseline",
        ],
        "recommended_long_term_baseline": [
            "paper_portfolio_advisory_baseline",
            "actual_holdings_derived_baseline_manual_reference_only",
        ],
        "selected_for_2326": selected,
        "selection_reason": _selection_reason(selected),
        "fallback_baseline": "synthetic_observe_only_baseline",
        "baseline_source_mode": selected.replace("_baseline", ""),
        "pit_status": pit_status,
        "reproducibility_status": reproducibility,
        "allowed_simulation_mode": "source_bound_dry_run_only",
        "blocked_simulation_mode": [
            "full_simulation",
            "paper_shadow",
            "production",
            "broker_action",
            "rebalance",
        ],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def build_exposure_cap_2326_task_route(
    *,
    recommendation: Mapping[str, Any],
    feasibility_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected = str(recommendation["selected_for_2326"])
    route_by_baseline = {
        "static_etf_allocation_baseline": (
            "TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Static_ETF_Baseline"
        ),
        "dynamic_strategy_target_exposure_baseline": (
            "TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline"
        ),
        "synthetic_observe_only_baseline": (
            "TRADING-2326_Continue_Synthetic_Baseline_Only"
        ),
        "portfolio_baseline_source_remediation": (
            "TRADING-2326_Portfolio_Baseline_Source_Remediation"
        ),
    }
    return {
        "schema_version": f"{REPORT_TYPE}.task_route.v1",
        "task_id": TASK_ID,
        "next_task": route_by_baseline.get(
            selected,
            "TRADING-2326_Portfolio_Baseline_Source_Remediation",
        ),
        "selected_baseline_for_2326": selected,
        "route_reason": str(recommendation["selection_reason"]),
        "eligible_baselines": [
            row["baseline_id"] for row in feasibility_rows if row["dry_run_eligible"]
        ],
        "rejected_or_deferred_baselines": [
            row["baseline_id"] for row in feasibility_rows if not row["dry_run_eligible"]
        ],
        **SAFETY_FIELDS,
    }


def build_portfolio_baseline_source_safety_boundary(
    *,
    generated_at: datetime,
    recommendation: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "generated_at": generated_at.isoformat(),
        "selected_for_2326": recommendation["selected_for_2326"],
        "forbidden_outputs": [
            "target_weight",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
            "broker_action",
            "production_decision",
            "paper_shadow_ready",
        ],
        **SAFETY_FIELDS,
    }


def build_portfolio_baseline_source_decision_summary(
    *,
    generated_at: datetime,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    portfolio_config_dir: Path | None,
    paper_portfolio_config: Path | None,
    actual_holdings_source: Path | None,
    allow_synthetic_baseline: bool,
    source_binding: Mapping[str, Any],
    simulation_policy: Mapping[str, Any],
    target_assets: Sequence[str],
    candidate_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    risk_rows: Sequence[Mapping[str, Any]],
    recommendation: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    del simulation_policy, pit_rows, risk_rows
    source_summary = mapping(source_binding.get("summary"))
    static_row = _row_by_baseline(feasibility_rows, "static_etf_allocation_baseline")
    dynamic_row = _row_by_baseline(
        feasibility_rows,
        "dynamic_strategy_target_exposure_baseline",
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "title": "Portfolio Baseline Source Decision",
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "actual_requested_date_range": source_summary.get(
                "actual_requested_date_range",
                "",
            ),
            "source_binding_dir": str(source_binding_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "portfolio_config_dir": str(portfolio_config_dir or ""),
            "paper_portfolio_config": str(paper_portfolio_config or ""),
            "actual_holdings_source": str(actual_holdings_source or ""),
            "allow_synthetic_baseline": allow_synthetic_baseline,
            "target_assets": list(target_assets),
            "source_binding_status": source_summary.get("source_binding_status", ""),
            "source_binding_dry_run_readiness_status": source_summary.get(
                "dry_run_readiness_status",
                "",
            ),
            "source_binding_portfolio_source_mode": source_summary.get(
                "portfolio_source_mode",
                "",
            ),
            "source_binding_dry_run_record_count": source_summary.get(
                "dry_run_record_count",
                0,
            ),
            "candidate_count": len(candidate_rows),
            "feasibility_row_count": len(feasibility_rows),
            "static_etf_allocation_dry_run_eligible": bool(
                static_row.get("dry_run_eligible")
            ),
            "dynamic_strategy_target_exposure_dry_run_eligible": bool(
                dynamic_row.get("dry_run_eligible")
            ),
            "selected_baseline_for_2326": recommendation["selected_for_2326"],
            "fallback_baseline": recommendation["fallback_baseline"],
            "next_task": task_route["next_task"],
            "portfolio_baseline_source_decision_cli": True,
            "portfolio_baseline_candidate_matrix_generated": True,
            "portfolio_baseline_source_feasibility_matrix_generated": True,
            "portfolio_baseline_pit_reproducibility_audit_generated": True,
            "portfolio_baseline_risk_matrix_generated": True,
            "recommended_exposure_cap_simulation_baseline_generated": True,
            "exposure_cap_2326_task_route_generated": True,
            "simulation_executed": False,
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_executed": False,
            "data_quality_gate_rationale": (
                "aits validate-data not applicable because this task only reads "
                "static config and prior research outputs"
            ),
            "cached_market_data_consumed": False,
            "portfolio_runtime_artifacts_consumed": False,
            **SAFETY_FIELDS,
        }
    )


def write_portfolio_baseline_source_decision_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    risk_rows: Sequence[Mapping[str, Any]],
    field_rows: Sequence[Mapping[str, Any]],
    recommendation: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    paths = {
        "summary": output_dir / "portfolio_baseline_source_decision_summary.json",
        "candidate_matrix": output_dir / "portfolio_baseline_candidate_matrix.json",
        "feasibility_matrix": output_dir
        / "portfolio_baseline_source_feasibility_matrix.json",
        "pit_audit": output_dir / "portfolio_baseline_pit_reproducibility_audit.json",
        "risk_matrix": output_dir / "portfolio_baseline_risk_matrix.json",
        "field_requirement_matrix": output_dir
        / "portfolio_baseline_field_requirement_matrix.json",
        "recommendation": output_dir / "portfolio_baseline_recommendation.json",
        "recommended_baseline": output_dir
        / "recommended_exposure_cap_simulation_baseline.json",
        "task_route": output_dir / "exposure_cap_2326_task_route.json",
        "safety_boundary": output_dir / "portfolio_baseline_source_safety_boundary.json",
        "report_doc": docs_root / "portfolio_baseline_source_decision_report.md",
        "candidate_matrix_doc": docs_root / "portfolio_baseline_candidate_matrix.md",
        "pit_audit_doc": docs_root / "portfolio_baseline_pit_reproducibility_audit.md",
        "recommended_baseline_doc": docs_root
        / "recommended_exposure_cap_simulation_baseline.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["candidate_matrix"], {**dict(summary), "rows": list(candidate_rows)})
    write_json(
        paths["feasibility_matrix"],
        {**dict(summary), "rows": list(feasibility_rows)},
    )
    write_json(paths["pit_audit"], {**dict(summary), "rows": list(pit_rows)})
    write_json(paths["risk_matrix"], {**dict(summary), "rows": list(risk_rows)})
    write_json(
        paths["field_requirement_matrix"],
        {**dict(summary), "rows": list(field_rows)},
    )
    write_json(paths["recommendation"], dict(recommendation))
    write_json(paths["recommended_baseline"], dict(recommendation))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_source_decision_report(
            summary,
            candidate_rows,
            feasibility_rows,
            recommendation,
            task_route,
        ),
    )
    write_markdown(
        paths["candidate_matrix_doc"],
        _render_candidate_matrix_doc(summary, candidate_rows, feasibility_rows),
    )
    write_markdown(
        paths["pit_audit_doc"],
        _render_pit_audit_doc(summary, pit_rows),
    )
    write_markdown(
        paths["recommended_baseline_doc"],
        _render_recommended_baseline_doc(summary, recommendation, task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _candidate_row(
    *,
    baseline_id: str,
    baseline_type: str,
    source_path: str,
    source_available: bool,
    history_start: str,
    history_end: str,
    coverage_ratio: float,
    target_assets_supported: Sequence[str],
    exposure_fields_available: Sequence[str],
    rebalance_calendar_available: bool,
    turnover_fields_available: bool,
    pit_status: str,
    reproducibility_status: str,
    maintenance_cost: str,
    implementation_cost: str,
    interpretation_quality: str,
    research_stage_fit: str,
    privacy_or_account_risk: str,
    recommended_usage: str,
) -> dict[str, Any]:
    return {
        "baseline_id": baseline_id,
        "baseline_type": baseline_type,
        "source_path": source_path,
        "source_available": source_available,
        "history_start": history_start,
        "history_end": history_end,
        "coverage_ratio": coverage_ratio,
        "target_assets_supported": list(target_assets_supported),
        "exposure_fields_available": list(exposure_fields_available),
        "rebalance_calendar_available": rebalance_calendar_available,
        "turnover_fields_available": turnover_fields_available,
        "pit_status": pit_status,
        "reproducibility_status": reproducibility_status,
        "maintenance_cost": maintenance_cost,
        "implementation_cost": implementation_cost,
        "interpretation_quality": interpretation_quality,
        "research_stage_fit": research_stage_fit,
        "privacy_or_account_risk": privacy_or_account_risk,
        "recommended_usage": recommended_usage,
        "recommended_now": baseline_id == "static_etf_allocation_baseline",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_source_decision_report(
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
    recommendation: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    lines = [
        "# Portfolio Baseline Source Decision",
        "",
        "TRADING-2325 只回答后续 exposure-cap simulation 应绑定哪一种 "
        "portfolio / exposure baseline source。TRADING-2324 已达到 "
        "`SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE`，但 synthetic "
        "observe-only baseline 不能被解释为真实组合层 simulation 结论。",
        "",
        f"- status: `{summary['status']}`",
        f"- selected_market_regime: `{summary['selected_market_regime']}`",
        f"- source_binding_dry_run_readiness_status: "
        f"`{summary['source_binding_dry_run_readiness_status']}`",
        f"- source_binding_portfolio_source_mode: "
        f"`{summary['source_binding_portfolio_source_mode']}`",
        f"- data_quality_status: `{summary['data_quality_status']}`",
        f"- aits_validate_data_executed: `{summary['aits_validate_data_executed']}`",
        f"- selected_baseline_for_2326: `{summary['selected_baseline_for_2326']}`",
        f"- fallback_baseline: `{summary['fallback_baseline']}`",
        f"- next_task: `{summary['next_task']}`",
        "- simulation_executed: `False`",
        "- promotion_allowed: `False`",
        "- paper_shadow_allowed: `False`",
        "- production_allowed: `False`",
        "- broker_action: `none`",
        "",
        "## Data Quality",
        "",
        "`aits validate-data` 不适用：本任务只读取 static config 和 prior research "
        "outputs。TRADING-2326 如果消费 cached market data，必须重新执行 cached-data "
        "quality gate。",
        "",
        "## Baseline Candidates",
        "",
        "|baseline|source_available|dry_run_eligible|pit_status|recommended_usage|",
        "|---|---|---|---|---|",
    ]
    feasible_by_id = {row["baseline_id"]: row for row in feasibility_rows}
    for row in candidate_rows:
        feasible = feasible_by_id[row["baseline_id"]]
        lines.append(
            f"|`{row['baseline_id']}`|`{row['source_available']}`|"
            f"`{feasible['dry_run_eligible']}`|`{row['pit_status']}`|"
            f"{row['recommended_usage']}|"
        )
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "短期推荐 `static_etf_allocation_baseline`，因为它比 synthetic baseline "
            "更接近组合研究，同时仍可复现、隐私风险低、工程成本低。中期应补 "
            "`dynamic_strategy_target_exposure_baseline` 的 PIT target exposure "
            "artifact。`actual_holdings_derived_baseline` 当前不推荐，因为会把研究层"
            "与真实账户、现金、税务和 broker 执行边界混在一起。",
            "",
            f"- selected_for_2326: `{recommendation['selected_for_2326']}`",
            f"- baseline_source_mode: `{recommendation['baseline_source_mode']}`",
            f"- pit_status: `{recommendation['pit_status']}`",
            f"- route: `{task_route['next_task']}`",
            "",
        ]
    )
    return "\n".join(lines)


def _render_candidate_matrix_doc(
    summary: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    feasibility_rows: Sequence[Mapping[str, Any]],
) -> str:
    feasible_by_id = {row["baseline_id"]: row for row in feasibility_rows}
    lines = [
        "# Portfolio Baseline Candidate Matrix",
        "",
        f"- status: `{summary['status']}`",
        f"- selected_baseline_for_2326: `{summary['selected_baseline_for_2326']}`",
        "",
        "|baseline|type|source_available|coverage|pit|privacy|2326|",
        "|---|---|---|---|---|---|---|",
    ]
    for row in candidate_rows:
        feasible = feasible_by_id[row["baseline_id"]]
        lines.append(
            f"|`{row['baseline_id']}`|`{row['baseline_type']}`|"
            f"`{row['source_available']}`|`{row['coverage_ratio']}`|"
            f"`{row['pit_status']}`|`{row['privacy_or_account_risk']}`|"
            f"`{feasible['recommended_for_2326']}`|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_pit_audit_doc(
    summary: Mapping[str, Any],
    pit_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Portfolio Baseline PIT Reproducibility Audit",
        "",
        f"- status: `{summary['status']}`",
        "",
        "|baseline|pit_status|replayable|artifact_hash|source_version|recommendation|",
        "|---|---|---|---|---|---|",
    ]
    for row in pit_rows:
        lines.append(
            f"|`{row['baseline_id']}`|`{row['pit_status']}`|"
            f"`{row['replayable']}`|`{row['artifact_hash_available']}`|"
            f"`{row['source_version_available']}`|{row['recommendation']}|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_recommended_baseline_doc(
    summary: Mapping[str, Any],
    recommendation: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Recommended Exposure-Cap Simulation Baseline",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_for_2326: `{recommendation['selected_for_2326']}`",
            f"- fallback_baseline: `{recommendation['fallback_baseline']}`",
            f"- baseline_source_mode: `{recommendation['baseline_source_mode']}`",
            f"- pit_status: `{recommendation['pit_status']}`",
            f"- reproducibility_status: `{recommendation['reproducibility_status']}`",
            f"- allowed_simulation_mode: `{recommendation['allowed_simulation_mode']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "TRADING-2326 应先使用 static ETF allocation baseline 做 source-bound "
            "exposure-cap dry-run；synthetic observe-only baseline 只作为 fallback。"
            "dynamic target baseline 保留为中期路线，actual holdings-derived baseline "
            "仅能作为 future owner-only manual reference。",
            "",
        ]
    )


def _select_baseline_for_2326(rows: Sequence[Mapping[str, Any]]) -> str:
    by_id = {str(row["baseline_id"]): row for row in rows}
    if by_id.get("static_etf_allocation_baseline", {}).get("dry_run_eligible"):
        return "static_etf_allocation_baseline"
    if by_id.get("dynamic_strategy_target_exposure_baseline", {}).get(
        "dry_run_eligible"
    ):
        return "dynamic_strategy_target_exposure_baseline"
    if by_id.get("synthetic_observe_only_baseline", {}).get("dry_run_eligible"):
        return "synthetic_observe_only_baseline"
    return "portfolio_baseline_source_remediation"


def _selection_reason(selected: str) -> str:
    if selected == "static_etf_allocation_baseline":
        return (
            "static ETF allocation is reproducible, privacy-safe, and more "
            "interpretable than synthetic observe-only for the first 2326 dry-run"
        )
    if selected == "dynamic_strategy_target_exposure_baseline":
        return "dynamic target exposure is eligible and closest to strategy decisions"
    if selected == "synthetic_observe_only_baseline":
        return "only synthetic observe-only baseline is eligible; conclusions stay proxy-only"
    return "no baseline source is eligible; remediate portfolio baseline sources first"


def _reproducibility_for(
    baseline_id: str,
    feasibility_rows: Sequence[Mapping[str, Any]],
) -> str:
    row = _row_by_baseline(feasibility_rows, baseline_id)
    if baseline_id == "static_etf_allocation_baseline" and row:
        return "REPLAYABLE_CONFIG_VERSIONED"
    if baseline_id == "synthetic_observe_only_baseline" and row:
        return "REPRODUCIBLE_SYSTEM_RULE"
    if baseline_id == "dynamic_strategy_target_exposure_baseline" and row:
        return "PIT_TARGET_EXPOSURE_ARTIFACT_READY"
    return "SOURCE_REMEDIATION_REQUIRED"


def _source_complete(row: Mapping[str, Any]) -> bool:
    baseline_id = str(row["baseline_id"])
    if baseline_id == "synthetic_observe_only_baseline":
        return bool(row["source_available"])
    if baseline_id == "static_etf_allocation_baseline":
        return bool(row["source_available"]) and float(row.get("coverage_ratio", 0.0)) >= 1.0
    if baseline_id == "dynamic_strategy_target_exposure_baseline":
        return (
            bool(row["source_available"])
            and bool(row["rebalance_calendar_available"])
            and bool(row["turnover_fields_available"])
        )
    if baseline_id == "paper_portfolio_advisory_baseline":
        return False
    return False


def _source_binding_date_range(source_binding: Mapping[str, Any]) -> tuple[str, str]:
    portfolio = mapping(source_binding.get("portfolio_baseline_binding"))
    start = str(portfolio.get("coverage_start") or "")
    end = str(portfolio.get("coverage_end") or "")
    if start and end:
        return start, end
    summary = mapping(source_binding.get("summary"))
    value = str(summary.get("actual_requested_date_range") or "")
    if ".." in value:
        left, right = value.split("..", 1)
        return left, right
    return "", ""


def _target_assets(source_binding: Mapping[str, Any]) -> list[str]:
    summary = mapping(source_binding.get("summary"))
    assets = summary.get("target_assets")
    if isinstance(assets, str):
        return [item.strip() for item in assets.split(",") if item.strip()]
    if isinstance(assets, Sequence):
        return [str(item) for item in assets]
    return ["QQQ", "SPY", "SMH"]


def _static_config_path(portfolio_config_dir: Path | None) -> Path | None:
    if portfolio_config_dir is None:
        return None
    assets_path = portfolio_config_dir / "assets.yaml"
    if assets_path.exists():
        return assets_path
    return portfolio_config_dir if portfolio_config_dir.is_file() else assets_path


def _static_supported_assets(config: Mapping[str, Any]) -> set[str]:
    assets = mapping(config.get("assets"))
    supported: set[str] = set()
    for symbol, payload in assets.items():
        item = mapping(payload)
        if item.get("default_weight") is not None and str(symbol).upper() != "CASH":
            supported.add(str(symbol).upper())
    return supported


def _source_version(path: Path) -> str:
    if not path.exists() or path.suffix.lower() not in {".yaml", ".yml"}:
        return ""
    payload = safe_load_yaml_path(path) or {}
    metadata = mapping(payload.get("policy_metadata")) or mapping(payload.get("metadata"))
    return str(metadata.get("version") or payload.get("version") or "")


def _load_yaml_if_exists(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists() or path.suffix.lower() not in {".yaml", ".yml"}:
        return {}
    payload = safe_load_yaml_path(path) or {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _config_history_value(config: Mapping[str, Any], field: str) -> str:
    for key in ("history", "coverage", "metadata", "policy_metadata"):
        value = mapping(config.get(key)).get(field)
        if value:
            return str(value)
    return ""


def _row_by_baseline(
    rows: Sequence[Mapping[str, Any]],
    baseline_id: str,
) -> Mapping[str, Any]:
    for row in rows:
        if row.get("baseline_id") == baseline_id:
            return row
    return {}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    banned_values = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
        "paper_shadow_ready",
        "production_decision",
        "rebalance_instruction",
        "target_weight",
    }
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise PortfolioBaselineSourceDecisionError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise PortfolioBaselineSourceDecisionError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise PortfolioBaselineSourceDecisionError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise PortfolioBaselineSourceDecisionError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise PortfolioBaselineSourceDecisionError(
                    f"{name} opens {forbidden}"
                )
        for value in item.values():
            if isinstance(value, str) and value in banned_values:
                raise PortfolioBaselineSourceDecisionError(
                    f"{name} emits banned value {value}"
                )


def _walk_mappings(payload: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        found.append(payload)
        for value in payload.values():
            found.extend(_walk_mappings(value))
    elif isinstance(payload, list | tuple):
        for value in payload:
            found.extend(_walk_mappings(value))
    return found
