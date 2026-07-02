from __future__ import annotations

import csv
import hashlib
import json
import re
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
    records,
    round_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2328_DYNAMIC_TARGET_BASELINE_PREPARATION"
REPORT_TYPE = "dynamic_target_baseline_preparation"
ARTIFACT_ROLE = "dynamic_target_baseline_preparation"
MODE = "dynamic_target_baseline_preparation"
STATUS = "DYNAMIC_TARGET_BASELINE_PREPARATION_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_PREPARATION_ONLY"

DEFAULT_DIAGNOSTICS_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_vs_no_cap_diagnostics_review"
)
DEFAULT_STATIC_DRY_RUN_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "source_bound_exposure_cap_dry_run_static_etf_baseline"
)
DEFAULT_BASELINE_DECISION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "portfolio_baseline_source_decision"
)
DEFAULT_SOURCE_BINDING_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_simulation_source_binding"
)
DEFAULT_SIMULATION_POLICY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_mechanics_simulation"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

DEFAULT_CANDIDATE_ARTIFACT_ROOTS = (
    PROJECT_ROOT / "outputs" / "research_trends",
    PROJECT_ROOT / "outputs" / "etf_portfolio",
    PROJECT_ROOT / "outputs" / "paper_portfolio",
    PROJECT_ROOT / "outputs" / "portfolio_advisory",
    PROJECT_ROOT / "outputs" / "daily_reports",
    PROJECT_ROOT / "config" / "etf_portfolio",
    PROJECT_ROOT / "config" / "paper_portfolio_v1.yaml",
    PROJECT_ROOT / "docs" / "research" / "report_registry.md",
    PROJECT_ROOT / "config" / "report_registry.yaml",
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "source_preparation_only": True,
    "simulation_executed": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "real_portfolio_effect": "none",
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

SOURCE_TYPES = {
    "dynamic_strategy_target_exposure",
    "paper_portfolio_advisory_target",
    "daily_advisory_target_exposure",
    "etf_allocation_dynamic_output",
    "risk_budget_target_exposure",
    "manual_review_only_target_exposure",
    "unknown_candidate_source",
}

REQUIRED_FIELDS = (
    "date",
    "target_asset",
    "target_exposure",
    "risk_asset_exposure",
    "asset_weight",
    "cash_weight",
    "as_of_timestamp",
    "decision_timestamp",
    "valid_from",
    "valid_until",
    "rebalance_flag",
    "cooldown_state",
    "source_artifact_hash",
    "signal_source_id",
    "advisory_id",
)

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "date": ("date", "source_date", "as_of_date", "decision_date", "signal_date"),
    "target_asset": ("target_asset", "asset", "ticker", "symbol"),
    "target_exposure": (
        "target_exposure",
        "recommended_exposure",
        "model_exposure",
        "baseline_exposure",
        "median_target_weight",
        "mean_target_weight",
        "recommended_weight",
        "weight",
    ),
    "risk_asset_exposure": ("risk_asset_exposure", "risk_exposure", "equity_exposure"),
    "asset_weight": (
        "asset_weight",
        "recommended_weight",
        "median_target_weight",
        "mean_target_weight",
        "weight",
    ),
    "cash_weight": ("cash_weight", "cash_allocation", "cash_exposure"),
    "as_of_timestamp": ("as_of_timestamp", "known_at", "created_at", "generated_at"),
    "decision_timestamp": ("decision_timestamp", "decision_time", "signal_timestamp"),
    "valid_from": ("valid_from", "effective_from", "start_date"),
    "valid_until": ("valid_until", "effective_until", "end_date", "expires_at"),
    "rebalance_flag": ("rebalance_flag", "rebalance_required", "rebalance_due"),
    "cooldown_state": ("cooldown_state", "cooldown_mode", "risk_cap_cooldown_state"),
    "source_artifact_hash": ("source_artifact_hash", "source_hash", "artifact_hash", "sha256"),
    "signal_source_id": ("signal_source_id", "source_id", "candidate_id", "strategy_id"),
    "advisory_id": ("advisory_id", "daily_advisory_id", "review_id"),
}

TIMESTAMP_FIELDS = ("as_of_timestamp", "decision_timestamp")
VALIDITY_FIELDS = ("valid_from", "valid_until")
REBALANCE_FIELDS = ("rebalance_flag",)
EXPOSURE_FIELDS = ("target_exposure", "risk_asset_exposure", "asset_weight")
DATE_FIELD_CANDIDATES = FIELD_ALIASES["date"] + VALIDITY_FIELDS
TARGET_ASSET_VALUES = {"QQQ", "SPY", "SMH", "SOXX", "TQQQ", "SGOV", "CASH"}

READINESS_DYNAMIC_ROUTE = (
    "TRADING-2329_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline"
)
READINESS_REMEDIATION_ROUTE = "TRADING-2329_Dynamic_Target_Baseline_Source_Remediation"
READINESS_SCHEMA_ADAPTER_ROUTE = "TRADING-2329_Dynamic_Target_Baseline_Schema_Adapter"
READINESS_STATIC_ONLY_ROUTE = "TRADING-2329_Continue_Static_Baseline_Only"


class DynamicTargetBaselinePreparationError(ValueError):
    pass


def run_dynamic_target_baseline_preparation(
    *,
    diagnostics_dir: Path = DEFAULT_DIAGNOSTICS_ROOT,
    static_dry_run_dir: Path = DEFAULT_STATIC_DRY_RUN_ROOT,
    baseline_decision_dir: Path = DEFAULT_BASELINE_DECISION_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    candidate_artifact_roots: str | Sequence[Path] | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicTargetBaselinePreparationError(
            f"dynamic target baseline preparation only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_target_baseline_preparation_inputs(
        diagnostics_dir=diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        baseline_decision_dir=baseline_decision_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    roots = parse_candidate_artifact_roots(candidate_artifact_roots)
    target_assets = _target_assets(inputs)
    inventory_rows = build_dynamic_target_source_inventory(
        candidate_roots=roots,
        target_assets=target_assets,
    )
    pit_rows = build_dynamic_target_pit_replayability_audit(inventory_rows)
    gap_rows = build_dynamic_target_source_gap_matrix(
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        required_assets=target_assets,
    )
    reference = build_alignment_reference(inputs, target_assets)
    risk_cap_alignment_rows = build_dynamic_target_risk_cap_alignment_readiness(
        inventory_rows=inventory_rows,
        reference=reference,
    )
    market_data_alignment_rows = build_dynamic_target_market_data_alignment_readiness(
        inventory_rows=inventory_rows,
        reference=reference,
        data_quality_status=_static_dry_run_data_quality_status(inputs),
    )
    candidate_rows = build_dynamic_target_baseline_candidate_matrix(
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        gap_rows=gap_rows,
        risk_cap_alignment_rows=risk_cap_alignment_rows,
        market_data_alignment_rows=market_data_alignment_rows,
    )
    recommended_spec = build_recommended_dynamic_target_baseline_spec(
        candidate_rows=candidate_rows,
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        gap_rows=gap_rows,
    )
    field_rows = build_dynamic_target_field_coverage_matrix(
        inventory_rows=inventory_rows,
        selected_source_id=str(recommended_spec.get("selected_source_id") or ""),
    )
    readiness = build_dynamic_target_baseline_2329_readiness_matrix(
        recommended_spec=recommended_spec,
        candidate_rows=candidate_rows,
        gap_rows=gap_rows,
        pit_rows=pit_rows,
        risk_cap_alignment_rows=risk_cap_alignment_rows,
        market_data_alignment_rows=market_data_alignment_rows,
        inventory_rows=inventory_rows,
    )
    task_route = build_dynamic_target_baseline_2329_task_route(
        readiness=readiness,
        candidate_rows=candidate_rows,
        gap_rows=gap_rows,
        inventory_rows=inventory_rows,
    )
    safety_boundary = build_dynamic_target_baseline_safety_boundary(generated_at)
    summary = build_dynamic_target_baseline_preparation_summary(
        generated_at=generated_at,
        diagnostics_dir=diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        baseline_decision_dir=baseline_decision_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        candidate_roots=roots,
        inputs=inputs,
        inventory_rows=inventory_rows,
        gap_rows=gap_rows,
        pit_rows=pit_rows,
        candidate_rows=candidate_rows,
        recommended_spec=recommended_spec,
        readiness=readiness,
        task_route=task_route,
    )
    paths = write_dynamic_target_baseline_preparation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        inventory_rows=inventory_rows,
        gap_rows=gap_rows,
        pit_rows=pit_rows,
        field_rows=field_rows,
        risk_cap_alignment_rows=risk_cap_alignment_rows,
        market_data_alignment_rows=market_data_alignment_rows,
        candidate_rows=candidate_rows,
        recommended_spec=recommended_spec,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_dynamic_target_baseline_preparation_inputs(
    *,
    diagnostics_dir: Path,
    static_dry_run_dir: Path,
    baseline_decision_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    diagnostics = load_trading_2327_diagnostics_outputs(diagnostics_dir)
    static_dry_run = load_trading_2326_static_dry_run_outputs(static_dry_run_dir)
    baseline_decision = load_trading_2325_baseline_decision_outputs(baseline_decision_dir)
    source_binding = load_trading_2324_source_binding_outputs(source_binding_dir)
    simulation_policy = load_trading_2323_policy_outputs(simulation_policy_dir)
    return {
        "diagnostics": diagnostics,
        "static_dry_run": static_dry_run,
        "baseline_decision": baseline_decision,
        "source_binding": source_binding,
        "simulation_policy": simulation_policy,
    }


def load_trading_2327_diagnostics_outputs(diagnostics_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": diagnostics_dir / "exposure_cap_diagnostics_review_summary.json",
        "dynamic_baseline": diagnostics_dir / "dynamic_baseline_readiness_recommendation.json",
        "decision": diagnostics_dir / "exposure_cap_diagnostics_decision_matrix.json",
        "task_route": diagnostics_dir / "exposure_cap_2328_task_route.json",
        "boundary": diagnostics_dir / "diagnostics_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2327 diagnostics review")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2327 {key}", payload)
    summary = mapping(payloads["summary"])
    task_route = mapping(payloads["task_route"])
    decision = mapping(payloads["decision"])
    expected_recommendation = "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    decision_recommendation = str(
        decision.get("overall_recommendation") or expected_recommendation
    )
    if (
        str(summary.get("overall_recommendation")) != expected_recommendation
        or decision_recommendation != expected_recommendation
    ):
        raise DynamicTargetBaselinePreparationError(
            "TRADING-2328 requires TRADING-2327 recommendation "
            "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
        )
    expected_next_task = "TRADING-2328_Dynamic_Target_Baseline_Preparation"
    if str(summary.get("next_task")) != expected_next_task or str(
        task_route.get("next_task")
    ) != expected_next_task:
        raise DynamicTargetBaselinePreparationError(
            "TRADING-2328 requires TRADING-2327 route to dynamic target baseline preparation"
        )
    return {"source_dir": str(diagnostics_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2326_static_dry_run_outputs(static_dry_run_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": static_dry_run_dir / "source_bound_static_etf_dry_run_summary.json",
        "baseline_source_report": static_dry_run_dir / "static_etf_baseline_source_report.json",
        "baseline_schedule": static_dry_run_dir / "static_etf_baseline_exposure_schedule.json",
        "risk_cap_alignment": static_dry_run_dir / "risk_cap_trigger_alignment_matrix.json",
        "dry_run_result": static_dry_run_dir
        / "source_bound_static_etf_exposure_cap_dry_run_result.json",
        "comparison": static_dry_run_dir / "exposure_cap_vs_no_cap_static_etf_comparison.json",
        "binding_day_matrix": static_dry_run_dir / "exposure_cap_binding_day_matrix.json",
        "data_quality_report": static_dry_run_dir / "exposure_cap_data_quality_report.json",
        "interpretation_boundary": static_dry_run_dir
        / "exposure_cap_simulation_interpretation_boundary.json",
        "task_route": static_dry_run_dir / "exposure_cap_2327_task_route.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2326 static ETF dry-run")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2326 {key}", payload)
    summary = mapping(payloads["summary"])
    if (
        summary.get("dry_run_only") is not True
        or summary.get("artifact_role") != "source_bound_static_etf_exposure_cap_dry_run"
    ):
        raise DynamicTargetBaselinePreparationError(
            "TRADING-2328 requires TRADING-2326 static dry-run summary"
        )
    return {"source_dir": str(static_dry_run_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2325_baseline_decision_outputs(baseline_decision_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": baseline_decision_dir / "portfolio_baseline_source_decision_summary.json",
        "candidate_matrix": baseline_decision_dir / "portfolio_baseline_candidate_matrix.json",
        "feasibility_matrix": baseline_decision_dir
        / "portfolio_baseline_source_feasibility_matrix.json",
        "pit_audit": baseline_decision_dir / "portfolio_baseline_pit_reproducibility_audit.json",
        "risk_matrix": baseline_decision_dir / "portfolio_baseline_risk_matrix.json",
        "field_requirement_matrix": baseline_decision_dir
        / "portfolio_baseline_field_requirement_matrix.json",
        "recommendation": baseline_decision_dir / "portfolio_baseline_recommendation.json",
        "recommended_baseline": baseline_decision_dir
        / "recommended_exposure_cap_simulation_baseline.json",
        "task_route": baseline_decision_dir / "exposure_cap_2326_task_route.json",
        "safety_boundary": baseline_decision_dir
        / "portfolio_baseline_source_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2325 baseline decision")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2325 {key}", payload)
    selected = str(mapping(payloads["recommended_baseline"]).get("selected_for_2326"))
    if selected != "static_etf_allocation_baseline":
        raise DynamicTargetBaselinePreparationError(
            "TRADING-2328 requires TRADING-2325 to have selected static ETF baseline"
        )
    return {"source_dir": str(baseline_decision_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2324_source_binding_outputs(source_binding_dir: Path) -> dict[str, Any]:
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
        "dry_run_readiness": source_binding_dir / "source_bound_dry_run_simulation_readiness.json",
        "safety_boundary": source_binding_dir / "source_bound_dry_run_safety_boundary.json",
        "next_task_route": source_binding_dir / "exposure_cap_simulation_next_task_route.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2324 source binding")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2324 {key}", payload)
    return {"source_dir": str(source_binding_dir), "paths": _string_paths(paths), **payloads}


def load_trading_2323_policy_outputs(simulation_policy_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": simulation_policy_dir / "exposure_cap_mechanics_simulation_summary.json",
        "readiness": simulation_policy_dir / "exposure_cap_simulation_readiness_matrix.json",
        "metric_contract": simulation_policy_dir / "exposure_cap_simulation_metric_contract.json",
        "safety_boundary": simulation_policy_dir
        / "exposure_cap_simulation_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2323 exposure-cap policy context")
    optional_policy = simulation_policy_dir / "exposure_cap_policy.json"
    if optional_policy.exists():
        payloads["policy"] = _load_json(optional_policy)
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return {"source_dir": str(simulation_policy_dir), "paths": _string_paths(paths), **payloads}


def parse_candidate_artifact_roots(
    candidate_artifact_roots: str | Sequence[Path] | None,
) -> list[Path]:
    if candidate_artifact_roots is None:
        return list(DEFAULT_CANDIDATE_ARTIFACT_ROOTS)
    if isinstance(candidate_artifact_roots, str):
        raw_roots = [item.strip() for item in candidate_artifact_roots.split(",") if item.strip()]
        return [_resolve_project_path(item) for item in raw_roots]
    return [_resolve_project_path(path) for path in candidate_artifact_roots]


def build_dynamic_target_source_inventory(
    *,
    candidate_roots: Sequence[Path],
    target_assets: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _candidate_paths(candidate_roots):
        candidate = _load_candidate_artifact(path)
        if candidate is None:
            continue
        row = _candidate_inventory_row(path, candidate, target_assets)
        if not _candidate_relevant(path, candidate, mapping(row.get("field_coverage"))):
            continue
        if row["source_type"] in SOURCE_TYPES:
            rows.append(row)
    if not any(row["source_type"] == "dynamic_strategy_target_exposure" for row in rows):
        rows.insert(0, _missing_dynamic_source_row())
    return sorted(rows, key=lambda row: (not bool(row["source_available"]), str(row["source_id"])))


def build_dynamic_target_pit_replayability_audit(
    inventory_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        source_available = bool(source.get("source_available"))
        as_of_available = _has_field(source, "as_of_timestamp")
        decision_available = _has_field(source, "decision_timestamp")
        valid_from_available = _has_field(source, "valid_from")
        valid_until_available = _has_field(source, "valid_until")
        generated_at_available = bool(source.get("generated_at_available"))
        hash_available = bool(source.get("source_hash"))
        registry_available = bool(source.get("registry_reference_available"))
        replayable = (
            source_available
            and bool(source.get("history_start"))
            and bool(source.get("history_end"))
            and int(source.get("record_count") or 0) > 0
        )

        source_type = str(source.get("source_type") or "")
        if not source_available:
            pit_status = "BLOCKED"
            recommendation = "SOURCE_REMEDIATION_REQUIRED"
            known_at = "not_available"
        elif source_type == "manual_review_only_target_exposure":
            pit_status = "MANUAL_REFERENCE_ONLY"
            recommendation = "NOT_RECOMMENDED_FOR_2329"
            known_at = "manual_reference_only"
        elif as_of_available and decision_available and valid_from_available and hash_available:
            pit_status = "STRICT_PIT_READY" if valid_until_available else "PIT_APPROXIMATION_READY"
            recommendation = (
                "ELIGIBLE_FOR_2329_DYNAMIC_DRY_RUN"
                if valid_until_available
                else "ELIGIBLE_WITH_PIT_CAVEAT"
            )
            known_at = "strict_known_at" if valid_until_available else "known_at_validity_caveat"
        elif replayable and hash_available:
            pit_status = "REPLAYABLE_BUT_NOT_STRICT_PIT"
            recommendation = "ELIGIBLE_WITH_PIT_CAVEAT"
            known_at = "replayable_without_full_timestamp_schema"
        elif replayable:
            pit_status = "CURRENT_ARTIFACT_ONLY"
            recommendation = "SOURCE_REMEDIATION_REQUIRED"
            known_at = "history_without_hash_or_known_at"
        else:
            pit_status = "CURRENT_ARTIFACT_ONLY"
            recommendation = "SOURCE_REMEDIATION_REQUIRED"
            known_at = "current_artifact_only"

        rows.append(
            {
                "source_id": source["source_id"],
                "pit_status": pit_status,
                "as_of_timestamp_available": as_of_available,
                "decision_timestamp_available": decision_available,
                "valid_from_available": valid_from_available,
                "valid_until_available": valid_until_available,
                "generated_at_available": generated_at_available,
                "source_artifact_hash_available": hash_available,
                "registry_reference_available": registry_available,
                "replayable": replayable,
                "known_at_semantics": known_at,
                "latency_model_available": bool(source.get("latency_model_available")),
                "rebalance_timing_available": bool(source.get("rebalance_fields_available")),
                "lookahead_risk": _lookahead_risk_label(pit_status),
                "revision_risk": "LOW" if hash_available else "HIGH",
                "recommendation": recommendation,
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_target_source_gap_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    required_assets: Sequence[str],
) -> list[dict[str, Any]]:
    pit_by_source = {str(row["source_id"]): row for row in pit_rows}
    rows: list[dict[str, Any]] = []
    if not inventory_rows:
        rows.append(
            _gap_row(
                "no_candidate_source",
                "missing_source",
                "BLOCKING",
                remediation_action="继续 static baseline only，直到提供 dynamic target source",
                blocking=True,
            )
        )
        return rows

    for source in inventory_rows:
        source_id = str(source["source_id"])
        source_available = bool(source.get("source_available"))
        if not source_available:
            rows.append(
                _gap_row(
                    source_id,
                    "missing_source",
                    "BLOCKING",
                    missing_fields=["dynamic_target_source"],
                    remediation_action="补充可 replay 的 dynamic target exposure artifact",
                    blocking=True,
                )
            )
            continue

        if not source.get("exposure_fields_available"):
            rows.append(
                _gap_row(
                    source_id,
                    "missing_exposure_fields",
                    "BLOCKING",
                    missing_fields=["target_exposure", "risk_asset_exposure", "asset_weight"],
                    remediation_action="建立 schema adapter 或补充 target exposure 字段",
                    blocking=True,
                )
            )
        missing_timestamps = [
            field for field in TIMESTAMP_FIELDS if not _has_field(source, field)
        ]
        if missing_timestamps:
            rows.append(
                _gap_row(
                    source_id,
                    "missing_timestamp_fields",
                    "BLOCKING",
                    missing_fields=missing_timestamps,
                    remediation_action="补充 as-of / decision timestamp known-at 字段",
                    blocking=True,
                )
            )
        missing_validity = [
            field for field in VALIDITY_FIELDS if not _has_field(source, field)
        ]
        if missing_validity:
            rows.append(
                _gap_row(
                    source_id,
                    "missing_validity_fields",
                    "WARNING",
                    missing_fields=missing_validity,
                    remediation_action="补充 valid-from / valid-until 生效区间",
                    blocking=False,
                )
            )
        if not source.get("rebalance_fields_available"):
            rows.append(
                _gap_row(
                    source_id,
                    "missing_rebalance_fields",
                    "WARNING",
                    missing_fields=["rebalance_flag"],
                    remediation_action="补充 rebalance timing 或显式 no-rebalance policy",
                    blocking=False,
                )
            )
        supported_assets = set(_strings(source.get("target_assets_supported")))
        missing_assets = [asset for asset in required_assets if asset not in supported_assets]
        if missing_assets:
            rows.append(
                _gap_row(
                    source_id,
                    "missing_asset_coverage",
                    "BLOCKING",
                    missing_assets=missing_assets,
                    remediation_action="补齐 QQQ/SPY/SMH dynamic exposure 覆盖",
                    blocking=True,
                )
            )
        if not source.get("horizons_supported"):
            rows.append(
                _gap_row(
                    source_id,
                    "missing_horizon_coverage",
                    "WARNING",
                    missing_horizons=["10d"],
                    remediation_action="声明 dynamic target horizon 或绑定默认研究 horizon",
                    blocking=False,
                )
            )
        if not source.get("registry_reference_available"):
            rows.append(
                _gap_row(
                    source_id,
                    "missing_registry_reference",
                    "WARNING",
                    missing_registry_reference=True,
                    remediation_action="在 report registry 或 source manifest 中登记 source",
                    blocking=False,
                )
            )
        if source.get("source_type") == "unknown_candidate_source":
            rows.append(
                _gap_row(
                    source_id,
                    "schema_incompatible",
                    "BLOCKING",
                    remediation_action="先建立 explicit dynamic target baseline schema adapter",
                    blocking=True,
                )
            )
        if not (source.get("history_start") and source.get("history_end")):
            rows.append(
                _gap_row(
                    source_id,
                    "source_not_replayable",
                    "BLOCKING",
                    missing_time_coverage="history_start/history_end",
                    remediation_action="补充可 replay 的历史 target exposure 序列",
                    blocking=True,
                )
            )
        pit = pit_by_source.get(source_id, {})
        if str(pit.get("pit_status")) in {"CURRENT_ARTIFACT_ONLY", "BLOCKED"}:
            rows.append(
                _gap_row(
                    source_id,
                    "source_not_pit",
                    "BLOCKING",
                    remediation_action="补充 known-at timestamp、validity 和 artifact hash",
                    blocking=True,
                )
            )
    return rows


def build_dynamic_target_field_coverage_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str = "",
) -> list[dict[str, Any]]:
    source_candidates_by_field: dict[str, list[str]] = {field: [] for field in REQUIRED_FIELDS}
    available_by_field: dict[str, bool] = {field: False for field in REQUIRED_FIELDS}
    available_selected: dict[str, bool] = {field: False for field in REQUIRED_FIELDS}

    for source in inventory_rows:
        if not source.get("source_available"):
            continue
        source_id = str(source.get("source_id"))
        for field in REQUIRED_FIELDS:
            if _has_field(source, field):
                available_by_field[field] = True
                source_candidates_by_field[field].append(source_id)
                if selected_source_id and source_id == selected_source_id:
                    available_selected[field] = True

    rows: list[dict[str, Any]] = []
    for field in REQUIRED_FIELDS:
        blocking = field in {
            "date",
            "target_asset",
            "target_exposure",
            "as_of_timestamp",
            "decision_timestamp",
            "source_artifact_hash",
        }
        rows.append(
            {
                "field_name": field,
                "required_for_2329": True,
                "required_for_full_simulation": True,
                "source_candidates": sorted(source_candidates_by_field[field]),
                "available": available_by_field[field],
                "available_in_recommended_source": available_selected[field],
                "fallback_allowed": not blocking,
                "fallback_policy": (
                    "no fallback allowed for PIT dynamic dry-run"
                    if blocking
                    else "allowed as explicit caveat before full simulation"
                ),
                "blocking_if_missing": blocking,
                **_safety_subset(),
            }
        )
    return rows


def build_alignment_reference(
    inputs: Mapping[str, Any],
    target_assets: Sequence[str],
) -> dict[str, Any]:
    static_dry_run = mapping(inputs.get("static_dry_run"))
    alignment_rows = records(mapping(static_dry_run.get("risk_cap_alignment")).get("rows"))
    schedule_rows = records(mapping(static_dry_run.get("baseline_schedule")).get("rows"))
    binding_rows = records(mapping(static_dry_run.get("binding_day_matrix")).get("rows"))
    rows = alignment_rows or schedule_rows or binding_rows
    dates = sorted({str(row.get("date") or row.get("source_date") or "") for row in rows if row})
    dates = [value for value in dates if value]
    assets = sorted(
        {
            str(row.get("target_asset") or row.get("asset") or "").upper()
            for row in rows
            if row.get("target_asset") or row.get("asset")
        }
        or set(target_assets)
    )
    horizons = sorted({str(row.get("horizon")) for row in rows if row.get("horizon")})
    return {
        "risk_cap_trigger_series_available": bool(rows),
        "market_data_available": bool(rows),
        "risk_cap_trigger_date_coverage": _date_coverage(dates),
        "market_data_coverage_start": dates[0] if dates else "",
        "market_data_coverage_end": dates[-1] if dates else "",
        "required_assets": list(target_assets),
        "risk_cap_assets": assets,
        "market_assets": assets,
        "horizons": horizons or ["10d"],
        "reference_record_count": len(rows),
    }


def build_dynamic_target_risk_cap_alignment_readiness(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    reference: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        alignment = _source_alignment(source, reference)
        rows.append(
            {
                "source_id": source["source_id"],
                "risk_cap_trigger_series_available": bool(
                    reference.get("risk_cap_trigger_series_available")
                ),
                "risk_cap_trigger_date_coverage": reference.get(
                    "risk_cap_trigger_date_coverage", ""
                ),
                "dynamic_target_date_coverage": alignment["dynamic_coverage"],
                "overlap_start": alignment["overlap_start"],
                "overlap_end": alignment["overlap_end"],
                "overlap_record_count": alignment["overlap_record_count"],
                "asset_overlap": alignment["asset_overlap"],
                "horizon_overlap": alignment["horizon_overlap"],
                "timestamp_alignment_status": alignment["timestamp_status"],
                "calendar_alignment_status": alignment["calendar_status"],
                "alignment_blockers": alignment["blockers"],
                "alignment_readiness_status": alignment["status"],
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_target_market_data_alignment_readiness(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    reference: Mapping[str, Any],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    market_assets = set(_strings(reference.get("market_assets")))
    for source in inventory_rows:
        if not source.get("source_available"):
            status = "ALIGNMENT_BLOCKED_BY_SOURCE_MISSING"
            missing_assets = list(_strings(reference.get("required_assets")))
            missing_dates = "dynamic target source missing"
            ready = False
        else:
            dynamic_assets = set(_strings(source.get("target_assets_supported")))
            missing_assets = sorted(asset for asset in market_assets if asset not in dynamic_assets)
            missing_dates = (
                ""
                if source.get("history_start") and source.get("history_end")
                else "dynamic target date coverage missing"
            )
            if missing_dates:
                status = "ALIGNMENT_BLOCKED_BY_DATE_COVERAGE"
                ready = False
            elif missing_assets:
                status = "ALIGNMENT_BLOCKED_BY_ASSET_COVERAGE"
                ready = False
            elif not _has_timestamp_schema(source):
                status = "ALIGNMENT_BLOCKED_BY_TIMESTAMP_SCHEMA"
                ready = False
            else:
                status = "ALIGNMENT_READY"
                ready = True
        rows.append(
            {
                "source_id": source["source_id"],
                "market_data_available": bool(reference.get("market_data_available")),
                "market_data_coverage_start": reference.get("market_data_coverage_start", ""),
                "market_data_coverage_end": reference.get("market_data_coverage_end", ""),
                "dynamic_target_coverage_start": source.get("history_start", ""),
                "dynamic_target_coverage_end": source.get("history_end", ""),
                "price_return_alignment_ready": ready,
                "missing_market_assets": missing_assets,
                "missing_market_dates": missing_dates,
                "data_quality_status": data_quality_status,
                "alignment_readiness_status": status,
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_target_baseline_candidate_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment_rows: Sequence[Mapping[str, Any]],
    market_data_alignment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pit_by_source = {str(row["source_id"]): row for row in pit_rows}
    risk_by_source = {str(row["source_id"]): row for row in risk_cap_alignment_rows}
    market_by_source = {str(row["source_id"]): row for row in market_data_alignment_rows}
    blocking_by_source = _blocking_gap_categories_by_source(gap_rows)
    warning_by_source = _warning_gap_categories_by_source(gap_rows)

    rows: list[dict[str, Any]] = []
    for index, source in enumerate(inventory_rows, start=1):
        source_id = str(source["source_id"])
        pit = pit_by_source.get(source_id, {})
        risk = risk_by_source.get(source_id, {})
        market = market_by_source.get(source_id, {})
        blockers = blocking_by_source.get(source_id, set())
        warnings = warning_by_source.get(source_id, set())
        pit_status = str(pit.get("pit_status") or "BLOCKED")
        risk_status = str(risk.get("alignment_readiness_status") or "")
        market_status = str(market.get("alignment_readiness_status") or "")
        timestamp_score = _timestamp_quality_score(pit_status)
        field_score = _field_coverage_score(source)
        alignment_score = _alignment_score(risk_status, market_status)
        coverage_ratio = 1.0 if risk.get("overlap_record_count", 0) else 0.0
        recommended = (
            bool(source.get("source_available"))
            and pit_status in {"STRICT_PIT_READY", "PIT_APPROXIMATION_READY"}
            and not blockers
            and risk_status in {"ALIGNMENT_READY", "ALIGNMENT_READY_WITH_WARNINGS"}
            and market_status in {"ALIGNMENT_READY", "ALIGNMENT_READY_WITH_WARNINGS"}
        )
        reason = (
            "ready for 2329 dynamic dry-run"
            if recommended
            else _candidate_rejection_reason(blockers, warnings, pit_status, risk_status)
        )
        rows.append(
            {
                "baseline_candidate_id": f"dynamic_baseline_candidate_{index:02d}",
                "source_id": source_id,
                "baseline_type": _baseline_type_for_source(source),
                "source_available": bool(source.get("source_available")),
                "pit_status": pit_status,
                "replayable": bool(pit.get("replayable")),
                "coverage_ratio": round_float(coverage_ratio),
                "asset_coverage_score": 1.0
                if "missing_asset_coverage" not in blockers
                and bool(source.get("target_assets_supported"))
                else 0.0,
                "timestamp_quality_score": timestamp_score,
                "field_coverage_score": field_score,
                "alignment_readiness_score": alignment_score,
                "interpretation_quality": _interpretation_quality(source, pit_status),
                "maintenance_cost": _maintenance_cost(source),
                "research_value": _research_value(source),
                "privacy_risk": _privacy_risk(source),
                "recommended_for_2329": recommended,
                "recommendation_reason": reason,
                **_safety_subset(),
            }
        )
    return rows


def build_recommended_dynamic_target_baseline_spec(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    inventory_by_source = {str(row["source_id"]): row for row in inventory_rows}
    pit_by_source = {str(row["source_id"]): row for row in pit_rows}
    recommended = [
        row for row in candidate_rows if row.get("recommended_for_2329") is True
    ]
    if recommended:
        selected = sorted(
            recommended,
            key=lambda row: (
                float(row.get("alignment_readiness_score") or 0.0),
                float(row.get("field_coverage_score") or 0.0),
                float(row.get("timestamp_quality_score") or 0.0),
            ),
            reverse=True,
        )[0]
        source = inventory_by_source[str(selected["source_id"])]
        pit = pit_by_source[str(selected["source_id"])]
        return clean_for_yaml(
            {
                "schema_version": f"{REPORT_TYPE}.recommended_spec.v1",
                "report_type": REPORT_TYPE,
                "artifact_role": "recommended_dynamic_target_baseline_spec",
                "task_id": TASK_ID,
                "selected_dynamic_baseline_id": selected["baseline_candidate_id"],
                "selected_source_id": source["source_id"],
                "selected_baseline_type": selected["baseline_type"],
                "source_path": source.get("source_path", ""),
                "source_hash": source.get("source_hash", ""),
                "pit_status": pit.get("pit_status", ""),
                "replayability_status": "REPLAYABLE" if pit.get("replayable") else "NOT_REPLAYABLE",
                "coverage_start": source.get("history_start", ""),
                "coverage_end": source.get("history_end", ""),
                "target_assets": source.get("target_assets_supported", []),
                "horizons": source.get("horizons_supported", []),
                "required_field_mapping": {
                    field: field for field in REQUIRED_FIELDS if _has_field(source, field)
                },
                "timestamp_policy": "use source as-of and decision timestamps",
                "rebalance_policy": "use source rebalance flag; missing values remain caveats",
                "latency_policy": "no additional latency model applied in TRADING-2328",
                "fallback_policy": "fail closed; no synthetic dynamic target baseline",
                "2329_allowed": True,
                "2329_blockers": [],
                "interpretation_boundary": (
                    "source preparation only; TRADING-2329 still must run dry-run before "
                    "any policy interpretation"
                ),
                **_safety_subset(),
            }
        )

    blockers = _summary_blockers(gap_rows, inventory_rows)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.recommended_spec.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": "recommended_dynamic_target_baseline_spec",
            "task_id": TASK_ID,
            "selected_dynamic_baseline_id": None,
            "selected_source_id": None,
            "selected_baseline_type": None,
            "source_path": "",
            "source_hash": "",
            "pit_status": "BLOCKED",
            "replayability_status": "NOT_REPLAYABLE",
            "coverage_start": "",
            "coverage_end": "",
            "target_assets": [],
            "horizons": [],
            "required_field_mapping": {},
            "timestamp_policy": "blocked until source remediation",
            "rebalance_policy": "blocked until source remediation",
            "latency_policy": "blocked until source remediation",
            "fallback_policy": "continue static baseline only; do not fabricate dynamic source",
            "2329_allowed": False,
            "2329_blockers": blockers,
            "interpretation_boundary": (
                "no dynamic target baseline selected; no dynamic dry-run should be executed"
            ),
            **_safety_subset(),
        }
    )


def build_dynamic_target_baseline_2329_readiness_matrix(
    *,
    recommended_spec: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment_rows: Sequence[Mapping[str, Any]],
    market_data_alignment_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected_source_id = str(recommended_spec.get("selected_source_id") or "")
    blockers = _summary_blockers(gap_rows, inventory_rows)
    warnings = sorted(
        {
            str(row["gap_category"])
            for row in gap_rows
            if row.get("gap_severity") == "WARNING"
        }
    )
    has_available_source = any(row.get("source_available") for row in inventory_rows)
    selected_pit = _row_by_source(pit_rows, selected_source_id)
    selected_risk = _row_by_source(risk_cap_alignment_rows, selected_source_id)
    selected_market = _row_by_source(market_data_alignment_rows, selected_source_id)
    if recommended_spec.get("2329_allowed") is True:
        readiness_status = (
            "DYNAMIC_BASELINE_READY_WITH_WARNINGS"
            if warnings or selected_pit.get("pit_status") == "PIT_APPROXIMATION_READY"
            else "DYNAMIC_BASELINE_READY_FOR_2329"
        )
        source_available = True
        pit_ready = True
        field_ready = True
        risk_ready = str(selected_risk.get("alignment_readiness_status")) in {
            "ALIGNMENT_READY",
            "ALIGNMENT_READY_WITH_WARNINGS",
        }
        market_ready = str(selected_market.get("alignment_readiness_status")) in {
            "ALIGNMENT_READY",
            "ALIGNMENT_READY_WITH_WARNINGS",
        }
    else:
        readiness_status = (
            "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
            if has_available_source
            else "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
        )
        source_available = has_available_source
        pit_ready = any(
            row.get("pit_status") in {"STRICT_PIT_READY", "PIT_APPROXIMATION_READY"}
            for row in pit_rows
        )
        field_ready = not any(
            row.get("gap_category") in {"missing_exposure_fields", "missing_timestamp_fields"}
            and row.get("2329_blocking") is True
            for row in gap_rows
        )
        risk_ready = False
        market_ready = False
        if not blockers and candidate_rows:
            readiness_status = "DYNAMIC_BASELINE_BLOCKED"

    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2329_readiness.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": "dynamic_target_baseline_2329_readiness_matrix",
            "task_id": TASK_ID,
            "readiness_status": readiness_status,
            "selected_dynamic_baseline_id": recommended_spec.get(
                "selected_dynamic_baseline_id"
            ),
            "source_available": source_available,
            "pit_ready_or_acceptable": pit_ready,
            "field_coverage_ready": field_ready,
            "risk_cap_alignment_ready": risk_ready,
            "market_data_alignment_ready": market_ready,
            "policy_compatible": True,
            "data_quality_status": DATA_QUALITY_STATUS,
            "2329_allowed": recommended_spec.get("2329_allowed") is True,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            "candidate_count": len(candidate_rows),
            **_safety_subset(),
        }
    )


def build_dynamic_target_baseline_2329_task_route(
    *,
    readiness: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    readiness_status = str(readiness.get("readiness_status") or "")
    has_source = any(row.get("source_available") for row in inventory_rows)
    schema_adapter_needed = _schema_adapter_needed(gap_rows, inventory_rows)
    if readiness_status == "DYNAMIC_BASELINE_READY_FOR_2329":
        next_task = READINESS_DYNAMIC_ROUTE
        caveat = ""
    elif readiness_status == "DYNAMIC_BASELINE_READY_WITH_WARNINGS":
        next_task = READINESS_DYNAMIC_ROUTE
        caveat = "READINESS_WARNINGS"
    elif schema_adapter_needed:
        next_task = READINESS_SCHEMA_ADAPTER_ROUTE
        caveat = "SCHEMA_ADAPTER_REQUIRED"
    elif not candidate_rows:
        next_task = READINESS_STATIC_ONLY_ROUTE
        caveat = "NO_CANDIDATE_SOURCE_DISCOVERED"
    elif (
        not has_source
        or _has_missing_source_gap(gap_rows)
        or _has_source_remediation_gap(gap_rows)
    ):
        next_task = READINESS_REMEDIATION_ROUTE
        caveat = "MISSING_OR_NOT_REPLAYABLE_DYNAMIC_TARGET_SOURCE"
    else:
        next_task = READINESS_STATIC_ONLY_ROUTE
        caveat = "DYNAMIC_BASELINE_NOT_USABLE"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2329_task_route.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": "dynamic_target_baseline_2329_task_route",
            "task_id": TASK_ID,
            "readiness_status": readiness_status,
            "next_task": next_task,
            "caveat": caveat,
            "2329_allowed": next_task == READINESS_DYNAMIC_ROUTE,
            "simulation_executed": False,
            **_safety_subset(),
        }
    )


def build_dynamic_target_baseline_safety_boundary(generated_at: datetime) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": "dynamic_target_baseline_safety_boundary",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_target_baseline_preparation_summary(
    *,
    generated_at: datetime,
    diagnostics_dir: Path,
    static_dry_run_dir: Path,
    baseline_decision_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    candidate_roots: Sequence[Path],
    inputs: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
    recommended_spec: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    del inputs
    readiness_status = str(readiness.get("readiness_status") or "")
    dynamic_readiness_status = (
        "DYNAMIC_BASELINE_SOURCE_READY"
        if readiness.get("2329_allowed") is True
        else (
            "BLOCKED_BY_DYNAMIC_TARGET_SOURCE_SCHEMA_GAPS"
            if _schema_adapter_needed(gap_rows, inventory_rows)
            else (
            "BLOCKED_BY_MISSING_DYNAMIC_TARGET_SOURCE"
            if _has_missing_source_gap(gap_rows)
            else "BLOCKED_BY_DYNAMIC_TARGET_SOURCE_GAPS"
            )
        )
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.v1",
            "report_type": REPORT_TYPE,
            "title": "Dynamic Target Baseline Preparation",
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "mode": MODE,
            "diagnostics_dir": str(diagnostics_dir),
            "static_dry_run_dir": str(static_dry_run_dir),
            "baseline_decision_dir": str(baseline_decision_dir),
            "source_binding_dir": str(source_binding_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "candidate_artifact_roots": [str(path) for path in candidate_roots],
            "diagnostics_overall_recommendation": "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION",
            "static_baseline_reference": "static_etf_allocation_baseline",
            "static_baseline_reference_only": True,
            "dynamic_target_baseline_preparation_cli": True,
            "dynamic_target_source_inventory_generated": True,
            "dynamic_target_source_gap_matrix_generated": True,
            "dynamic_target_pit_replayability_audit_generated": True,
            "dynamic_target_field_coverage_matrix_generated": True,
            "dynamic_target_risk_cap_alignment_readiness_generated": True,
            "dynamic_target_market_data_alignment_readiness_generated": True,
            "dynamic_target_baseline_candidate_matrix_generated": True,
            "recommended_dynamic_target_baseline_spec_generated": True,
            "dynamic_target_baseline_2329_readiness_matrix_generated": True,
            "dynamic_target_baseline_2329_task_route_generated": True,
            "inventory_source_count": len(inventory_rows),
            "available_source_count": sum(
                1 for row in inventory_rows if row.get("source_available")
            ),
            "blocking_gap_count": sum(1 for row in gap_rows if row.get("2329_blocking") is True),
            "pit_ready_source_count": sum(
                1
                for row in pit_rows
                if row.get("pit_status") in {"STRICT_PIT_READY", "PIT_APPROXIMATION_READY"}
            ),
            "candidate_count": len(candidate_rows),
            "recommended_candidate_count": sum(
                1 for row in candidate_rows if row.get("recommended_for_2329") is True
            ),
            "selected_dynamic_baseline_id": recommended_spec.get(
                "selected_dynamic_baseline_id"
            ),
            "dynamic_target_baseline_readiness_status": dynamic_readiness_status,
            "readiness_status": readiness_status,
            "next_task": task_route.get("next_task"),
            "data_quality_status": DATA_QUALITY_STATUS,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_executed": False,
            "data_quality_gate_rationale": (
                "aits validate-data not applicable because TRADING-2328 only reads "
                "prior validated outputs / static config / registry candidate artifacts"
            ),
            "cached_market_data_consumed": False,
            "dynamic_target_runtime_data_consumed": False,
            "simulation_executed": False,
            **SAFETY_FIELDS,
        }
    )


def write_dynamic_target_baseline_preparation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    field_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment_rows: Sequence[Mapping[str, Any]],
    market_data_alignment_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
    recommended_spec: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    output_payloads = [
        summary,
        *inventory_rows,
        *gap_rows,
        *pit_rows,
        *field_rows,
        *risk_cap_alignment_rows,
        *market_data_alignment_rows,
        *candidate_rows,
        recommended_spec,
        readiness,
        task_route,
        safety_boundary,
    ]
    for index, payload in enumerate(output_payloads):
        _validate_no_unsafe_fields(f"TRADING-2328 output {index}", payload)

    paths = {
        "summary": output_dir / "dynamic_target_baseline_preparation_summary.json",
        "inventory_json": output_dir / "dynamic_target_source_inventory.json",
        "inventory_csv": output_dir / "dynamic_target_source_inventory.csv",
        "gap_json": output_dir / "dynamic_target_source_gap_matrix.json",
        "gap_csv": output_dir / "dynamic_target_source_gap_matrix.csv",
        "pit_json": output_dir / "dynamic_target_pit_replayability_audit.json",
        "pit_csv": output_dir / "dynamic_target_pit_replayability_audit.csv",
        "field_json": output_dir / "dynamic_target_field_coverage_matrix.json",
        "field_csv": output_dir / "dynamic_target_field_coverage_matrix.csv",
        "risk_cap_alignment_json": output_dir
        / "dynamic_target_risk_cap_alignment_readiness.json",
        "risk_cap_alignment_csv": output_dir
        / "dynamic_target_risk_cap_alignment_readiness.csv",
        "market_alignment_json": output_dir
        / "dynamic_target_market_data_alignment_readiness.json",
        "market_alignment_csv": output_dir
        / "dynamic_target_market_data_alignment_readiness.csv",
        "candidate_json": output_dir / "dynamic_target_baseline_candidate_matrix.json",
        "candidate_csv": output_dir / "dynamic_target_baseline_candidate_matrix.csv",
        "recommended_spec": output_dir / "recommended_dynamic_target_baseline_spec.json",
        "readiness": output_dir / "dynamic_target_baseline_2329_readiness_matrix.json",
        "task_route": output_dir / "dynamic_target_baseline_2329_task_route.json",
        "safety_boundary": output_dir / "dynamic_target_baseline_safety_boundary.json",
        "report_doc": docs_root / "dynamic_target_baseline_preparation_report.md",
        "inventory_doc": docs_root / "dynamic_target_source_inventory.md",
        "pit_doc": docs_root / "dynamic_target_pit_replayability_audit.md",
        "risk_cap_doc": docs_root / "dynamic_target_risk_cap_alignment_readiness.md",
        "recommended_doc": docs_root / "recommended_dynamic_target_baseline_spec.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["inventory_json"], {**dict(summary), "rows": list(inventory_rows)})
    write_csv_rows(paths["inventory_csv"], inventory_rows)
    write_json(paths["gap_json"], {**dict(summary), "rows": list(gap_rows)})
    write_csv_rows(paths["gap_csv"], gap_rows)
    write_json(paths["pit_json"], {**dict(summary), "rows": list(pit_rows)})
    write_csv_rows(paths["pit_csv"], pit_rows)
    write_json(paths["field_json"], {**dict(summary), "rows": list(field_rows)})
    write_csv_rows(paths["field_csv"], field_rows)
    write_json(
        paths["risk_cap_alignment_json"],
        {**dict(summary), "rows": list(risk_cap_alignment_rows)},
    )
    write_csv_rows(paths["risk_cap_alignment_csv"], risk_cap_alignment_rows)
    write_json(
        paths["market_alignment_json"],
        {**dict(summary), "rows": list(market_data_alignment_rows)},
    )
    write_csv_rows(paths["market_alignment_csv"], market_data_alignment_rows)
    write_json(paths["candidate_json"], {**dict(summary), "rows": list(candidate_rows)})
    write_csv_rows(paths["candidate_csv"], candidate_rows)
    write_json(paths["recommended_spec"], dict(recommended_spec))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["report_doc"],
        _render_preparation_report(summary, inventory_rows, gap_rows, readiness, task_route),
    )
    write_markdown(paths["inventory_doc"], _render_inventory_doc(summary, inventory_rows, gap_rows))
    write_markdown(paths["pit_doc"], _render_pit_doc(summary, pit_rows))
    write_markdown(
        paths["risk_cap_doc"],
        _render_risk_cap_alignment_doc(
            summary,
            risk_cap_alignment_rows,
            market_data_alignment_rows,
        ),
    )
    write_markdown(
        paths["recommended_doc"],
        _render_recommended_spec_doc(summary, recommended_spec, readiness, task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _candidate_inventory_row(
    path: Path,
    candidate: Mapping[str, Any],
    target_assets: Sequence[str],
) -> dict[str, Any]:
    rows = records(candidate.get("rows"))
    field_names = set(_strings(candidate.get("field_names")))
    field_coverage = _field_coverage(field_names)
    field_coverage["source_artifact_hash"] = True
    source_type = _classify_source(path, candidate, field_coverage)
    dates = _candidate_dates(rows)
    target_assets_supported = sorted(
        set(_candidate_assets(rows, field_names)) & (TARGET_ASSET_VALUES | set(target_assets))
    )
    if not target_assets_supported and source_type == "paper_portfolio_advisory_target":
        target_assets_supported = list(target_assets)
    horizons = sorted(_candidate_horizons(rows, field_names))
    source_hash = _file_hash(path)
    generated_at_available = "generated_at" in field_names or bool(candidate.get("generated_at"))
    return {
        "source_id": _source_id_for_path(path),
        "source_type": source_type,
        "source_path": str(_relative_display_path(path)),
        "source_available": True,
        "source_hash": source_hash,
        "artifact_role": str(candidate.get("artifact_role") or source_type),
        "candidate_baseline_type": _candidate_baseline_type(source_type),
        "history_start": dates[0] if dates else "",
        "history_end": dates[-1] if dates else "",
        "record_count": int(candidate.get("record_count") or len(rows)),
        "target_assets_supported": target_assets_supported,
        "horizons_supported": horizons,
        "timestamp_fields_available": [
            field for field in TIMESTAMP_FIELDS if field_coverage.get(field)
        ],
        "exposure_fields_available": [
            field for field in EXPOSURE_FIELDS if field_coverage.get(field)
        ],
        "rebalance_fields_available": [
            field for field in REBALANCE_FIELDS if field_coverage.get(field)
        ],
        "validity_fields_available": [
            field for field in VALIDITY_FIELDS if field_coverage.get(field)
        ],
        "field_coverage": field_coverage,
        "data_quality_status": str(candidate.get("data_quality_status") or "NOT_DECLARED"),
        "generated_at_available": generated_at_available,
        "registry_reference_available": _registry_reference_available(path, candidate),
        "latency_model_available": bool(
            field_names & {"latency_model", "execution_lag", "latency_policy"}
        ),
        **_safety_subset(),
    }


def _missing_dynamic_source_row() -> dict[str, Any]:
    return {
        "source_id": "dynamic_strategy_target_exposure_missing",
        "source_type": "dynamic_strategy_target_exposure",
        "source_path": "",
        "source_available": False,
        "source_hash": "",
        "artifact_role": "dynamic_strategy_target_exposure",
        "candidate_baseline_type": "dynamic_strategy_target_exposure",
        "history_start": "",
        "history_end": "",
        "record_count": 0,
        "target_assets_supported": [],
        "horizons_supported": [],
        "timestamp_fields_available": [],
        "exposure_fields_available": [],
        "rebalance_fields_available": [],
        "validity_fields_available": [],
        "field_coverage": {field: False for field in REQUIRED_FIELDS},
        "data_quality_status": "SOURCE_MISSING",
        "generated_at_available": False,
        "registry_reference_available": False,
        "latency_model_available": False,
        **_safety_subset(),
    }


def _load_candidate_artifact(path: Path) -> dict[str, Any] | None:
    if not path.exists() or path.is_dir():
        return None
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            raw = _load_json(path)
        elif suffix in {".yaml", ".yml"}:
            raw_yaml = safe_load_yaml_path(path) or {}
            raw = dict(raw_yaml) if isinstance(raw_yaml, Mapping) else {"rows": raw_yaml}
        elif suffix == ".csv":
            raw = {"rows": _load_csv_sample(path)}
        elif suffix == ".jsonl":
            raw = {"rows": _load_jsonl_sample(path)}
        elif suffix == ".md":
            raw = {"rows": [], "artifact_role": "manual_registry_reference"}
        else:
            return None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None
    _validate_safety_flags_only(f"candidate source {path}", raw)
    rows = _extract_candidate_rows(raw)
    field_names = _collect_field_names(raw, rows)
    return {
        "artifact_role": str(mapping(raw).get("artifact_role") or ""),
        "data_quality_status": str(mapping(raw).get("data_quality_status") or ""),
        "generated_at": str(mapping(raw).get("generated_at") or ""),
        "rows": rows,
        "field_names": sorted(field_names),
        "record_count": len(rows),
    }


def _candidate_paths(candidate_roots: Sequence[Path]) -> list[Path]:
    suffixes = {".json", ".yaml", ".yml", ".csv", ".jsonl", ".md"}
    terms = (
        "target_exposure",
        "target-exposure",
        "target_weight",
        "target-weight",
        "target_vs_actual_position_path",
        "daily_consensus_weights",
        "daily_candidate_targets",
        "daily_advisory_actions",
        "advisory",
        "paper_portfolio",
        "risk_budget",
        "position",
        "model_target_portfolio",
    )
    found: dict[str, Path] = {}
    for root in candidate_roots:
        resolved = _resolve_project_path(root)
        if not resolved.exists():
            continue
        paths = [resolved] if resolved.is_file() else resolved.rglob("*")
        for path in paths:
            if path.is_dir() or path.suffix.lower() not in suffixes:
                continue
            normalized = str(path).replace("\\", "/").lower()
            if any(
                skip in normalized
                for skip in ("/.git/", "/__pycache__/", "/validation_runtime/")
            ):
                continue
            if REPORT_TYPE in normalized:
                continue
            if not any(term in normalized for term in terms):
                continue
            try:
                if path.stat().st_size > 5_000_000:
                    continue
            except OSError:
                continue
            found[str(path.resolve()).lower()] = path
            if len(found) >= 300:
                break
    return list(found.values())


def _extract_candidate_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    payload = mapping(raw)
    for key in ("rows", "records", "signals", "decisions", "actions", "candidate_targets"):
        rows = records(payload.get(key))
        if rows:
            return rows[:5000]
    nested_rows: list[dict[str, Any]] = []
    for value in payload.values():
        if isinstance(value, list):
            nested_rows.extend(dict(item) for item in value if isinstance(item, Mapping))
    if nested_rows:
        return nested_rows[:5000]
    return [payload] if payload else []


def _collect_field_names(raw: Any, rows: Sequence[Mapping[str, Any]]) -> set[str]:
    field_names: set[str] = set()
    for row in rows[:1000]:
        field_names.update(_flatten_keys(row))
    if isinstance(raw, Mapping):
        field_names.update(_flatten_keys(raw, max_depth=2))
    return {str(field) for field in field_names if str(field)}


def _flatten_keys(payload: Mapping[str, Any], *, max_depth: int = 3) -> set[str]:
    keys: set[str] = set()
    if max_depth <= 0:
        return keys
    for key, value in payload.items():
        text_key = str(key)
        keys.add(text_key)
        if isinstance(value, Mapping):
            keys.update(_flatten_keys(value, max_depth=max_depth - 1))
    return keys


def _field_coverage(field_names: set[str]) -> dict[str, bool]:
    normalized = {_normalize_field_name(field) for field in field_names}
    coverage: dict[str, bool] = {}
    for field, aliases in FIELD_ALIASES.items():
        coverage[field] = any(_normalize_field_name(alias) in normalized for alias in aliases)
    wide_weight_prefixes = (
        "target_weight_",
        "target_exposure_",
        "median_target_weight_",
        "mean_target_weight_",
    )
    wide_weight_fields = any(name.startswith(wide_weight_prefixes) for name in normalized)
    if wide_weight_fields:
        coverage["target_exposure"] = True
        coverage["asset_weight"] = True
        coverage["target_asset"] = True
    return coverage


def _classify_source(
    path: Path,
    candidate: Mapping[str, Any],
    field_coverage: Mapping[str, bool],
) -> str:
    haystack = " ".join(
        [
            str(path).replace("\\", "/").lower(),
            str(candidate.get("artifact_role") or "").lower(),
            " ".join(_strings(candidate.get("field_names"))).lower(),
        ]
    )
    if "dynamic_strategy_target_exposure" in haystack or (
        "dynamic" in haystack and field_coverage.get("target_exposure")
    ):
        return "dynamic_strategy_target_exposure"
    if "daily_advisory" in haystack:
        return "daily_advisory_target_exposure"
    if "paper_portfolio" in haystack or "paper-shadow" in haystack:
        return "paper_portfolio_advisory_target"
    if "allocation" in haystack and "dynamic" in haystack:
        return "etf_allocation_dynamic_output"
    if "risk_budget" in haystack:
        return "risk_budget_target_exposure"
    if "manual" in haystack or "owner_review" in haystack:
        return "manual_review_only_target_exposure"
    return "unknown_candidate_source"


def _candidate_relevant(
    path: Path,
    candidate: Mapping[str, Any],
    field_coverage: Mapping[str, bool],
) -> bool:
    normalized = str(path).replace("\\", "/").lower()
    explicit_terms = (
        "target_exposure",
        "target-exposure",
        "target_weight",
        "target-weight",
        "target_vs_actual_position_path",
        "daily_consensus_weights",
        "daily_candidate_targets",
        "daily_advisory_actions",
        "position_advisory",
        "model_target_portfolio",
        "risk_budget",
    )
    if any(term in normalized for term in explicit_terms):
        return True
    if "advisory" in normalized and (
        field_coverage.get("target_exposure") or field_coverage.get("asset_weight")
    ):
        return True
    if "allocation" in normalized and field_coverage.get("target_exposure"):
        return True
    artifact_role = str(candidate.get("artifact_role") or "").lower()
    return "target_exposure" in artifact_role or "advisory" in artifact_role


def _candidate_dates(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    dates: set[str] = set()
    for row in rows:
        for field in DATE_FIELD_CANDIDATES:
            value = row.get(field)
            date_value = _date_prefix(value)
            if date_value:
                dates.add(date_value)
    return sorted(dates)


def _candidate_assets(rows: Sequence[Mapping[str, Any]], field_names: set[str]) -> list[str]:
    assets: set[str] = set()
    for row in rows:
        for field in FIELD_ALIASES["target_asset"]:
            value = row.get(field)
            if isinstance(value, str) and value.upper() in TARGET_ASSET_VALUES:
                assets.add(value.upper())
    for field in field_names:
        upper = str(field).upper()
        if upper in TARGET_ASSET_VALUES:
            assets.add(upper)
        for asset in TARGET_ASSET_VALUES:
            if upper.endswith(f"_{asset}") or upper.startswith(f"{asset}_"):
                assets.add(asset)
    assets.discard("CASH")
    return sorted(assets)


def _candidate_horizons(rows: Sequence[Mapping[str, Any]], field_names: set[str]) -> list[str]:
    horizons: set[str] = set()
    for row in rows:
        value = row.get("horizon") or row.get("lookahead_horizon")
        if value:
            horizons.add(str(value))
    for field in field_names:
        if str(field).lower() in {"horizon", "lookahead_horizon"}:
            horizons.add("declared")
    return sorted(horizons)


def _source_alignment(source: Mapping[str, Any], reference: Mapping[str, Any]) -> dict[str, Any]:
    if not source.get("source_available"):
        return {
            "dynamic_coverage": "",
            "overlap_start": "",
            "overlap_end": "",
            "overlap_record_count": 0,
            "asset_overlap": [],
            "horizon_overlap": [],
            "timestamp_status": "MISSING_SOURCE",
            "calendar_status": "MISSING_SOURCE",
            "blockers": ["source_missing"],
            "status": "ALIGNMENT_BLOCKED_BY_SOURCE_MISSING",
        }
    start = str(source.get("history_start") or "")
    end = str(source.get("history_end") or "")
    ref_start, ref_end = _split_coverage(str(reference.get("risk_cap_trigger_date_coverage") or ""))
    overlap_start = max(start, ref_start) if start and ref_start else ""
    overlap_end = min(end, ref_end) if end and ref_end else ""
    date_overlap = bool(overlap_start and overlap_end and overlap_start <= overlap_end)
    dynamic_assets = set(_strings(source.get("target_assets_supported")))
    ref_assets = set(_strings(reference.get("required_assets")))
    asset_overlap = sorted(dynamic_assets & ref_assets)
    dynamic_horizons = set(_strings(source.get("horizons_supported")))
    ref_horizons = set(_strings(reference.get("horizons")))
    horizon_overlap = sorted(dynamic_horizons & ref_horizons) or (
        ["declared_default"] if dynamic_horizons or ref_horizons else []
    )
    blockers: list[str] = []
    if not date_overlap:
        blockers.append("date_coverage")
    if ref_assets and not ref_assets.issubset(dynamic_assets):
        blockers.append("asset_coverage")
    if not _has_timestamp_schema(source):
        blockers.append("timestamp_schema")
    if "date_coverage" in blockers:
        status = "ALIGNMENT_BLOCKED_BY_DATE_COVERAGE"
    elif "asset_coverage" in blockers:
        status = "ALIGNMENT_BLOCKED_BY_ASSET_COVERAGE"
    elif "timestamp_schema" in blockers:
        status = "ALIGNMENT_BLOCKED_BY_TIMESTAMP_SCHEMA"
    elif not horizon_overlap:
        status = "ALIGNMENT_READY_WITH_WARNINGS"
    else:
        status = "ALIGNMENT_READY"
    return {
        "dynamic_coverage": _date_coverage([start, end]),
        "overlap_start": overlap_start,
        "overlap_end": overlap_end,
        "overlap_record_count": 1 if date_overlap else 0,
        "asset_overlap": asset_overlap,
        "horizon_overlap": horizon_overlap,
        "timestamp_status": (
            "TIMESTAMP_SCHEMA_READY"
            if _has_timestamp_schema(source)
            else "TIMESTAMP_SCHEMA_MISSING"
        ),
        "calendar_status": "CALENDAR_OVERLAP_READY" if date_overlap else "CALENDAR_OVERLAP_MISSING",
        "blockers": blockers,
        "status": status,
    }


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, dict[str, Any]]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicTargetBaselinePreparationError(
            f"{label} required artifacts missing: " + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {"rows": payload}


def _load_csv_sample(path: Path, limit: int = 5000) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for _, row in zip(range(limit), reader, strict=False)]


def _load_jsonl_sample(path: Path, limit: int = 500) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if index >= limit:
                break
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, Mapping):
                rows.append(dict(payload))
    return rows


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    _validate_safety_flags_only(name, payload)
    banned_values = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
        "paper_shadow_ready",
        "production_decision",
    }
    banned_keys = {
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
        "paper_shadow_ready",
        "production_decision",
    }
    for item in _walk_mappings(payload):
        for key in banned_keys:
            if key in item:
                raise DynamicTargetBaselinePreparationError(f"{name} emits banned key {key}")
        for value in item.values():
            if isinstance(value, str) and value in banned_values:
                raise DynamicTargetBaselinePreparationError(
                    f"{name} emits banned value {value}"
                )


def _validate_safety_flags_only(name: str, payload: Any) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise DynamicTargetBaselinePreparationError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise DynamicTargetBaselinePreparationError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise DynamicTargetBaselinePreparationError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise DynamicTargetBaselinePreparationError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise DynamicTargetBaselinePreparationError(f"{name} opens {forbidden}")


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


def _target_assets(inputs: Mapping[str, Any]) -> list[str]:
    for section in ("baseline_decision", "source_binding", "static_dry_run"):
        payload = mapping(mapping(inputs.get(section)).get("summary"))
        assets = _strings(payload.get("target_assets"))
        if assets:
            return assets
    return ["QQQ", "SPY", "SMH"]


def _static_dry_run_data_quality_status(inputs: Mapping[str, Any]) -> str:
    static_dry_run = mapping(inputs.get("static_dry_run"))
    report = mapping(static_dry_run.get("data_quality_report"))
    return str(report.get("data_quality_status") or report.get("status") or "UNKNOWN")


def _has_field(source: Mapping[str, Any], field: str) -> bool:
    coverage = mapping(source.get("field_coverage"))
    return bool(coverage.get(field))


def _has_timestamp_schema(source: Mapping[str, Any]) -> bool:
    return _has_field(source, "as_of_timestamp") and _has_field(source, "decision_timestamp")


def _gap_row(
    source_id: str,
    gap_category: str,
    gap_severity: str,
    *,
    missing_fields: Sequence[str] | None = None,
    missing_time_coverage: str = "",
    missing_assets: Sequence[str] | None = None,
    missing_horizons: Sequence[str] | None = None,
    missing_registry_reference: bool = False,
    remediation_action: str,
    blocking: bool,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "required_for_2329": True,
        "gap_category": gap_category,
        "gap_severity": gap_severity,
        "missing_fields": list(missing_fields or []),
        "missing_time_coverage": missing_time_coverage,
        "missing_assets": list(missing_assets or []),
        "missing_horizons": list(missing_horizons or []),
        "missing_registry_reference": missing_registry_reference,
        "remediation_action": remediation_action,
        "2329_blocking": blocking,
        **_safety_subset(),
    }


def _blocking_gap_categories_by_source(
    gap_rows: Sequence[Mapping[str, Any]],
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for row in gap_rows:
        if row.get("2329_blocking") is True:
            result.setdefault(str(row["source_id"]), set()).add(str(row["gap_category"]))
    return result


def _warning_gap_categories_by_source(
    gap_rows: Sequence[Mapping[str, Any]],
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for row in gap_rows:
        if str(row.get("gap_severity")) == "WARNING":
            result.setdefault(str(row["source_id"]), set()).add(str(row["gap_category"]))
    return result


def _summary_blockers(
    gap_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    blockers = sorted(
        {
            str(row["gap_category"])
            for row in gap_rows
            if row.get("2329_blocking") is True
        }
    )
    if _has_missing_source_gap(gap_rows) and not any(
        row.get("source_available") for row in inventory_rows
    ):
        return ["missing_dynamic_target_source"]
    return blockers or ["dynamic_target_baseline_not_selected"]


def _schema_adapter_needed(
    gap_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
) -> bool:
    available_source_ids = {
        str(row["source_id"]) for row in inventory_rows if row.get("source_available")
    }
    adapter_categories = {
        "missing_exposure_fields",
        "missing_timestamp_fields",
        "schema_incompatible",
    }
    hard_remediation_categories = {
        "missing_source",
        "missing_asset_coverage",
        "source_not_replayable",
        "source_not_pit",
    }
    categories_by_source: dict[str, set[str]] = {}
    for row in gap_rows:
        if (
            row.get("2329_blocking") is True
            and str(row.get("source_id")) in available_source_ids
        ):
            categories_by_source.setdefault(str(row["source_id"]), set()).add(
                str(row["gap_category"])
            )
    for categories in categories_by_source.values():
        if categories & adapter_categories and not categories & hard_remediation_categories:
            return True
    return False


def _has_missing_source_gap(gap_rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(row.get("gap_category") == "missing_source" for row in gap_rows)


def _has_source_remediation_gap(gap_rows: Sequence[Mapping[str, Any]]) -> bool:
    remediation_categories = {
        "missing_asset_coverage",
        "source_not_replayable",
        "source_not_pit",
    }
    return any(
        row.get("2329_blocking") is True
        and row.get("gap_category") in remediation_categories
        for row in gap_rows
    )


def _row_by_source(
    rows: Sequence[Mapping[str, Any]],
    source_id: str,
) -> Mapping[str, Any]:
    for row in rows:
        if str(row.get("source_id")) == source_id:
            return row
    return {}


def _baseline_type_for_source(source: Mapping[str, Any]) -> str:
    source_type = str(source.get("source_type") or "")
    if source_type == "manual_review_only_target_exposure":
        return "manual_reference_only"
    if source_type in {
        "dynamic_strategy_target_exposure",
        "paper_portfolio_advisory_target",
        "daily_advisory_target_exposure",
        "risk_budget_target_exposure",
    }:
        return source_type
    return (
        "manual_reference_only"
        if "manual" in source_type
        else "dynamic_strategy_target_exposure"
    )


def _candidate_baseline_type(source_type: str) -> str:
    if source_type == "manual_review_only_target_exposure":
        return "manual_reference_only"
    return source_type


def _timestamp_quality_score(pit_status: str) -> float:
    if pit_status == "STRICT_PIT_READY":
        return 1.0
    if pit_status == "PIT_APPROXIMATION_READY":
        return 0.75
    if pit_status == "REPLAYABLE_BUT_NOT_STRICT_PIT":
        return 0.5
    return 0.0


def _field_coverage_score(source: Mapping[str, Any]) -> float:
    if not source.get("source_available"):
        return 0.0
    coverage = mapping(source.get("field_coverage"))
    if not coverage:
        return 0.0
    available_count = sum(1 for field in REQUIRED_FIELDS if coverage.get(field))
    return round_float(available_count / len(REQUIRED_FIELDS))


def _alignment_score(risk_status: str, market_status: str) -> float:
    if risk_status == "ALIGNMENT_READY" and market_status == "ALIGNMENT_READY":
        return 1.0
    if "READY_WITH_WARNINGS" in {risk_status, market_status}:
        return 0.75
    return 0.0


def _candidate_rejection_reason(
    blockers: set[str],
    warnings: set[str],
    pit_status: str,
    risk_status: str,
) -> str:
    if blockers:
        return "blocked by " + ",".join(sorted(blockers))
    if pit_status not in {"STRICT_PIT_READY", "PIT_APPROXIMATION_READY"}:
        return f"pit status {pit_status} not eligible"
    if risk_status.startswith("ALIGNMENT_BLOCKED"):
        return f"risk-cap alignment {risk_status}"
    if warnings:
        return "warnings require remediation: " + ",".join(sorted(warnings))
    return "not selected"


def _interpretation_quality(source: Mapping[str, Any], pit_status: str) -> str:
    if (
        pit_status == "STRICT_PIT_READY"
        and source.get("source_type") == "dynamic_strategy_target_exposure"
    ):
        return "HIGH"
    if pit_status in {"PIT_APPROXIMATION_READY", "REPLAYABLE_BUT_NOT_STRICT_PIT"}:
        return "MEDIUM"
    return "LOW"


def _maintenance_cost(source: Mapping[str, Any]) -> str:
    source_type = str(source.get("source_type") or "")
    if source_type in {"dynamic_strategy_target_exposure", "risk_budget_target_exposure"}:
        return "MEDIUM"
    if source_type == "paper_portfolio_advisory_target":
        return "MEDIUM_HIGH"
    return "HIGH"


def _research_value(source: Mapping[str, Any]) -> str:
    source_type = str(source.get("source_type") or "")
    if source_type == "dynamic_strategy_target_exposure":
        return "HIGH"
    if source_type in {"paper_portfolio_advisory_target", "daily_advisory_target_exposure"}:
        return "MEDIUM"
    return "LOW"


def _privacy_risk(source: Mapping[str, Any]) -> str:
    source_type = str(source.get("source_type") or "")
    if source_type == "paper_portfolio_advisory_target":
        return "MEDIUM"
    if source_type == "manual_review_only_target_exposure":
        return "HIGH"
    return "LOW"


def _lookahead_risk_label(pit_status: str) -> str:
    if pit_status == "STRICT_PIT_READY":
        return "LOW"
    if pit_status in {"PIT_APPROXIMATION_READY", "REPLAYABLE_BUT_NOT_STRICT_PIT"}:
        return "MEDIUM"
    return "HIGH"


def _registry_reference_available(path: Path, candidate: Mapping[str, Any]) -> bool:
    normalized = str(path).replace("\\", "/").lower()
    if "report_registry" in normalized or "registry" in normalized:
        return True
    return bool(candidate.get("registry_reference") or candidate.get("report_id"))


def _source_id_for_path(path: Path) -> str:
    relative = str(_relative_display_path(path)).replace("\\", "/")
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", relative).strip("_").lower()
    return cleaned[-96:] or "candidate_source"


def _relative_display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return path


def _resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _strings(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _normalize_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _date_prefix(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    return match.group(0) if match else ""


def _date_coverage(dates: Sequence[str]) -> str:
    filtered = sorted({date for date in dates if date})
    if not filtered:
        return ""
    return f"{filtered[0]}..{filtered[-1]}"


def _split_coverage(value: str) -> tuple[str, str]:
    if ".." not in value:
        return "", ""
    start, end = value.split("..", 1)
    return start, end


def _safety_subset() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_preparation_report(
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Target Baseline Preparation",
        "",
        "TRADING-2328 只做 dynamic target baseline source preparation / audit / routing。",
        "TRADING-2327 已推荐先准备 dynamic target baseline，再决定是否进入动态 baseline dry-run。",
        "",
        f"- status: `{summary['status']}`",
        f"- static_baseline_reference: `{summary['static_baseline_reference']}`",
        f"- data_quality_status: `{summary['data_quality_status']}`",
        f"- inventory_source_count: `{summary['inventory_source_count']}`",
        f"- available_source_count: `{summary['available_source_count']}`",
        f"- blocking_gap_count: `{summary['blocking_gap_count']}`",
        f"- readiness_status: `{readiness['readiness_status']}`",
        f"- next_task: `{task_route['next_task']}`",
        "- simulation_executed: `False`",
        "- promotion_allowed: `False`",
        "- paper_shadow_allowed: `False`",
        "- production_allowed: `False`",
        "- broker_action: `none`",
        "",
        "## Data Quality",
        "",
        "`aits validate-data` 不适用：本任务只读取 prior validated outputs、static config "
        "和 registry/candidate artifacts，不读取 cached market data 或 runtime exposure data。",
        "",
        "## Source Inventory",
        "",
        "|source|type|available|pit/gaps|",
        "|---|---|---|---|",
    ]
    gap_by_source: dict[str, list[str]] = {}
    for row in gap_rows:
        gap_by_source.setdefault(str(row["source_id"]), []).append(str(row["gap_category"]))
    for row in inventory_rows:
        source_gaps = ",".join(gap_by_source.get(str(row["source_id"]), []))
        lines.append(
            f"|`{row['source_id']}`|`{row['source_type']}`|"
            f"`{row['source_available']}`|`{source_gaps}`|"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "static ETF baseline 是固定配置 proxy，不能回答 exposure-cap 对系统自身 dynamic "
            "target exposure 的边际影响。TRADING-2328 的结论只决定 2329 是否具备 source "
            "条件；即使 2329 allowed，也仍不是 policy change、paper-shadow 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_inventory_doc(
    summary: Mapping[str, Any],
    inventory_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
) -> str:
    del gap_rows
    lines = [
        "# Dynamic Target Source Inventory",
        "",
        f"- status: `{summary['status']}`",
        f"- available_source_count: `{summary['available_source_count']}`",
        "",
        "|source|type|available|history|assets|fields|",
        "|---|---|---|---|---|---|",
    ]
    for row in inventory_rows:
        fields = sorted(
            set(row.get("timestamp_fields_available", []))
            | set(row.get("exposure_fields_available", []))
        )
        lines.append(
            f"|`{row['source_id']}`|`{row['source_type']}`|`{row['source_available']}`|"
            f"`{row.get('history_start','')}..{row.get('history_end','')}`|"
            f"`{','.join(_strings(row.get('target_assets_supported')) )}`|"
            f"`{','.join(fields)}`|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_pit_doc(summary: Mapping[str, Any], pit_rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Dynamic Target PIT Replayability Audit",
        "",
        f"- status: `{summary['status']}`",
        "",
        "|source|pit_status|replayable|known_at|recommendation|",
        "|---|---|---|---|---|",
    ]
    for row in pit_rows:
        lines.append(
            f"|`{row['source_id']}`|`{row['pit_status']}`|`{row['replayable']}`|"
            f"`{row['known_at_semantics']}`|`{row['recommendation']}`|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_risk_cap_alignment_doc(
    summary: Mapping[str, Any],
    risk_rows: Sequence[Mapping[str, Any]],
    market_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Target Risk-Cap Alignment Readiness",
        "",
        f"- status: `{summary['status']}`",
        "",
        "|source|risk_cap_alignment|market_alignment|overlap|assets|",
        "|---|---|---|---|---|",
    ]
    market_by_source = {str(row["source_id"]): row for row in market_rows}
    for row in risk_rows:
        market = market_by_source.get(str(row["source_id"]), {})
        lines.append(
            f"|`{row['source_id']}`|`{row['alignment_readiness_status']}`|"
            f"`{market.get('alignment_readiness_status','')}`|"
            f"`{row.get('overlap_start','')}..{row.get('overlap_end','')}`|"
            f"`{','.join(_strings(row.get('asset_overlap')) )}`|"
        )
    lines.append("")
    return "\n".join(lines)


def _render_recommended_spec_doc(
    summary: Mapping[str, Any],
    recommended_spec: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Recommended Dynamic Target Baseline Spec",
            "",
            f"- status: `{summary['status']}`",
            "- selected_dynamic_baseline_id: "
            f"`{recommended_spec.get('selected_dynamic_baseline_id')}`",
            f"- selected_source_id: `{recommended_spec.get('selected_source_id')}`",
            f"- pit_status: `{recommended_spec.get('pit_status')}`",
            f"- replayability_status: `{recommended_spec.get('replayability_status')}`",
            f"- 2329_allowed: `{recommended_spec.get('2329_allowed')}`",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "若未选出 source，后续只能进入 source remediation 或 schema adapter，不能执行 "
            "dynamic baseline dry-run，也不能把 static baseline 结论外推为 dynamic strategy 结论。",
            "",
        ]
    )
