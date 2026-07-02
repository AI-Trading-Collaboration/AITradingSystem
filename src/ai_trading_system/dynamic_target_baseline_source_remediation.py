from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_target_baseline_preparation import (
    DEFAULT_DIAGNOSTICS_ROOT,
    DEFAULT_SIMULATION_POLICY_ROOT,
    DEFAULT_SOURCE_BINDING_ROOT,
    DEFAULT_STATIC_DRY_RUN_ROOT,
    FIELD_ALIASES,
    DynamicTargetBaselinePreparationError,
    load_trading_2323_policy_outputs,
    load_trading_2324_source_binding_outputs,
    load_trading_2326_static_dry_run_outputs,
    load_trading_2327_diagnostics_outputs,
    parse_candidate_artifact_roots,
)
from ai_trading_system.dynamic_target_baseline_preparation import (
    _validate_no_unsafe_fields as validate_no_unsafe_fields,
)
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

TASK_ID = "TRADING-2329_DYNAMIC_TARGET_BASELINE_SOURCE_REMEDIATION"
REPORT_TYPE = "dynamic_target_baseline_source_remediation"
ARTIFACT_ROLE = "dynamic_target_baseline_source_remediation"
MODE = "source_remediation"
STATUS = "DYNAMIC_TARGET_BASELINE_SOURCE_REMEDIATION_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_SOURCE_REMEDIATION_ONLY"
BASELINE_SCHEMA_VERSION = "dynamic_target_baseline.v1"

DEFAULT_DYNAMIC_PREPARATION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "dynamic_target_baseline_preparation"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

NEXT_DYNAMIC_DRY_RUN_TASK = (
    "TRADING-2330_Source_Bound_Exposure_Cap_Dry_Run_With_Remediated_Dynamic_Target_Baseline"
)
NEXT_SOURCE_GENERATION_TASK = "TRADING-2330_Dynamic_Target_Baseline_Source_Generation"
NEXT_REGISTRY_BINDING_TASK = "TRADING-2330_Dynamic_Target_Baseline_Registry_Binding"
NEXT_TIMESTAMP_REMEDIATION_TASK = "TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation"
NEXT_STATIC_ONLY_TASK = "TRADING-2330_Continue_Static_Baseline_Only"

SOURCE_FAMILY_ORDER = (
    "dynamic_strategy_target_exposure",
    "paper_portfolio_advisory_target",
    "daily_advisory_target_exposure",
    "etf_allocation_dynamic_output",
    "risk_budget_target_exposure",
    "manual_review_only_target_exposure",
    "unknown_candidate_source",
)

BASELINE_SCHEMA_FIELDS = (
    "baseline_id",
    "source_id",
    "source_type",
    "source_path",
    "source_hash",
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
    "rebalance_timestamp",
    "source_artifact_hash",
    "signal_source_id",
    "advisory_id",
    "generated_at",
    "baseline_schema_version",
    "pit_policy",
    "replayability_status",
    "known_at_semantics",
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
)

FIELD_BLOCKING_DEFAULTS = {
    "baseline_id",
    "source_id",
    "source_type",
    "source_path",
    "source_hash",
    "date",
    "target_asset",
    "target_exposure",
    "source_artifact_hash",
    "baseline_schema_version",
    "pit_policy",
    "replayability_status",
    "known_at_semantics",
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
}


class DynamicTargetBaselineSourceRemediationError(ValueError):
    pass


def run_dynamic_target_baseline_source_remediation(
    *,
    dynamic_preparation_dir: Path = DEFAULT_DYNAMIC_PREPARATION_ROOT,
    diagnostics_dir: Path = DEFAULT_DIAGNOSTICS_ROOT,
    static_dry_run_dir: Path = DEFAULT_STATIC_DRY_RUN_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    candidate_artifact_roots: str | Sequence[Path] | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicTargetBaselineSourceRemediationError(
            f"dynamic target baseline source remediation only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_target_source_remediation_inputs(
        dynamic_preparation_dir=dynamic_preparation_dir,
        diagnostics_dir=diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    roots = parse_candidate_artifact_roots(candidate_artifact_roots)
    inventory_rows = _rows_from_payload(inputs["dynamic_preparation"]["inventory"])
    pit_rows = _rows_from_payload(inputs["dynamic_preparation"]["pit_audit"])
    source_gap_rows = _rows_from_payload(inputs["dynamic_preparation"]["source_gap_matrix"])
    risk_alignment_rows = _rows_from_payload(
        inputs["dynamic_preparation"]["risk_cap_alignment"]
    )
    market_alignment_rows = _rows_from_payload(
        inputs["dynamic_preparation"]["market_data_alignment"]
    )

    schema_contract = build_dynamic_target_baseline_schema_contract(generated_at)
    family_rows = build_dynamic_target_source_family_ranking(
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        source_gap_rows=source_gap_rows,
        risk_alignment_rows=risk_alignment_rows,
        market_alignment_rows=market_alignment_rows,
    )
    gap_rows = build_dynamic_target_gap_to_schema_matrix(
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        source_gap_rows=source_gap_rows,
        schema_contract=schema_contract,
    )
    action_rows = build_dynamic_target_remediation_action_matrix(
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        gap_rows=gap_rows,
        risk_alignment_rows=risk_alignment_rows,
        market_alignment_rows=market_alignment_rows,
    )
    adapter_rows = build_dynamic_target_schema_adapter_spec(
        inventory_rows=inventory_rows,
        action_rows=action_rows,
        gap_rows=gap_rows,
    )
    selected_source_id = _select_wrapper_source(action_rows, family_rows)
    wrapper_rows = build_dynamic_target_baseline_wrapper_artifact(
        selected_source_id=selected_source_id,
        inventory_rows=inventory_rows,
        pit_rows=pit_rows,
        action_rows=action_rows,
        adapter_rows=adapter_rows,
        generated_at=generated_at,
    )
    remediated_rows = build_dynamic_target_remediated_baseline_candidates(
        inventory_rows=inventory_rows,
        action_rows=action_rows,
        selected_source_id=selected_source_id,
    )
    wrapper_validation = build_dynamic_target_wrapper_validation_summary(
        wrapper_rows=wrapper_rows,
        selected_source_id=selected_source_id,
        gap_rows=gap_rows,
        action_rows=action_rows,
    )
    pit_caveat_report = build_dynamic_target_wrapper_pit_caveat_report(
        wrapper_rows=wrapper_rows,
        selected_source_id=selected_source_id,
        pit_rows=pit_rows,
        gap_rows=gap_rows,
    )
    alignment_readiness = build_dynamic_target_wrapper_alignment_readiness(
        selected_source_id=selected_source_id,
        wrapper_rows=wrapper_rows,
        risk_alignment_rows=risk_alignment_rows,
        market_alignment_rows=market_alignment_rows,
    )
    blocked_sources = build_dynamic_target_remediation_blocked_sources(action_rows)
    no_source_report = build_dynamic_target_no_remediable_source_report(
        action_rows=action_rows,
        inventory_rows=inventory_rows,
    )
    generation_requirements = build_dynamic_target_source_generation_requirements(
        gap_rows=gap_rows,
        action_rows=action_rows,
    )
    readiness = build_dynamic_target_2330_readiness_matrix(
        wrapper_validation=wrapper_validation,
        pit_caveat_report=pit_caveat_report,
        alignment_readiness=alignment_readiness,
        action_rows=action_rows,
        selected_source_id=selected_source_id,
    )
    task_route = build_dynamic_target_2330_task_route(
        readiness=readiness,
        action_rows=action_rows,
        gap_rows=gap_rows,
        selected_source_id=selected_source_id,
    )
    safety_boundary = build_dynamic_target_source_remediation_safety_boundary(generated_at)
    summary = build_dynamic_target_source_remediation_summary(
        generated_at=generated_at,
        dynamic_preparation_dir=dynamic_preparation_dir,
        diagnostics_dir=diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        candidate_roots=roots,
        inputs=inputs,
        family_rows=family_rows,
        gap_rows=gap_rows,
        action_rows=action_rows,
        adapter_rows=adapter_rows,
        wrapper_rows=wrapper_rows,
        wrapper_validation=wrapper_validation,
        readiness=readiness,
        task_route=task_route,
    )
    paths = write_dynamic_target_source_remediation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        family_rows=family_rows,
        gap_rows=gap_rows,
        action_rows=action_rows,
        schema_contract=schema_contract,
        adapter_rows=adapter_rows,
        remediated_rows=remediated_rows,
        wrapper_rows=wrapper_rows,
        wrapper_validation=wrapper_validation,
        pit_caveat_report=pit_caveat_report,
        alignment_readiness=alignment_readiness,
        blocked_sources=blocked_sources,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
        no_source_report=no_source_report,
        generation_requirements=generation_requirements,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_dynamic_target_source_remediation_inputs(
    *,
    dynamic_preparation_dir: Path,
    diagnostics_dir: Path,
    static_dry_run_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    dynamic_preparation = load_trading_2328_dynamic_preparation_outputs(
        dynamic_preparation_dir
    )
    diagnostics = load_trading_2327_diagnostics_outputs(diagnostics_dir)
    static_dry_run = load_trading_2326_static_dry_run_outputs(static_dry_run_dir)
    source_binding = load_trading_2324_source_binding_outputs(source_binding_dir)
    simulation_policy = load_trading_2323_policy_outputs(simulation_policy_dir)
    return {
        "dynamic_preparation": dynamic_preparation,
        "diagnostics": diagnostics,
        "static_dry_run": static_dry_run,
        "source_binding": source_binding,
        "simulation_policy": simulation_policy,
    }


def load_trading_2328_dynamic_preparation_outputs(
    dynamic_preparation_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": dynamic_preparation_dir / "dynamic_target_baseline_preparation_summary.json",
        "inventory": dynamic_preparation_dir / "dynamic_target_source_inventory.json",
        "source_gap_matrix": dynamic_preparation_dir / "dynamic_target_source_gap_matrix.json",
        "pit_audit": dynamic_preparation_dir / "dynamic_target_pit_replayability_audit.json",
        "field_coverage": dynamic_preparation_dir / "dynamic_target_field_coverage_matrix.json",
        "risk_cap_alignment": dynamic_preparation_dir
        / "dynamic_target_risk_cap_alignment_readiness.json",
        "market_data_alignment": dynamic_preparation_dir
        / "dynamic_target_market_data_alignment_readiness.json",
        "candidate_matrix": dynamic_preparation_dir
        / "dynamic_target_baseline_candidate_matrix.json",
        "recommended_spec": dynamic_preparation_dir
        / "recommended_dynamic_target_baseline_spec.json",
        "readiness": dynamic_preparation_dir
        / "dynamic_target_baseline_2329_readiness_matrix.json",
        "task_route": dynamic_preparation_dir / "dynamic_target_baseline_2329_task_route.json",
        "safety_boundary": dynamic_preparation_dir
        / "dynamic_target_baseline_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2328 dynamic target preparation")
    for key, payload in payloads.items():
        try:
            validate_no_unsafe_fields(f"TRADING-2328 {key}", payload)
        except DynamicTargetBaselinePreparationError as exc:
            raise DynamicTargetBaselineSourceRemediationError(str(exc)) from exc

    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    expected_status = "DYNAMIC_TARGET_BASELINE_PREPARATION_READY_PROMOTION_BLOCKED"
    expected_readiness = "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
    expected_next_task = "TRADING-2329_Dynamic_Target_Baseline_Source_Remediation"
    if str(summary.get("status")) != expected_status:
        raise DynamicTargetBaselineSourceRemediationError(
            "TRADING-2329 requires TRADING-2328 preparation status"
        )
    if (
        str(summary.get("readiness_status")) != expected_readiness
        or str(readiness.get("readiness_status")) != expected_readiness
        or str(task_route.get("next_task")) != expected_next_task
    ):
        raise DynamicTargetBaselineSourceRemediationError(
            "TRADING-2329 route mismatch: expected dynamic target source remediation"
        )
    if int(summary.get("pit_ready_source_count") or 0) > 0 or int(
        summary.get("recommended_candidate_count") or 0
    ) > 0:
        raise DynamicTargetBaselineSourceRemediationError(
            "TRADING-2329 refuses route with already-ready dynamic baseline source"
        )
    if not _rows_from_payload(payloads["inventory"]):
        raise DynamicTargetBaselineSourceRemediationError(
            "TRADING-2329 requires TRADING-2328 candidate source inventory"
        )
    return {
        "source_dir": str(dynamic_preparation_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def build_dynamic_target_baseline_schema_contract(generated_at: datetime) -> dict[str, Any]:
    field_semantics = {
        "target_exposure": "dynamic strategy intended exposure for asset or risk bucket",
        "risk_asset_exposure": (
            "aggregate risk asset exposure after mapping assets to risk categories"
        ),
        "asset_weight": "asset-level exposure weight if available",
        "cash_weight": "cash or defensive allocation if available",
        "as_of_timestamp": "latest input time used by source artifact",
        "decision_timestamp": "timestamp when dynamic target decision was theoretically available",
        "valid_from": "validity window start for the dynamic target exposure",
        "valid_until": "validity window end for the dynamic target exposure",
        "rebalance_flag": "whether the source decision implies a rebalance event",
        "rebalance_timestamp": "timestamp when rebalance decision would be applied in simulation",
        "source_artifact_hash": "sha256 hash of original source artifact",
        "known_at_semantics": "description of when the system could have known this target",
    }
    return {
        "schema_version": f"{REPORT_TYPE}.schema_contract.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_baseline_schema_contract",
        "task_id": TASK_ID,
        "generated_at": generated_at.isoformat(),
        "baseline_schema_version": BASELINE_SCHEMA_VERSION,
        "required_fields": list(BASELINE_SCHEMA_FIELDS),
        "field_semantics": field_semantics,
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def build_dynamic_target_source_family_ranking(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    source_gap_rows: Sequence[Mapping[str, Any]],
    risk_alignment_rows: Sequence[Mapping[str, Any]],
    market_alignment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pit_by_source = _by_source(pit_rows)
    risk_by_source = _by_source(risk_alignment_rows)
    market_by_source = _by_source(market_alignment_rows)
    source_gap_by_source = _gap_categories_by_source(source_gap_rows, blocking_key="2329_blocking")
    rows: list[dict[str, Any]] = []

    for source_family in SOURCE_FAMILY_ORDER:
        sources = [
            source
            for source in inventory_rows
            if str(source.get("source_type") or "unknown_candidate_source") == source_family
        ]
        if not sources:
            rows.append(
                {
                    "source_family": source_family,
                    "source_count": 0,
                    "best_source_id": "",
                    "field_coverage_score": 0.0,
                    "timestamp_quality_score": 0.0,
                    "pit_quality_score": 0.0,
                    "replayability_score": 0.0,
                    "alignment_score": 0.0,
                    "schema_adapter_effort": "HIGH",
                    "remediation_feasibility": "NOT_REMEDIABLE",
                    "research_value": _research_value_for_family(source_family),
                    "ranking_score": 0.0,
                    "ranking_label": "NOT_REMEDIABLE",
                    "recommended_action": "BLOCK_SOURCE",
                    **_safety_subset(),
                }
            )
            continue

        scored = []
        for source in sources:
            source_id = str(source.get("source_id"))
            pit = pit_by_source.get(source_id, {})
            risk = risk_by_source.get(source_id, {})
            market = market_by_source.get(source_id, {})
            blockers = source_gap_by_source.get(source_id, set())
            field_score = _field_coverage_score(source)
            timestamp_score = _timestamp_score(pit)
            pit_score = _pit_score(pit)
            replay_score = 1.0 if pit.get("replayable") is True else 0.0
            align_score = _alignment_score(risk, market)
            semantics_score = _target_semantics_score(source)
            blocker_penalty = min(len(blockers) * 0.08, 0.4)
            score = round_float(
                field_score * 0.25
                + timestamp_score * 0.2
                + pit_score * 0.15
                + replay_score * 0.15
                + align_score * 0.15
                + semantics_score * 0.1
                - blocker_penalty
            )
            scored.append((score, source))

        scored.sort(key=lambda item: (item[0], str(item[1].get("source_id"))), reverse=True)
        best_score, best = scored[0]
        best_id = str(best.get("source_id"))
        best_pit = pit_by_source.get(best_id, {})
        blockers = source_gap_by_source.get(best_id, set())
        adapter_effort = _schema_adapter_effort(best, blockers)
        label = _ranking_label(best, best_pit, blockers, adapter_effort)
        rows.append(
            {
                "source_family": source_family,
                "source_count": len(sources),
                "best_source_id": best_id,
                "field_coverage_score": _field_coverage_score(best),
                "timestamp_quality_score": _timestamp_score(best_pit),
                "pit_quality_score": _pit_score(best_pit),
                "replayability_score": 1.0 if best_pit.get("replayable") is True else 0.0,
                "alignment_score": _alignment_score(
                    risk_by_source.get(best_id, {}),
                    market_by_source.get(best_id, {}),
                ),
                "schema_adapter_effort": adapter_effort,
                "remediation_feasibility": _remediation_feasibility(label),
                "research_value": _research_value_for_family(source_family),
                "ranking_score": best_score,
                "ranking_label": label,
                "recommended_action": _recommended_action_for_label(label),
                **_safety_subset(),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            -float(row.get("ranking_score") or 0.0),
            SOURCE_FAMILY_ORDER.index(str(row.get("source_family")))
            if str(row.get("source_family")) in SOURCE_FAMILY_ORDER
            else len(SOURCE_FAMILY_ORDER),
        ),
    )


def build_dynamic_target_gap_to_schema_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    source_gap_rows: Sequence[Mapping[str, Any]],
    schema_contract: Mapping[str, Any],
) -> list[dict[str, Any]]:
    pit_by_source = _by_source(pit_rows)
    source_gap_by_source = _gap_categories_by_source(source_gap_rows, blocking_key="2329_blocking")
    warning_gap_by_source = _gap_categories_by_source(source_gap_rows, severity="WARNING")
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        source_id = str(source.get("source_id"))
        source_family = str(source.get("source_type") or "unknown_candidate_source")
        coverage = mapping(source.get("field_coverage"))
        pit = pit_by_source.get(source_id, {})
        blockers = source_gap_by_source.get(source_id, set())
        warnings = warning_gap_by_source.get(source_id, set())
        for field in schema_contract.get("required_fields", BASELINE_SCHEMA_FIELDS):
            field_name = str(field)
            available = _schema_field_available(field_name, source, pit)
            mapping_candidate = _field_mapping_candidate(field_name, coverage)
            fallback_allowed = _fallback_allowed(field_name, source, pit)
            if available:
                severity = "INFO"
                remediation_action = "COPY_OR_BIND_FIELD"
                blocking_wrapper = False
            elif fallback_allowed:
                severity = "WARNING"
                remediation_action = _fallback_policy(field_name)
                blocking_wrapper = False
            else:
                severity = "BLOCKING"
                remediation_action = _blocking_remediation_action(field_name)
                blocking_wrapper = True
            rows.append(
                {
                    "source_id": source_id,
                    "source_family": source_family,
                    "required_field": field_name,
                    "field_available": available,
                    "field_mapping_candidate": mapping_candidate,
                    "fallback_allowed": fallback_allowed,
                    "fallback_policy": _fallback_policy(field_name)
                    if fallback_allowed
                    else "no fallback allowed",
                    "gap_severity": severity,
                    "remediation_action": remediation_action,
                    "blocking_for_wrapper": blocking_wrapper,
                    "blocking_for_2330": _blocking_for_2330(
                        field_name=field_name,
                        available=available,
                        fallback_allowed=fallback_allowed,
                        source=source,
                        blockers=blockers,
                        warnings=warnings,
                    ),
                    **_safety_subset(),
                }
            )
    return rows


def build_dynamic_target_remediation_action_matrix(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    risk_alignment_rows: Sequence[Mapping[str, Any]],
    market_alignment_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pit_by_source = _by_source(pit_rows)
    gap_by_source = _schema_gaps_by_source(gap_rows)
    risk_by_source = _by_source(risk_alignment_rows)
    market_by_source = _by_source(market_alignment_rows)
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        source_id = str(source.get("source_id"))
        source_family = str(source.get("source_type") or "unknown_candidate_source")
        pit = pit_by_source.get(source_id, {})
        gaps = gap_by_source.get(source_id, {})
        blocking_gaps = sorted(gaps.get("blocking", set()))
        warning_gaps = sorted(gaps.get("warning", set()))
        risk = risk_by_source.get(source_id, {})
        market = market_by_source.get(source_id, {})
        status, action = _remediation_status_and_action(
            source=source,
            pit=pit,
            blocking_gaps=blocking_gaps,
            warning_gaps=warning_gaps,
        )
        wrapper_allowed = status in {
            "REMEDIATION_READY",
            "REMEDIATION_READY_WITH_PIT_CAVEAT",
            "REMEDIATION_READY_WITH_SCHEMA_ADAPTER",
        }
        rows.append(
            {
                "source_id": source_id,
                "source_family": source_family,
                "remediation_status": status,
                "remediation_action": action,
                "adapter_required": _adapter_required(blocking_gaps, warning_gaps, source),
                "wrapper_allowed": wrapper_allowed,
                "pit_caveat_required": _pit_caveat_required(pit, warning_gaps),
                "source_generation_required": status
                == "REMEDIATION_BLOCKED_NO_TARGET_EXPOSURE_SEMANTICS",
                "blocking_gaps": blocking_gaps,
                "warnings": _action_warnings(warning_gaps, risk, market),
                "risk_cap_alignment_status": str(
                    risk.get("alignment_readiness_status") or "UNKNOWN"
                ),
                "market_data_alignment_status": str(
                    market.get("alignment_readiness_status") or "UNKNOWN"
                ),
                "next_action": _next_action_for_status(status, action),
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_target_schema_adapter_spec(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    action_by_source = _by_source(action_rows)
    gaps_by_source = _schema_gaps_by_source(gap_rows)
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        source_id = str(source.get("source_id"))
        action = action_by_source.get(source_id, {})
        if not action.get("wrapper_allowed"):
            continue
        source_family = str(source.get("source_type") or "unknown_candidate_source")
        adapter_id = _adapter_id(source_id)
        coverage = mapping(source.get("field_coverage"))
        warning_gaps = gaps_by_source.get(source_id, {}).get("warning", set())
        for field in BASELINE_SCHEMA_FIELDS:
            if field in {
                "promotion_allowed",
                "paper_shadow_allowed",
                "production_allowed",
                "broker_action",
            }:
                source_field = field
                mapping_rule = "force_research_safety_value"
                confidence = "HIGH"
            elif field in {"baseline_id", "baseline_schema_version", "pit_policy"}:
                source_field = ""
                mapping_rule = "derive_wrapper_metadata"
                confidence = "HIGH"
            elif field in {"source_hash", "source_artifact_hash"}:
                source_field = "source_hash"
                mapping_rule = "bind_inventory_source_hash"
                confidence = "HIGH"
            else:
                source_field = _field_mapping_candidate(field, coverage)
                mapping_rule = (
                    "copy_if_non_actionable_research_artifact"
                    if source_field
                    else _fallback_policy(field)
                )
                confidence = "MEDIUM" if source_field else "LOW"
            rows.append(
                {
                    "adapter_id": adapter_id,
                    "source_id": source_id,
                    "source_family": source_family,
                    "source_field": source_field,
                    "target_baseline_field": field,
                    "mapping_rule": mapping_rule,
                    "mapping_confidence": confidence,
                    "fallback_rule": _fallback_policy(field),
                    "pit_caveat": field in warning_gaps
                    or field
                    in {
                        "as_of_timestamp",
                        "decision_timestamp",
                        "valid_from",
                        "valid_until",
                    },
                    "validation_rule": _validation_rule(field),
                    **_safety_subset(),
                }
            )
    return rows


def build_dynamic_target_remediated_baseline_candidates(
    *,
    inventory_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
) -> list[dict[str, Any]]:
    action_by_source = _by_source(action_rows)
    rows: list[dict[str, Any]] = []
    for source in inventory_rows:
        source_id = str(source.get("source_id"))
        action = action_by_source.get(source_id, {})
        if not action.get("wrapper_allowed"):
            continue
        rows.append(
            {
                "baseline_id": _baseline_id(source_id),
                "source_id": source_id,
                "source_family": source.get("source_type", "unknown_candidate_source"),
                "source_path": source.get("source_path", ""),
                "source_hash": source.get("source_hash", ""),
                "adapter_id": _adapter_id(source_id),
                "remediation_status": action.get("remediation_status", ""),
                "pit_caveat_required": bool(action.get("pit_caveat_required")),
                "selected_for_wrapper": source_id == selected_source_id,
                "simulation_ready_candidate": source_id == selected_source_id,
                "wrapper_generation_mode": "deterministic_schema_adapter",
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_target_baseline_wrapper_artifact(
    *,
    selected_source_id: str,
    inventory_rows: Sequence[Mapping[str, Any]],
    pit_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    adapter_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    if not selected_source_id:
        return []
    source = _row_by_source(inventory_rows, selected_source_id)
    action = _row_by_source(action_rows, selected_source_id)
    if not source or not action.get("wrapper_allowed"):
        return []
    raw_rows = _load_source_rows(Path(str(source.get("source_path") or "")))
    if not raw_rows:
        return []
    pit = _row_by_source(pit_rows, selected_source_id)
    adapter_id = _adapter_id(selected_source_id)
    adapter_fields = {
        str(row.get("target_baseline_field")): row
        for row in adapter_rows
        if str(row.get("source_id")) == selected_source_id
    }
    pit_policy = (
        "PIT_APPROXIMATION_READY"
        if action.get("pit_caveat_required")
        else "STRICT_PIT_REMEDIATED_READY"
    )
    wrapper_rows: list[dict[str, Any]] = []
    for raw in raw_rows[:5000]:
        date_value = _value_for_target_field(
            raw, "date", adapter_fields
        ) or _date_from_source(source)
        target_asset_value = str(
            _value_for_target_field(raw, "target_asset", adapter_fields) or ""
        ).upper()
        target_assets = (
            [(target_asset_value, None)]
            if target_asset_value
            else [(asset, exposure) for asset, exposure in _wide_target_exposures(raw)]
        )
        if not date_value or not target_assets:
            continue
        as_of_timestamp = _timestamp_value(raw, "as_of_timestamp", adapter_fields, date_value)
        decision_timestamp = _timestamp_value(raw, "decision_timestamp", adapter_fields, date_value)
        valid_from = _value_for_target_field(raw, "valid_from", adapter_fields) or str(date_value)
        valid_until = _value_for_target_field(raw, "valid_until", adapter_fields) or str(date_value)
        rebalance_flag = _bool_value(
            _value_for_target_field(raw, "rebalance_flag", adapter_fields), default=False
        )
        for target_asset, wide_target_exposure in target_assets:
            target_exposure = wide_target_exposure
            if target_exposure is None:
                target_exposure = _numeric_or_none(
                    _value_for_target_field(raw, "target_exposure", adapter_fields)
                )
            asset_weight = _numeric_or_none(
                _value_for_target_field(raw, "asset_weight", adapter_fields)
            )
            if target_exposure is None:
                target_exposure = asset_weight
            if asset_weight is None:
                asset_weight = target_exposure
            if target_exposure is None:
                continue
            risk_asset_exposure = _numeric_or_none(
                _value_for_target_field(raw, "risk_asset_exposure", adapter_fields)
            )
            if risk_asset_exposure is None:
                risk_asset_exposure = target_exposure
            cash_weight = _numeric_or_none(
                _value_for_target_field(raw, "cash_weight", adapter_fields)
            )
            if cash_weight is None:
                cash_weight = _wide_cash_weight(raw)
            row = {
                "baseline_id": _baseline_id(selected_source_id),
                "source_id": selected_source_id,
                "source_type": source.get("source_type", "unknown_candidate_source"),
                "source_family": source.get("source_type", "unknown_candidate_source"),
                "date": str(date_value)[:10],
                "target_asset": target_asset,
                "target_exposure": round_float(float(target_exposure)),
                "risk_asset_exposure": round_float(float(risk_asset_exposure)),
                "asset_weight": round_float(float(asset_weight)),
                "cash_weight": round_float(float(cash_weight))
                if cash_weight is not None
                else None,
                "as_of_timestamp": as_of_timestamp,
                "decision_timestamp": decision_timestamp,
                "valid_from": str(valid_from)[:10],
                "valid_until": str(valid_until)[:10],
                "rebalance_flag": rebalance_flag,
                "rebalance_timestamp": decision_timestamp,
                "source_artifact_hash": source.get("source_hash", ""),
                "source_hash": source.get("source_hash", ""),
                "source_path": source.get("source_path", ""),
                "baseline_schema_version": BASELINE_SCHEMA_VERSION,
                "adapter_id": adapter_id,
                "signal_source_id": _value_for_target_field(raw, "signal_source_id", adapter_fields)
                or selected_source_id,
                "advisory_id": _value_for_target_field(raw, "advisory_id", adapter_fields)
                or "not_applicable",
                "generated_at": generated_at.isoformat(),
                "pit_policy": pit_policy,
                "replayability_status": "REPLAYABLE"
                if pit.get("replayable")
                else "SOURCE_HASH_BOUND",
                "known_at_semantics": _known_at_semantics(pit, action),
                "wrapper_generation_mode": "deterministic_schema_adapter",
                "simulation_ready_candidate": bool(action.get("wrapper_allowed")),
                "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
                **_safety_subset(),
            }
            wrapper_rows.append(row)
    return wrapper_rows


def build_dynamic_target_wrapper_validation_summary(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
    gap_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    required_fields = list(BASELINE_SCHEMA_FIELDS)
    fail_count = 0
    pass_count = 0
    validation_errors: list[str] = []
    for row in wrapper_rows:
        for field in required_fields:
            if field in row and row.get(field) not in ("", None):
                pass_count += 1
            else:
                fail_count += 1
                validation_errors.append(f"{field} missing for {row.get('source_id')}")
    action = _row_by_source(action_rows, selected_source_id)
    warnings = []
    if action.get("pit_caveat_required"):
        warnings.append("pit_or_known_at_caveat_required")
    if not wrapper_rows:
        validation_status = "FAIL"
    elif fail_count:
        validation_status = "FAIL"
    elif warnings:
        validation_status = "PASS_WITH_WARNINGS"
    else:
        validation_status = "PASS"
    return {
        "schema_version": f"{REPORT_TYPE}.wrapper_validation.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_wrapper_validation_summary",
        "task_id": TASK_ID,
        "baseline_id": _baseline_id(selected_source_id) if selected_source_id else "",
        "source_id": selected_source_id,
        "wrapper_generated": bool(wrapper_rows),
        "wrapper_record_count": len(wrapper_rows),
        "required_field_pass_count": pass_count,
        "required_field_fail_count": fail_count,
        "timestamp_validation_status": _field_validation_status(
            wrapper_rows,
            ("as_of_timestamp", "decision_timestamp"),
        ),
        "exposure_field_validation_status": _field_validation_status(
            wrapper_rows,
            ("target_exposure", "asset_weight"),
        ),
        "source_hash_validation_status": _field_validation_status(
            wrapper_rows,
            ("source_artifact_hash",),
        ),
        "registry_reference_validation_status": "PASS_WITH_WARNINGS"
        if action.get("remediation_action") == "REQUIRE_REGISTRY_BINDING"
        else "PASS",
        "alignment_validation_status": "NOT_EVALUATED_IN_WRAPPER_VALIDATION",
        "simulation_ready_candidate": bool(wrapper_rows) and validation_status != "FAIL",
        "validation_status": validation_status,
        "validation_errors": sorted(set(validation_errors))[:20],
        "validation_warnings": warnings,
        **_safety_subset(),
    }


def build_dynamic_target_wrapper_pit_caveat_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
    pit_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pit = _row_by_source(pit_rows, selected_source_id)
    missing_pit_fields = sorted(
        {
            str(row.get("required_field"))
            for row in gap_rows
            if str(row.get("source_id")) == selected_source_id
            and str(row.get("required_field"))
            in {"as_of_timestamp", "decision_timestamp", "valid_from", "valid_until"}
            and row.get("field_available") is not True
        }
    )
    pit_policy = "PIT_APPROXIMATION_READY" if missing_pit_fields else "STRICT_PIT_REMEDIATED_READY"
    return {
        "schema_version": f"{REPORT_TYPE}.pit_caveat.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_wrapper_pit_caveat_report",
        "task_id": TASK_ID,
        "baseline_id": _baseline_id(selected_source_id) if selected_source_id else "",
        "source_id": selected_source_id,
        "pit_policy": pit_policy if wrapper_rows else "NO_WRAPPER_GENERATED",
        "strict_pit_ready": bool(wrapper_rows) and not missing_pit_fields,
        "pit_approximation_ready": bool(wrapper_rows) and bool(missing_pit_fields),
        "known_at_semantics": str(pit.get("known_at_semantics") or "not_available"),
        "missing_pit_fields": missing_pit_fields,
        "lookahead_risk": "MEDIUM" if missing_pit_fields else "LOW",
        "revision_risk": str(pit.get("revision_risk") or "HIGH"),
        "allowed_usage": [
            "dynamic_target_dry_run_diagnostics",
            "research_only_alignment_test",
            "source_bound_simulation_proxy",
        ]
        if wrapper_rows
        else [],
        "blocked_usage": ["promotion", "paper_shadow", "production", "broker_action"],
        **_safety_subset(),
    }


def build_dynamic_target_wrapper_alignment_readiness(
    *,
    selected_source_id: str,
    wrapper_rows: Sequence[Mapping[str, Any]],
    risk_alignment_rows: Sequence[Mapping[str, Any]],
    market_alignment_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    risk = _row_by_source(risk_alignment_rows, selected_source_id)
    market = _row_by_source(market_alignment_rows, selected_source_id)
    risk_status = str(risk.get("alignment_readiness_status") or "")
    market_status = str(market.get("alignment_readiness_status") or "")
    blockers = list(risk.get("alignment_blockers") or [])
    if not wrapper_rows:
        status = "WRAPPER_ALIGNMENT_BLOCKED"
        blockers.append("wrapper_not_generated")
    elif risk_status.startswith("ALIGNMENT_READY") and market_status.startswith(
        "ALIGNMENT_READY"
    ):
        status = (
            "WRAPPER_ALIGNMENT_READY_WITH_WARNINGS"
            if blockers
            else "WRAPPER_ALIGNMENT_READY"
        )
    else:
        status = "WRAPPER_ALIGNMENT_BLOCKED"
        if risk_status:
            blockers.append(risk_status)
        if market_status:
            blockers.append(market_status)
    return {
        "schema_version": f"{REPORT_TYPE}.alignment_readiness.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_wrapper_alignment_readiness",
        "task_id": TASK_ID,
        "baseline_id": _baseline_id(selected_source_id) if selected_source_id else "",
        "risk_cap_trigger_series_available": bool(risk.get("risk_cap_trigger_series_available")),
        "market_data_available": bool(market.get("market_data_available")),
        "date_overlap_start": risk.get("overlap_start", ""),
        "date_overlap_end": risk.get("overlap_end", ""),
        "overlap_record_count": int(risk.get("overlap_record_count") or 0),
        "target_asset_overlap": risk.get("asset_overlap", []),
        "horizon_overlap": risk.get("horizon_overlap", []),
        "calendar_alignment_status": risk.get("calendar_alignment_status", ""),
        "timestamp_alignment_status": risk.get("timestamp_alignment_status", ""),
        "policy_compatibility_status": "COMPATIBLE_RESEARCH_ONLY",
        "alignment_readiness_status": status,
        "alignment_blockers": sorted(set(str(item) for item in blockers if str(item))),
        "alignment_warnings": []
        if status == "WRAPPER_ALIGNMENT_READY"
        else ["alignment requires review before 2330"],
        **_safety_subset(),
    }


def build_dynamic_target_remediation_blocked_sources(
    action_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    blocked = [
        dict(row)
        for row in action_rows
        if not row.get("wrapper_allowed")
        and str(row.get("remediation_status", "")).startswith("REMEDIATION_BLOCKED")
    ]
    return {
        "schema_version": f"{REPORT_TYPE}.blocked_sources.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_remediation_blocked_sources",
        "task_id": TASK_ID,
        "blocked_source_count": len(blocked),
        "rows": blocked,
        **_safety_subset(),
    }


def build_dynamic_target_no_remediable_source_report(
    *,
    action_rows: Sequence[Mapping[str, Any]],
    inventory_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    remediable_count = sum(1 for row in action_rows if row.get("wrapper_allowed"))
    return {
        "schema_version": f"{REPORT_TYPE}.no_remediable_source.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_no_remediable_source_report",
        "task_id": TASK_ID,
        "remediation_status": "REMEDIATION_BLOCKED_NO_REMEDIABLE_SOURCE"
        if remediable_count == 0
        else "REMEDIABLE_SOURCE_AVAILABLE",
        "candidate_source_count": len(inventory_rows),
        "remediable_source_count": remediable_count,
        "next_task": NEXT_SOURCE_GENERATION_TASK
        if remediable_count == 0
        else NEXT_DYNAMIC_DRY_RUN_TASK,
        "simulation_executed": False,
        **_safety_subset(),
    }


def build_dynamic_target_source_generation_requirements(
    *,
    gap_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    blocking_fields = sorted(
        {
            str(row.get("required_field"))
            for row in gap_rows
            if row.get("blocking_for_wrapper") is True
        }
    )
    no_semantics_count = sum(
        1
        for row in action_rows
        if row.get("remediation_status")
        == "REMEDIATION_BLOCKED_NO_TARGET_EXPOSURE_SEMANTICS"
    )
    return {
        "schema_version": f"{REPORT_TYPE}.source_generation_requirements.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_source_generation_requirements",
        "task_id": TASK_ID,
        "required_output_schema": BASELINE_SCHEMA_VERSION,
        "required_fields": list(BASELINE_SCHEMA_FIELDS),
        "blocking_fields_observed": blocking_fields,
        "no_target_exposure_semantics_source_count": no_semantics_count,
        "source_generation_required": no_semantics_count > 0 or bool(blocking_fields),
        "recommended_next_task": NEXT_SOURCE_GENERATION_TASK,
        "generation_boundary": "research_only_source_artifact_no_trading_instruction",
        **_safety_subset(),
    }


def build_dynamic_target_2330_readiness_matrix(
    *,
    wrapper_validation: Mapping[str, Any],
    pit_caveat_report: Mapping[str, Any],
    alignment_readiness: Mapping[str, Any],
    action_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
) -> dict[str, Any]:
    wrapper_generated = bool(wrapper_validation.get("wrapper_generated"))
    validation_status = str(wrapper_validation.get("validation_status") or "FAIL")
    alignment_status = str(alignment_readiness.get("alignment_readiness_status") or "")
    remediable_count = sum(1 for row in action_rows if row.get("wrapper_allowed"))
    blockers: list[str] = []
    warnings: list[str] = []
    if not wrapper_generated:
        blockers.append("wrapper_not_generated")
    if validation_status == "FAIL":
        blockers.append("wrapper_validation_failed")
    if alignment_status == "WRAPPER_ALIGNMENT_BLOCKED":
        blockers.extend(_strings(alignment_readiness.get("alignment_blockers")))
    if pit_caveat_report.get("pit_approximation_ready"):
        warnings.append("PIT_OR_ALIGNMENT_WARNINGS")
    if alignment_status == "WRAPPER_ALIGNMENT_READY_WITH_WARNINGS":
        warnings.append("alignment_ready_with_warnings")

    if (
        wrapper_generated
        and validation_status == "PASS"
        and alignment_status == "WRAPPER_ALIGNMENT_READY"
    ):
        readiness_status = "DYNAMIC_WRAPPER_READY_FOR_2330"
        allowed = True
    elif wrapper_generated and validation_status in {"PASS", "PASS_WITH_WARNINGS"} and not blockers:
        readiness_status = "DYNAMIC_WRAPPER_READY_WITH_WARNINGS_FOR_2330"
        allowed = True
    elif remediable_count == 0:
        readiness_status = "DYNAMIC_WRAPPER_SOURCE_GENERATION_REQUIRED"
        allowed = False
    elif any(row.get("adapter_required") for row in action_rows if row.get("wrapper_allowed")):
        readiness_status = "DYNAMIC_WRAPPER_SCHEMA_ADAPTER_REQUIRED"
        allowed = False
    else:
        readiness_status = "DYNAMIC_WRAPPER_BLOCKED"
        allowed = False
    return {
        "schema_version": f"{REPORT_TYPE}.2330_readiness.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_2330_readiness_matrix",
        "task_id": TASK_ID,
        "readiness_status": readiness_status,
        "baseline_id": _baseline_id(selected_source_id) if selected_source_id else "",
        "wrapper_generated": wrapper_generated,
        "wrapper_validation_status": validation_status,
        "pit_policy": pit_caveat_report.get("pit_policy", ""),
        "alignment_readiness_status": alignment_status,
        "field_coverage_ready": validation_status in {"PASS", "PASS_WITH_WARNINGS"},
        "risk_cap_alignment_ready": alignment_status in {
            "WRAPPER_ALIGNMENT_READY",
            "WRAPPER_ALIGNMENT_READY_WITH_WARNINGS",
        },
        "market_data_alignment_ready": alignment_status in {
            "WRAPPER_ALIGNMENT_READY",
            "WRAPPER_ALIGNMENT_READY_WITH_WARNINGS",
        },
        "policy_compatible": True,
        "2330_allowed": allowed,
        "readiness_blockers": sorted(set(blockers)),
        "readiness_warnings": sorted(set(warnings)),
        **_safety_subset(),
    }


def build_dynamic_target_2330_task_route(
    *,
    readiness: Mapping[str, Any],
    action_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status") or "")
    if status in {
        "DYNAMIC_WRAPPER_READY_FOR_2330",
        "DYNAMIC_WRAPPER_READY_WITH_WARNINGS_FOR_2330",
    }:
        next_task = NEXT_DYNAMIC_DRY_RUN_TASK
        caveat = "PIT_OR_ALIGNMENT_WARNINGS" if readiness.get("readiness_warnings") else ""
    elif status == "DYNAMIC_WRAPPER_SCHEMA_ADAPTER_REQUIRED":
        selected_action = _row_by_source(action_rows, selected_source_id)
        if selected_action.get("remediation_action") == "REQUIRE_REGISTRY_BINDING":
            next_task = NEXT_REGISTRY_BINDING_TASK
        else:
            next_task = NEXT_TIMESTAMP_REMEDIATION_TASK
        caveat = "SCHEMA_ADAPTER_OR_REGISTRY_REMEDIATION_REQUIRED"
    elif _selected_missing_timestamp(gap_rows, selected_source_id):
        next_task = NEXT_TIMESTAMP_REMEDIATION_TASK
        caveat = "TIMESTAMP_FIELDS_MISSING"
    elif status == "DYNAMIC_WRAPPER_SOURCE_GENERATION_REQUIRED":
        next_task = NEXT_SOURCE_GENERATION_TASK
        caveat = "NO_REMEDIABLE_DYNAMIC_TARGET_SOURCE"
    else:
        next_task = NEXT_STATIC_ONLY_TASK
        caveat = "DYNAMIC_WRAPPER_BLOCKED"
    return {
        "schema_version": f"{REPORT_TYPE}.2330_task_route.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_2330_task_route",
        "task_id": TASK_ID,
        "readiness_status": status,
        "next_task": next_task,
        "caveat": caveat,
        "simulation_executed": False,
        **_safety_subset(),
    }


def build_dynamic_target_source_remediation_safety_boundary(
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": "dynamic_target_source_remediation_safety_boundary",
        "task_id": TASK_ID,
        "generated_at": generated_at.isoformat(),
        "research_only": True,
        "source_remediation_only": True,
        "simulation_executed": False,
        "portfolio_effect": "none",
        "production_effect": "none",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "manual_review_only": True,
        "target_weight_action_generated": False,
        "rebalance_instruction_generated": False,
        "buy_signal_generated": False,
        "sell_signal_generated": False,
        "broker_order_generated": False,
    }


def build_dynamic_target_source_remediation_summary(
    *,
    generated_at: datetime,
    dynamic_preparation_dir: Path,
    diagnostics_dir: Path,
    static_dry_run_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    candidate_roots: Sequence[Path],
    inputs: Mapping[str, Any],
    family_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    adapter_rows: Sequence[Mapping[str, Any]],
    wrapper_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    prep_summary = mapping(mapping(inputs.get("dynamic_preparation")).get("summary"))
    remediable_count = sum(1 for row in action_rows if row.get("wrapper_allowed"))
    blocked_count = sum(1 for row in action_rows if not row.get("wrapper_allowed"))
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "task_id": TASK_ID,
        "title": "Dynamic Target Baseline Source Remediation",
        "status": STATUS,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "dynamic_preparation_dir": str(dynamic_preparation_dir),
        "diagnostics_dir": str(diagnostics_dir),
        "static_dry_run_dir": str(static_dry_run_dir),
        "source_binding_dir": str(source_binding_dir),
        "simulation_policy_dir": str(simulation_policy_dir),
        "candidate_artifact_roots": [str(path) for path in candidate_roots],
        "upstream_candidate_artifacts_found": int(
            prep_summary.get("candidate_count")
            or prep_summary.get("inventory_source_count")
            or 0
        ),
        "upstream_pit_ready_source_count": int(prep_summary.get("pit_ready_source_count") or 0),
        "upstream_recommended_candidate_count": int(
            prep_summary.get("recommended_candidate_count") or 0
        ),
        "source_family_count": len(family_rows),
        "gap_to_schema_row_count": len(gap_rows),
        "remediation_action_count": len(action_rows),
        "remediable_source_count": remediable_count,
        "blocked_source_count": blocked_count,
        "schema_adapter_spec_row_count": len(adapter_rows),
        "wrapper_generated": bool(wrapper_rows),
        "wrapper_record_count": len(wrapper_rows),
        "wrapper_validation_status": wrapper_validation.get("validation_status", "FAIL"),
        "readiness_status": readiness.get("readiness_status", ""),
        "2330_allowed": bool(readiness.get("2330_allowed")),
        "next_task": task_route.get("next_task", ""),
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_gate_required": False,
        "data_quality_gate_executed": False,
        "aits_validate_data_executed": False,
        "data_quality_gate_rationale": (
            "aits validate-data not applicable because TRADING-2329 only reads prior "
            "research outputs / static config / registry / candidate artifacts"
        ),
        "dynamic_target_source_remediation_cli": True,
        "dynamic_target_source_family_ranking_generated": True,
        "dynamic_target_gap_to_schema_matrix_generated": True,
        "dynamic_target_remediation_action_matrix_generated": True,
        "dynamic_target_baseline_schema_contract_generated": True,
        "dynamic_target_schema_adapter_spec_generated": True,
        "dynamic_target_2330_readiness_matrix_generated": True,
        "dynamic_target_2330_task_route_generated": True,
        "simulation_executed": False,
        "research_only": True,
        "source_remediation_only": True,
        "manual_review_only": True,
        "portfolio_effect": "none",
        "production_effect": "none",
        **_safety_subset(),
    }


def write_dynamic_target_source_remediation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    family_rows: Sequence[Mapping[str, Any]],
    gap_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    schema_contract: Mapping[str, Any],
    adapter_rows: Sequence[Mapping[str, Any]],
    remediated_rows: Sequence[Mapping[str, Any]],
    wrapper_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    pit_caveat_report: Mapping[str, Any],
    alignment_readiness: Mapping[str, Any],
    blocked_sources: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    no_source_report: Mapping[str, Any],
    generation_requirements: Mapping[str, Any],
) -> dict[str, str]:
    output_payloads = [
        summary,
        *family_rows,
        *gap_rows,
        *action_rows,
        schema_contract,
        *adapter_rows,
        *remediated_rows,
        *wrapper_rows,
        wrapper_validation,
        pit_caveat_report,
        alignment_readiness,
        blocked_sources,
        readiness,
        task_route,
        safety_boundary,
        no_source_report,
        generation_requirements,
    ]
    for index, payload in enumerate(output_payloads):
        validate_no_unsafe_fields(f"TRADING-2329 output {index}", payload)

    paths: dict[str, Path] = {
        "summary": output_dir / "dynamic_target_source_remediation_summary.json",
        "family_json": output_dir / "dynamic_target_source_family_ranking.json",
        "family_csv": output_dir / "dynamic_target_source_family_ranking.csv",
        "gap_json": output_dir / "dynamic_target_gap_to_schema_matrix.json",
        "gap_csv": output_dir / "dynamic_target_gap_to_schema_matrix.csv",
        "action_json": output_dir / "dynamic_target_remediation_action_matrix.json",
        "action_csv": output_dir / "dynamic_target_remediation_action_matrix.csv",
        "schema_contract": output_dir / "dynamic_target_baseline_schema_contract.json",
        "adapter_json": output_dir / "dynamic_target_schema_adapter_spec.json",
        "adapter_csv": output_dir / "dynamic_target_schema_adapter_spec.csv",
        "remediated_json": output_dir / "dynamic_target_remediated_baseline_candidates.json",
        "remediated_csv": output_dir / "dynamic_target_remediated_baseline_candidates.csv",
        "wrapper_validation": output_dir / "dynamic_target_wrapper_validation_summary.json",
        "pit_caveat": output_dir / "dynamic_target_wrapper_pit_caveat_report.json",
        "alignment": output_dir / "dynamic_target_wrapper_alignment_readiness.json",
        "blocked_sources": output_dir / "dynamic_target_remediation_blocked_sources.json",
        "readiness": output_dir / "dynamic_target_2330_readiness_matrix.json",
        "task_route": output_dir / "dynamic_target_2330_task_route.json",
        "safety_boundary": output_dir / "dynamic_target_source_remediation_safety_boundary.json",
        "no_source": output_dir / "dynamic_target_no_remediable_source_report.json",
        "generation_requirements": output_dir
        / "dynamic_target_source_generation_requirements.json",
        "report_doc": docs_root / "dynamic_target_baseline_source_remediation_report.md",
        "schema_doc": docs_root / "dynamic_target_baseline_schema_contract.md",
        "adapter_doc": docs_root / "dynamic_target_schema_adapter_spec.md",
        "pit_doc": docs_root / "dynamic_target_wrapper_pit_caveat_report.md",
        "route_doc": docs_root / "dynamic_target_2330_readiness_route.md",
    }
    if wrapper_rows:
        paths["wrapper_json"] = output_dir / "dynamic_target_baseline_wrapper_artifact.json"
        paths["wrapper_csv"] = output_dir / "dynamic_target_baseline_wrapper_artifact.csv"

    write_json(paths["summary"], dict(summary))
    write_json(paths["family_json"], {**dict(summary), "rows": list(family_rows)})
    write_csv_rows(paths["family_csv"], family_rows)
    write_json(paths["gap_json"], {**dict(summary), "rows": list(gap_rows)})
    write_csv_rows(paths["gap_csv"], gap_rows)
    write_json(paths["action_json"], {**dict(summary), "rows": list(action_rows)})
    write_csv_rows(paths["action_csv"], action_rows)
    write_json(paths["schema_contract"], dict(schema_contract))
    write_json(paths["adapter_json"], {**dict(summary), "rows": list(adapter_rows)})
    write_csv_rows(paths["adapter_csv"], adapter_rows)
    write_json(paths["remediated_json"], {**dict(summary), "rows": list(remediated_rows)})
    write_csv_rows(paths["remediated_csv"], remediated_rows)
    if wrapper_rows:
        write_json(paths["wrapper_json"], {**dict(summary), "rows": list(wrapper_rows)})
        write_csv_rows(paths["wrapper_csv"], wrapper_rows)
    write_json(paths["wrapper_validation"], dict(wrapper_validation))
    write_json(paths["pit_caveat"], dict(pit_caveat_report))
    write_json(paths["alignment"], dict(alignment_readiness))
    write_json(paths["blocked_sources"], dict(blocked_sources))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    if not remediated_rows:
        write_json(paths["no_source"], dict(no_source_report))
        write_json(paths["generation_requirements"], dict(generation_requirements))
    else:
        write_json(paths["no_source"], dict(no_source_report))
        write_json(paths["generation_requirements"], dict(generation_requirements))

    write_markdown(
        paths["report_doc"],
        _render_source_remediation_report(
            summary,
            family_rows,
            action_rows,
            wrapper_validation,
            readiness,
            task_route,
        ),
    )
    write_markdown(paths["schema_doc"], _render_schema_contract_doc(schema_contract))
    write_markdown(paths["adapter_doc"], _render_adapter_spec_doc(summary, adapter_rows))
    write_markdown(paths["pit_doc"], _render_pit_caveat_doc(pit_caveat_report))
    write_markdown(paths["route_doc"], _render_2330_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicTargetBaselineSourceRemediationError(
            f"{label} missing required artifacts: {missing}"
        )
    for key, path in paths.items():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise DynamicTargetBaselineSourceRemediationError(
                f"{label} artifact is not valid JSON: {path}"
            ) from exc
        if not isinstance(payload, Mapping):
            raise DynamicTargetBaselineSourceRemediationError(
                f"{label} artifact must be a JSON object: {path}"
            )
        payloads[key] = dict(payload)
    return payloads


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in records(mapping(payload).get("rows"))]


def _by_source(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(row.get("source_id")): row for row in rows if row.get("source_id")}


def _row_by_source(
    rows: Sequence[Mapping[str, Any]],
    source_id: str,
) -> Mapping[str, Any]:
    return _by_source(rows).get(source_id, {})


def _gap_categories_by_source(
    rows: Sequence[Mapping[str, Any]],
    *,
    blocking_key: str | None = None,
    severity: str | None = None,
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for row in rows:
        if blocking_key and row.get(blocking_key) is not True:
            continue
        if severity and str(row.get("gap_severity")) != severity:
            continue
        source_id = str(row.get("source_id"))
        category = str(row.get("gap_category") or row.get("required_field") or "")
        if source_id and category:
            result.setdefault(source_id, set()).add(category)
    return result


def _schema_gaps_by_source(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, set[str]]]:
    result: dict[str, dict[str, set[str]]] = {}
    for row in rows:
        source_id = str(row.get("source_id"))
        field = str(row.get("required_field"))
        bucket = "blocking" if row.get("blocking_for_wrapper") is True else "warning"
        if row.get("field_available") is True:
            continue
        result.setdefault(source_id, {"blocking": set(), "warning": set()})[bucket].add(field)
    return result


def _field_coverage_score(source: Mapping[str, Any]) -> float:
    coverage = mapping(source.get("field_coverage"))
    if not source.get("source_available") or not coverage:
        return 0.0
    baseline_fields = (
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
        "source_artifact_hash",
        "signal_source_id",
        "advisory_id",
    )
    available = sum(1 for field in baseline_fields if _schema_field_available(field, source, {}))
    return round_float(available / len(baseline_fields))


def _timestamp_score(pit: Mapping[str, Any]) -> float:
    if pit.get("as_of_timestamp_available") and pit.get("decision_timestamp_available"):
        return 1.0
    if pit.get("as_of_timestamp_available") or pit.get("decision_timestamp_available"):
        return 0.5
    return 0.0


def _pit_score(pit: Mapping[str, Any]) -> float:
    status = str(pit.get("pit_status") or "")
    if status == "STRICT_PIT_READY":
        return 1.0
    if status in {"PIT_APPROXIMATION_READY", "REPLAYABLE_BUT_NOT_STRICT_PIT"}:
        return 0.65
    if pit.get("replayable"):
        return 0.35
    return 0.0


def _alignment_score(risk: Mapping[str, Any], market: Mapping[str, Any]) -> float:
    statuses = {
        str(risk.get("alignment_readiness_status") or ""),
        str(market.get("alignment_readiness_status") or ""),
    }
    if statuses <= {"ALIGNMENT_READY"}:
        return 1.0
    if any("READY_WITH_WARNINGS" in status for status in statuses):
        return 0.75
    if any(status.startswith("ALIGNMENT_READY") for status in statuses):
        return 0.5
    return 0.0


def _target_semantics_score(source: Mapping[str, Any]) -> float:
    source_type = str(source.get("source_type") or "")
    if source_type == "dynamic_strategy_target_exposure":
        return 1.0
    if source_type in {"paper_portfolio_advisory_target", "daily_advisory_target_exposure"}:
        return 0.7
    if source_type in {"etf_allocation_dynamic_output", "risk_budget_target_exposure"}:
        return 0.55
    return 0.0


def _schema_adapter_effort(source: Mapping[str, Any], blockers: set[str]) -> str:
    if not source.get("source_available") or "schema_incompatible" in blockers:
        return "HIGH"
    coverage = mapping(source.get("field_coverage"))
    missing_timestamp = not (
        coverage.get("as_of_timestamp") and coverage.get("decision_timestamp")
    )
    missing_validity = not (coverage.get("valid_from") and coverage.get("valid_until"))
    if missing_timestamp and missing_validity:
        return "MEDIUM" if _has_target_exposure_semantics(source) else "HIGH"
    if missing_timestamp or missing_validity or not coverage.get("target_exposure"):
        return "MEDIUM"
    return "LOW"


def _ranking_label(
    source: Mapping[str, Any],
    pit: Mapping[str, Any],
    blockers: set[str],
    adapter_effort: str,
) -> str:
    if (
        not source.get("source_available")
        or source.get("source_type") == "unknown_candidate_source"
    ):
        return "NOT_REMEDIABLE"
    if not _has_target_exposure_semantics(source):
        return "NOT_REMEDIABLE"
    if "source_not_replayable" in blockers:
        return "NOT_REMEDIABLE"
    if adapter_effort == "HIGH":
        return "REMEDIATION_COST_HIGH"
    if pit.get("pit_status") in {"CURRENT_ARTIFACT_ONLY", "REPLAYABLE_BUT_NOT_STRICT_PIT"}:
        return "REMEDIABLE_WITH_PIT_CAVEAT"
    if adapter_effort == "MEDIUM":
        return "REMEDIABLE_WITH_SCHEMA_ADAPTER"
    return "TOP_REMEDIATION_CANDIDATE"


def _remediation_feasibility(label: str) -> str:
    if label == "NOT_REMEDIABLE":
        return "NOT_REMEDIABLE"
    if label == "REMEDIATION_COST_HIGH":
        return "REMEDIATION_COST_HIGH"
    return "REMEDIABLE"


def _recommended_action_for_label(label: str) -> str:
    return {
        "TOP_REMEDIATION_CANDIDATE": "GENERATE_BASELINE_WRAPPER",
        "REMEDIABLE_WITH_SCHEMA_ADAPTER": "GENERATE_SCHEMA_ADAPTER_THEN_WRAPPER",
        "REMEDIABLE_WITH_PIT_CAVEAT": "ADD_PIT_CAVEAT_AND_WRAPPER",
        "REMEDIATION_COST_HIGH": "REQUIRE_SOURCE_ARTIFACT_REGENERATION",
        "NOT_REMEDIABLE": "BLOCK_SOURCE",
    }[label]


def _research_value_for_family(source_family: str) -> str:
    if source_family == "dynamic_strategy_target_exposure":
        return "HIGH"
    if source_family in {
        "paper_portfolio_advisory_target",
        "daily_advisory_target_exposure",
    }:
        return "MEDIUM"
    if source_family in {"etf_allocation_dynamic_output", "risk_budget_target_exposure"}:
        return "MEDIUM_LOW"
    return "LOW"


def _schema_field_available(
    field_name: str,
    source: Mapping[str, Any],
    pit: Mapping[str, Any],
) -> bool:
    coverage = mapping(source.get("field_coverage"))
    if field_name in {
        "baseline_id",
        "baseline_schema_version",
        "pit_policy",
        "known_at_semantics",
    }:
        return True
    if field_name in {"source_id", "source_type", "source_path"}:
        return bool(source.get(field_name))
    if field_name in {"source_hash", "source_artifact_hash"}:
        return bool(source.get("source_hash")) or bool(coverage.get("source_artifact_hash"))
    if field_name == "rebalance_timestamp":
        return bool(coverage.get("rebalance_timestamp") or coverage.get("decision_timestamp"))
    if field_name == "replayability_status":
        return bool(pit.get("replayable") or source.get("source_hash"))
    if field_name in {
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "broker_action",
    }:
        return True
    if field_name in coverage:
        return bool(coverage.get(field_name))
    return False


def _field_mapping_candidate(field_name: str, coverage: Mapping[str, Any]) -> str:
    if field_name in {"source_hash", "source_artifact_hash"}:
        return "source_hash"
    if field_name == "rebalance_timestamp":
        return "decision_timestamp" if coverage.get("decision_timestamp") else ""
    aliases = FIELD_ALIASES.get(field_name, (field_name,))
    for alias in aliases:
        normalized = str(alias)
        if field_name in coverage and coverage.get(field_name) and alias == field_name:
            return normalized
        if coverage.get(field_name):
            return normalized
    if field_name == "target_exposure" and coverage.get("asset_weight"):
        return "asset_weight"
    if field_name == "risk_asset_exposure" and coverage.get("target_exposure"):
        return "target_exposure"
    return ""


def _fallback_allowed(field_name: str, source: Mapping[str, Any], pit: Mapping[str, Any]) -> bool:
    if field_name in FIELD_BLOCKING_DEFAULTS:
        return False
    if field_name in {"as_of_timestamp", "decision_timestamp"}:
        return bool(_schema_field_available("date", source, pit))
    if field_name in {"valid_from", "valid_until", "rebalance_timestamp"}:
        return bool(_schema_field_available("date", source, pit))
    if field_name in {"risk_asset_exposure", "asset_weight"}:
        return _has_target_exposure_semantics(source)
    if field_name in {"cash_weight", "rebalance_flag", "signal_source_id", "advisory_id"}:
        return True
    return False


def _fallback_policy(field_name: str) -> str:
    return {
        "as_of_timestamp": "derive_date_level_timestamp_with_pit_caveat",
        "decision_timestamp": "derive_date_level_decision_timestamp_with_pit_caveat",
        "valid_from": "derive_from_date_with_validity_caveat",
        "valid_until": "derive_from_date_with_validity_caveat",
        "rebalance_timestamp": "derive_from_decision_timestamp",
        "risk_asset_exposure": "copy_target_exposure_pending_asset_mapping",
        "asset_weight": "copy_target_exposure_if_asset_level",
        "cash_weight": "leave_null_if_source_does_not_emit_cash_allocation",
        "rebalance_flag": "default_false_no_rebalance_instruction",
        "signal_source_id": "derive_from_source_id",
        "advisory_id": "empty_if_not_advisory_source",
    }.get(field_name, "no fallback allowed")


def _blocking_remediation_action(field_name: str) -> str:
    if field_name in {"target_exposure", "target_asset", "date"}:
        return "REQUIRE_SOURCE_ARTIFACT_REGENERATION"
    if field_name in {"source_hash", "source_artifact_hash"}:
        return "REQUIRE_REGISTRY_BINDING"
    return "BLOCK_SOURCE"


def _blocking_for_2330(
    *,
    field_name: str,
    available: bool,
    fallback_allowed: bool,
    source: Mapping[str, Any],
    blockers: set[str],
    warnings: set[str],
) -> bool:
    if available:
        return False
    if field_name in {"as_of_timestamp", "decision_timestamp"}:
        return True
    if field_name in {"valid_from", "valid_until"}:
        return True
    if "missing_asset_coverage" in blockers or "source_not_pit" in blockers:
        return True
    return not fallback_allowed


def _remediation_status_and_action(
    *,
    source: Mapping[str, Any],
    pit: Mapping[str, Any],
    blocking_gaps: Sequence[str],
    warning_gaps: Sequence[str],
) -> tuple[str, str]:
    source_type = str(source.get("source_type") or "unknown_candidate_source")
    if not source.get("source_available"):
        return (
            "REMEDIATION_BLOCKED_MISSING_REQUIRED_FIELDS",
            "REQUIRE_SOURCE_ARTIFACT_REGENERATION",
        )
    if source_type in {"unknown_candidate_source", "manual_review_only_target_exposure"}:
        return (
            "REMEDIATION_BLOCKED_NO_TARGET_EXPOSURE_SEMANTICS",
            "BLOCK_SOURCE",
        )
    if not _has_target_exposure_semantics(source):
        return (
            "REMEDIATION_BLOCKED_NO_TARGET_EXPOSURE_SEMANTICS",
            "REQUIRE_SOURCE_ARTIFACT_REGENERATION",
        )
    if "source_hash" in blocking_gaps or "source_artifact_hash" in blocking_gaps:
        return ("REMEDIATION_BLOCKED_MISSING_REQUIRED_FIELDS", "REQUIRE_REGISTRY_BINDING")
    if "date" in blocking_gaps or "target_asset" in blocking_gaps:
        return (
            "REMEDIATION_BLOCKED_MISSING_REQUIRED_FIELDS",
            "REQUIRE_SOURCE_ARTIFACT_REGENERATION",
        )
    if source.get("registry_reference_available") is not True and not _pit_caveat_required(
        pit, warning_gaps
    ):
        return (
            "REMEDIATION_READY_WITH_SCHEMA_ADAPTER",
            "REQUIRE_REGISTRY_BINDING",
        )
    if not pit.get("replayable") and not source.get("source_hash"):
        return (
            "REMEDIATION_BLOCKED_SOURCE_NOT_REPLAYABLE",
            "REQUIRE_SOURCE_ARTIFACT_REGENERATION",
        )
    timestamp_gaps = {"as_of_timestamp", "decision_timestamp", "valid_from", "valid_until"}
    if set(warning_gaps) & timestamp_gaps or str(pit.get("pit_status")) != "STRICT_PIT_READY":
        return (
            "REMEDIATION_READY_WITH_PIT_CAVEAT",
            "ADD_PIT_CAVEAT_AND_WRAPPER",
        )
    if _adapter_required(blocking_gaps, warning_gaps, source):
        return (
            "REMEDIATION_READY_WITH_SCHEMA_ADAPTER",
            "GENERATE_SCHEMA_ADAPTER_THEN_WRAPPER",
        )
    return ("REMEDIATION_READY", "GENERATE_BASELINE_WRAPPER")


def _adapter_required(
    blocking_gaps: Sequence[str],
    warning_gaps: Sequence[str],
    source: Mapping[str, Any],
) -> bool:
    if blocking_gaps:
        return True
    coverage = mapping(source.get("field_coverage"))
    adapter_fields = {"risk_asset_exposure", "cash_weight", "rebalance_flag", "signal_source_id"}
    return any(field in warning_gaps for field in adapter_fields) or not coverage.get(
        "target_exposure"
    )


def _pit_caveat_required(pit: Mapping[str, Any], warning_gaps: Sequence[str]) -> bool:
    return str(pit.get("pit_status")) != "STRICT_PIT_READY" or bool(
        set(warning_gaps)
        & {"as_of_timestamp", "decision_timestamp", "valid_from", "valid_until"}
    )


def _action_warnings(
    warning_gaps: Sequence[str],
    risk: Mapping[str, Any],
    market: Mapping[str, Any],
) -> list[str]:
    warnings = list(warning_gaps)
    for payload in (risk, market):
        status = str(payload.get("alignment_readiness_status") or "")
        if status and status != "ALIGNMENT_READY":
            warnings.append(status)
    return sorted(set(warnings))


def _next_action_for_status(status: str, action: str) -> str:
    if status.startswith("REMEDIATION_READY"):
        return action
    if status == "REMEDIATION_BLOCKED_NO_TARGET_EXPOSURE_SEMANTICS":
        return "GENERATE_NATIVE_DYNAMIC_TARGET_BASELINE_SOURCE"
    return action


def _has_target_exposure_semantics(source: Mapping[str, Any]) -> bool:
    coverage = mapping(source.get("field_coverage"))
    source_type = str(source.get("source_type") or "")
    return bool(
        coverage.get("target_exposure")
        or coverage.get("asset_weight")
        or source_type
        in {
            "dynamic_strategy_target_exposure",
            "paper_portfolio_advisory_target",
            "daily_advisory_target_exposure",
            "etf_allocation_dynamic_output",
            "risk_budget_target_exposure",
        }
    )


def _select_wrapper_source(
    action_rows: Sequence[Mapping[str, Any]],
    family_rows: Sequence[Mapping[str, Any]],
) -> str:
    allowed = {str(row.get("source_id")) for row in action_rows if row.get("wrapper_allowed")}
    for family in family_rows:
        source_id = str(family.get("best_source_id") or "")
        if source_id in allowed and str(family.get("ranking_label")) != "REMEDIATION_COST_HIGH":
            return source_id
    return sorted(allowed)[0] if allowed else ""


def _adapter_id(source_id: str) -> str:
    return f"dynamic_target_schema_adapter_{source_id[-48:]}"


def _baseline_id(source_id: str) -> str:
    return f"dynamic_target_baseline_{source_id[-48:]}" if source_id else ""


def _load_source_rows(source_path: Path) -> list[dict[str, Any]]:
    resolved = _resolve_project_path(source_path)
    if not resolved.exists() or resolved.is_dir():
        return []
    try:
        if resolved.suffix.lower() == ".json":
            raw = json.loads(resolved.read_text(encoding="utf-8"))
            _validate_raw_candidate_safety(resolved, raw)
            return _extract_source_rows(raw)
        if resolved.suffix.lower() in {".yaml", ".yml"}:
            raw = safe_load_yaml_path(resolved) or {}
            _validate_raw_candidate_safety(resolved, raw)
            return _extract_source_rows(raw)
        if resolved.suffix.lower() == ".csv":
            with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            _validate_raw_candidate_safety(resolved, {"rows": rows[:10]})
            return rows
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return []
    return []


def _extract_source_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [dict(row) for row in raw if isinstance(row, Mapping)]
    payload = mapping(raw)
    for key in ("rows", "records", "signals", "decisions", "actions", "candidate_targets"):
        rows_payload = records(payload.get(key))
        if rows_payload:
            return [dict(row) for row in rows_payload]
    return [dict(payload)] if payload else []


def _validate_raw_candidate_safety(path: Path, payload: Any) -> None:
    validate_no_unsafe_fields(
        f"TRADING-2329 candidate source {path}",
        _strip_research_weight_keys(payload),
    )
    for item in _walk_mappings(payload):
        if item.get("target_weight_action") or item.get("rebalance_instruction_action"):
            raise DynamicTargetBaselineSourceRemediationError(
                f"candidate source {path} emits actionable target/rebalance instruction"
            )


def _strip_research_weight_keys(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {
            key: _strip_research_weight_keys(value)
            for key, value in payload.items()
            if key != "target_weight"
        }
    if isinstance(payload, list):
        return [_strip_research_weight_keys(value) for value in payload[:20]]
    return payload


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


def _resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _value_for_target_field(
    row: Mapping[str, Any],
    target_field: str,
    adapter_fields: Mapping[str, Mapping[str, Any]],
) -> Any:
    adapter = adapter_fields.get(target_field, {})
    source_field = str(adapter.get("source_field") or "")
    if source_field and source_field in row:
        return row.get(source_field)
    for alias in FIELD_ALIASES.get(target_field, (target_field,)):
        if alias in row:
            return row.get(alias)
    if target_field == "target_exposure":
        for alias in ("asset_weight", "recommended_weight", "weight", "target_weight"):
            if alias in row:
                return row.get(alias)
    if target_field == "asset_weight":
        return _value_for_target_field(row, "target_exposure", adapter_fields)
    if target_field == "risk_asset_exposure":
        return _value_for_target_field(row, "target_exposure", adapter_fields)
    return None


def _wide_target_exposures(row: Mapping[str, Any]) -> list[tuple[str, float | None]]:
    exposures: list[tuple[str, float | None]] = []
    for key, value in row.items():
        text = str(key).lower()
        for prefix in ("target_weight_", "target_exposure_", "asset_weight_"):
            if text.startswith(prefix):
                asset = text.removeprefix(prefix).upper()
                if asset:
                    exposures.append((asset, _numeric_or_none(value)))
    return exposures


def _wide_cash_weight(row: Mapping[str, Any]) -> float | None:
    for key in ("target_weight_cash", "target_exposure_cash", "cash_weight"):
        if key in row:
            return _numeric_or_none(row.get(key))
    for key in ("target_weight_sgov", "target_exposure_sgov", "asset_weight_sgov"):
        if key in row:
            return _numeric_or_none(row.get(key))
    return 0.0


def _timestamp_value(
    row: Mapping[str, Any],
    target_field: str,
    adapter_fields: Mapping[str, Mapping[str, Any]],
    date_value: Any,
) -> str:
    value = _value_for_target_field(row, target_field, adapter_fields)
    if value:
        return str(value)
    return f"{str(date_value)[:10]}T00:00:00Z"


def _date_from_source(source: Mapping[str, Any]) -> str:
    return str(source.get("history_start") or source.get("history_end") or "")[:10]


def _numeric_or_none(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_value(value: Any, *, default: bool) -> bool:
    if value in ("", None):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _known_at_semantics(pit: Mapping[str, Any], action: Mapping[str, Any]) -> str:
    if action.get("pit_caveat_required"):
        return (
            "PIT approximation: date-level known-at inferred for research-only "
            "dry-run diagnostics"
        )
    return str(pit.get("known_at_semantics") or "strict_known_at")


def _field_validation_status(
    wrapper_rows: Sequence[Mapping[str, Any]],
    fields: Sequence[str],
) -> str:
    if not wrapper_rows:
        return "FAIL"
    missing = [
        field
        for row in wrapper_rows
        for field in fields
        if row.get(field) in ("", None)
    ]
    return "PASS" if not missing else "FAIL"


def _validation_rule(field: str) -> str:
    if field in {"promotion_allowed", "paper_shadow_allowed", "production_allowed"}:
        return "must_be_false"
    if field == "broker_action":
        return "must_equal_none"
    if field == "target_exposure":
        return "numeric_research_baseline_field_not_actionable_weight"
    if field in {"as_of_timestamp", "decision_timestamp"}:
        return "timestamp_or_explicit_pit_caveat_required"
    return "non_empty_or_documented_fallback"


def _selected_missing_timestamp(
    gap_rows: Sequence[Mapping[str, Any]],
    selected_source_id: str,
) -> bool:
    return any(
        str(row.get("source_id")) == selected_source_id
        and str(row.get("required_field")) in {"as_of_timestamp", "decision_timestamp"}
        and row.get("field_available") is not True
        for row in gap_rows
    )


def _strings(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _safety_subset() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_source_remediation_report(
    summary: Mapping[str, Any],
    family_rows: Sequence[Mapping[str, Any]],
    action_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    top_families = family_rows[:5]
    remediable = [row for row in action_rows if row.get("wrapper_allowed")]
    lines = [
        "# Dynamic Target Baseline Source Remediation",
        "",
        (
            "TRADING-2329 只做 source remediation、schema adapter 和 "
            "research-only wrapper preparation。"
        ),
        (
            "TRADING-2328 已发现 34 个 candidate artifacts，但 0 个 PIT-ready "
            "source，因此不能直接进入 dynamic target baseline simulation。"
        ),
        "",
        f"- status: `{summary['status']}`",
        f"- upstream_candidate_artifacts_found: `{summary['upstream_candidate_artifacts_found']}`",
        f"- upstream_pit_ready_source_count: `{summary['upstream_pit_ready_source_count']}`",
        f"- remediable_source_count: `{summary['remediable_source_count']}`",
        f"- wrapper_generated: `{summary['wrapper_generated']}`",
        f"- wrapper_validation_status: `{wrapper_validation['validation_status']}`",
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
        (
            "`aits validate-data` 不适用：TRADING-2329 只读取 prior research "
            "outputs、static config、registry 和 candidate artifacts，不读取 "
            "cached market data 或 runtime exposure data。"
        ),
        "",
        "## Source Families",
        "",
    ]
    for row in top_families:
        lines.append(
            f"- `{row['source_family']}`: best_source_id=`{row['best_source_id']}`, "
            f"ranking_label=`{row['ranking_label']}`, score=`{row['ranking_score']}`"
        )
    lines.extend(["", "## Remediation", ""])
    if remediable:
        for row in remediable[:10]:
            lines.append(
                f"- `{row['source_id']}`: `{row['remediation_status']}` / "
                f"`{row['remediation_action']}`"
            )
    else:
        lines.append(
            "- 当前没有可 remediation source；需要生成 native dynamic target "
            "baseline source。"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            (
                "Wrapper 中的 `target_exposure` 只是 research baseline field，"
                "不是 trading target weight、rebalance instruction、buy/sell "
                "signal、paper-shadow、production 或 broker action。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_schema_contract_doc(schema_contract: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Target Baseline Schema Contract",
        "",
        f"- baseline_schema_version: `{schema_contract['baseline_schema_version']}`",
        "- promotion_allowed: `False`",
        "- paper_shadow_allowed: `False`",
        "- production_allowed: `False`",
        "- broker_action: `none`",
        "",
        "## Required Fields",
        "",
    ]
    for field in schema_contract.get("required_fields", []):
        lines.append(f"- `{field}`")
    lines.extend(
        [
            "",
            "`target_exposure` 是 research baseline field，不得解释为交易 target weight。",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_adapter_spec_doc(
    summary: Mapping[str, Any],
    adapter_rows: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Dynamic Target Schema Adapter Spec",
        "",
        f"- adapter_row_count: `{len(adapter_rows)}`",
        f"- wrapper_generated: `{summary['wrapper_generated']}`",
        "",
        "## Mapping Rules",
        "",
    ]
    for row in adapter_rows[:30]:
        lines.append(
            f"- `{row['source_id']}`: `{row['source_field']}` -> "
            f"`{row['target_baseline_field']}` via `{row['mapping_rule']}`"
        )
    return "\n".join(lines) + "\n"


def _render_pit_caveat_doc(pit_caveat_report: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Target Wrapper PIT Caveat Report",
        "",
        f"- baseline_id: `{pit_caveat_report.get('baseline_id', '')}`",
        f"- source_id: `{pit_caveat_report.get('source_id', '')}`",
        f"- pit_policy: `{pit_caveat_report.get('pit_policy', '')}`",
        f"- strict_pit_ready: `{pit_caveat_report.get('strict_pit_ready')}`",
        f"- pit_approximation_ready: `{pit_caveat_report.get('pit_approximation_ready')}`",
        f"- lookahead_risk: `{pit_caveat_report.get('lookahead_risk')}`",
        f"- revision_risk: `{pit_caveat_report.get('revision_risk')}`",
        "",
        "Blocked usage remains promotion, paper-shadow, production and broker action.",
    ]
    return "\n".join(lines) + "\n"


def _render_2330_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Target 2330 Readiness Route",
        "",
        f"- readiness_status: `{readiness.get('readiness_status')}`",
        f"- 2330_allowed: `{readiness.get('2330_allowed')}`",
        f"- next_task: `{task_route.get('next_task')}`",
        f"- caveat: `{task_route.get('caveat', '')}`",
        "",
        "TRADING-2330 之前仍不得打开 promotion、paper-shadow、production 或 broker action。",
    ]
    return "\n".join(lines) + "\n"
