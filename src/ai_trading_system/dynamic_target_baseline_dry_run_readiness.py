from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_target_baseline_preparation import (
    DEFAULT_SIMULATION_POLICY_ROOT,
    DEFAULT_SOURCE_BINDING_ROOT,
    DEFAULT_STATIC_DRY_RUN_ROOT,
    DynamicTargetBaselinePreparationError,
    load_trading_2323_policy_outputs,
    load_trading_2324_source_binding_outputs,
    load_trading_2326_static_dry_run_outputs,
)
from ai_trading_system.dynamic_target_baseline_preparation import (
    _validate_no_unsafe_fields as validate_no_unsafe_fields,
)
from ai_trading_system.dynamic_target_baseline_source_remediation import (
    DEFAULT_DYNAMIC_PREPARATION_ROOT,
    load_trading_2328_dynamic_preparation_outputs,
)
from ai_trading_system.dynamic_target_baseline_source_remediation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SOURCE_REMEDIATION_ROOT,
)
from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
)
from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    load_trading_2329_source_remediation_outputs,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    mapping,
    records,
    round_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2331_DYNAMIC_TARGET_BASELINE_DRY_RUN_READINESS_WITH_PIT_CAVEAT"
REPORT_TYPE = "dynamic_target_baseline_dry_run_readiness_with_pit_caveat"
ARTIFACT_ROLE = "dynamic_target_baseline_dry_run_readiness_with_pit_caveat"
MODE = "dry_run_readiness_with_pit_caveat"
STATUS = "DYNAMIC_TARGET_BASELINE_DRY_RUN_READINESS_READY_PROMOTION_BLOCKED"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_DRY_RUN_READINESS_ONLY"

UPSTREAM_NEXT_TASK = "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat"
NEXT_DRY_RUN_TASK = (
    "TRADING-2332_Source_Bound_Exposure_Cap_Dry_Run_With_Dynamic_Target_Baseline"
)
NEXT_WRAPPER_REMEDIATION_TASK = (
    "TRADING-2332_Dynamic_Target_Baseline_Wrapper_Remediation"
)
NEXT_ALIGNMENT_REMEDIATION_TASK = (
    "TRADING-2332_Dynamic_Target_Baseline_Alignment_Remediation"
)
NEXT_PIT_CAVEAT_REMEDIATION_TASK = (
    "TRADING-2332_Dynamic_Target_Baseline_PIT_Caveat_Remediation"
)
NEXT_POLICY_REMEDIATION_TASK = (
    "TRADING-2332_Dynamic_Target_Baseline_Policy_Compatibility_Remediation"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

WRAPPER_IDENTITY_FIELDS = (
    "baseline_id",
    "source_id",
    "source_family",
    "source_path",
    "source_hash",
    "source_artifact_hash",
    "baseline_schema_version",
    "adapter_id",
)
WRAPPER_BASELINE_FIELDS = (
    "date",
    "target_asset",
    "target_exposure",
    "target_exposure_role",
    "risk_asset_exposure",
    "asset_weight",
    "cash_weight",
)
WRAPPER_TIMESTAMP_FIELDS = (
    "as_of_timestamp",
    "decision_timestamp",
    "valid_from",
    "valid_until",
    "rebalance_timestamp",
    "generated_at",
)
WRAPPER_POLICY_FIELDS = (
    "timestamp_remediation_policy_id",
    "timestamp_derivation_mode",
    "pit_policy",
    "known_at_policy",
    "latency_policy",
    "rebalance_policy",
    "replayability_status",
    "simulation_ready_candidate",
)
WRAPPER_SAFETY_FIELDS = (
    "promotion_allowed",
    "paper_shadow_allowed",
    "production_allowed",
    "broker_action",
)
WRAPPER_REQUIRED_FIELDS = (
    *WRAPPER_IDENTITY_FIELDS,
    *WRAPPER_BASELINE_FIELDS,
    *WRAPPER_TIMESTAMP_FIELDS,
    *WRAPPER_POLICY_FIELDS,
    *WRAPPER_SAFETY_FIELDS,
)

EXPECTED_KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
EXPECTED_LATENCY_POLICY = "NEXT_TRADING_DAY_DECISION"
EXPECTED_REBALANCE_POLICY = "DAILY_DECISION_REBALANCE"


class DynamicTargetBaselineDryRunReadinessError(ValueError):
    pass


def run_dynamic_target_baseline_dry_run_readiness_with_pit_caveat(
    *,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    source_remediation_dir: Path = DEFAULT_SOURCE_REMEDIATION_ROOT,
    dynamic_preparation_dir: Path = DEFAULT_DYNAMIC_PREPARATION_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    static_dry_run_dir: Path = DEFAULT_STATIC_DRY_RUN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicTargetBaselineDryRunReadinessError(
            f"dynamic target baseline dry-run readiness only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_target_dry_run_readiness_inputs(
        timestamp_remediation_dir=timestamp_remediation_dir,
        source_remediation_dir=source_remediation_dir,
        dynamic_preparation_dir=dynamic_preparation_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        static_dry_run_dir=static_dry_run_dir,
    )
    wrapper_rows = _rows_from_payload(inputs["timestamp_remediation"]["wrapper"])
    selected_source = _selected_source_identity(wrapper_rows)
    timestamp_summary = mapping(inputs["timestamp_remediation"]["summary"])
    wrapper_validation = mapping(inputs["timestamp_remediation"]["wrapper_validation"])
    pit_caveat = mapping(inputs["timestamp_remediation"]["pit_caveat"])
    risk_cap_alignment = mapping(inputs["timestamp_remediation"]["risk_cap_alignment"])

    wrapper_field_rows = build_dynamic_dry_run_wrapper_field_validation_matrix(
        wrapper_rows
    )
    timestamp_alignment_rows = build_dynamic_dry_run_timestamp_alignment_matrix(
        wrapper_rows
    )
    risk_cap_alignment_rows = build_dynamic_dry_run_risk_cap_alignment_matrix(
        wrapper_rows=wrapper_rows,
        risk_cap_alignment=risk_cap_alignment,
    )
    market_data_alignment_rows = build_dynamic_dry_run_market_data_alignment_matrix(
        wrapper_rows=wrapper_rows,
        source_binding=mapping(inputs["source_binding"]),
        static_dry_run=mapping(inputs["static_dry_run"]),
    )
    policy_compatibility_rows = build_dynamic_dry_run_policy_compatibility_matrix(
        wrapper_rows=wrapper_rows,
        timestamp_remediation=mapping(inputs["timestamp_remediation"]),
        source_binding=mapping(inputs["source_binding"]),
        simulation_policy=mapping(inputs["policy"]),
        static_dry_run=mapping(inputs["static_dry_run"]),
    )
    pit_acceptance = build_dynamic_dry_run_pit_caveat_acceptance_report(
        wrapper_rows=wrapper_rows,
        pit_caveat=pit_caveat,
        known_at_report=mapping(inputs["timestamp_remediation"]["known_at"]),
        risk_cap_alignment=risk_cap_alignment,
    )
    data_quality_precheck = build_dynamic_dry_run_data_quality_precheck(
        source_binding=mapping(inputs["source_binding"]),
        static_dry_run=mapping(inputs["static_dry_run"]),
    )
    input_contract = build_dynamic_dry_run_input_contract(
        wrapper_rows=wrapper_rows,
        selected_source=selected_source,
        pit_acceptance=pit_acceptance,
        data_quality_precheck=data_quality_precheck,
    )
    interpretation_boundary = build_dynamic_dry_run_interpretation_boundary(
        generated_at
    )
    gate_checklist = build_dynamic_dry_run_gate_checklist(
        timestamp_summary=timestamp_summary,
        wrapper_validation=wrapper_validation,
        wrapper_field_rows=wrapper_field_rows,
        timestamp_alignment_rows=timestamp_alignment_rows,
        risk_cap_alignment_rows=risk_cap_alignment_rows,
        market_data_alignment_rows=market_data_alignment_rows,
        policy_compatibility_rows=policy_compatibility_rows,
        pit_acceptance=pit_acceptance,
        data_quality_precheck=data_quality_precheck,
    )
    readiness = build_dynamic_dry_run_2332_readiness_matrix(
        selected_source=selected_source,
        wrapper_validation=wrapper_validation,
        gate_checklist=gate_checklist,
    )
    task_route = build_dynamic_dry_run_2332_task_route(readiness)
    summary = build_dynamic_dry_run_readiness_summary(
        generated_at=generated_at,
        timestamp_remediation_dir=timestamp_remediation_dir,
        source_remediation_dir=source_remediation_dir,
        dynamic_preparation_dir=dynamic_preparation_dir,
        source_binding_dir=source_binding_dir,
        simulation_policy_dir=simulation_policy_dir,
        static_dry_run_dir=static_dry_run_dir,
        wrapper_rows=wrapper_rows,
        wrapper_validation=wrapper_validation,
        pit_acceptance=pit_acceptance,
        gate_checklist=gate_checklist,
        readiness=readiness,
        task_route=task_route,
        mode=mode,
    )
    paths = write_dynamic_dry_run_readiness_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        gate_checklist=gate_checklist,
        pit_acceptance=pit_acceptance,
        wrapper_field_rows=wrapper_field_rows,
        timestamp_alignment_rows=timestamp_alignment_rows,
        risk_cap_alignment_rows=risk_cap_alignment_rows,
        market_data_alignment_rows=market_data_alignment_rows,
        policy_compatibility_rows=policy_compatibility_rows,
        input_contract=input_contract,
        data_quality_precheck=data_quality_precheck,
        interpretation_boundary=interpretation_boundary,
        readiness=readiness,
        task_route=task_route,
    )
    return {**summary, "output_paths": paths}


def load_dynamic_target_dry_run_readiness_inputs(
    *,
    timestamp_remediation_dir: Path,
    source_remediation_dir: Path,
    dynamic_preparation_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    static_dry_run_dir: Path,
) -> dict[str, Any]:
    timestamp_remediation = load_trading_2330_timestamp_remediation_outputs(
        timestamp_remediation_dir
    )
    try:
        source_remediation = load_trading_2329_source_remediation_outputs(
            source_remediation_dir
        )
        dynamic_preparation = load_trading_2328_dynamic_preparation_outputs(
            dynamic_preparation_dir
        )
        source_binding = load_trading_2324_source_binding_outputs(source_binding_dir)
        policy = load_trading_2323_policy_outputs(simulation_policy_dir)
        static_dry_run = load_trading_2326_static_dry_run_outputs(static_dry_run_dir)
    except (DynamicTargetBaselinePreparationError, ValueError) as exc:
        raise DynamicTargetBaselineDryRunReadinessError(str(exc)) from exc
    return {
        "timestamp_remediation": timestamp_remediation,
        "source_remediation": source_remediation,
        "dynamic_preparation": dynamic_preparation,
        "source_binding": source_binding,
        "policy": policy,
        "static_dry_run": static_dry_run,
    }


def load_trading_2330_timestamp_remediation_outputs(
    timestamp_remediation_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": timestamp_remediation_dir
        / "dynamic_target_timestamp_remediation_summary.json",
        "policy": timestamp_remediation_dir
        / "dynamic_target_timestamp_remediation_policy.json",
        "derivation": timestamp_remediation_dir
        / "dynamic_target_timestamp_derivation_matrix.json",
        "known_at": timestamp_remediation_dir
        / "dynamic_target_known_at_semantics_report.json",
        "validity": timestamp_remediation_dir
        / "dynamic_target_validity_window_remediation_report.json",
        "latency": timestamp_remediation_dir / "dynamic_target_latency_policy_report.json",
        "rebalance": timestamp_remediation_dir
        / "dynamic_target_rebalance_timing_report.json",
        "wrapper": timestamp_remediation_dir
        / "dynamic_target_timestamp_remediated_wrapper_artifact.json",
        "wrapper_validation": timestamp_remediation_dir
        / "dynamic_target_timestamp_wrapper_validation_summary.json",
        "pit_caveat": timestamp_remediation_dir
        / "dynamic_target_timestamp_pit_caveat_report.json",
        "risk_cap_alignment": timestamp_remediation_dir
        / "dynamic_target_risk_cap_timestamp_alignment_report.json",
        "readiness": timestamp_remediation_dir / "dynamic_target_2331_readiness_matrix.json",
        "task_route": timestamp_remediation_dir / "dynamic_target_2331_task_route.json",
        "safety_boundary": timestamp_remediation_dir
        / "dynamic_target_timestamp_remediation_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2330 timestamp remediation")
    _validate_timestamp_remediation_inputs(payloads)
    return {"source_dir": str(timestamp_remediation_dir), "paths": _string_paths(paths), **payloads}


def build_dynamic_dry_run_wrapper_field_validation_matrix(
    wrapper_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    timestamp_approximation = any(
        str(row.get("timestamp_derivation_mode")) != "native_timestamp_copy"
        or str(row.get("pit_policy")) != "STRICT_PIT_READY"
        for row in wrapper_rows
    )
    for field in WRAPPER_REQUIRED_FIELDS:
        group = _field_group(field)
        present_count = sum(1 for row in wrapper_rows if not _field_missing(row, field))
        missing_count = len(wrapper_rows) - present_count
        invalid_count = sum(1 for row in wrapper_rows if _field_invalid(row, field))
        coverage_ratio = _ratio(present_count, len(wrapper_rows))
        blocking = missing_count > 0 or invalid_count > 0
        status = "PASS"
        caveat = ""
        if blocking:
            status = "FAIL"
            caveat = "required wrapper field missing or invalid"
        elif group == "timestamp" and timestamp_approximation:
            status = "PASS_WITH_WARNINGS"
            caveat = "timestamp field is present but uses PIT approximation policy"
        elif field in {"pit_policy", "known_at_policy"} and timestamp_approximation:
            status = "PASS_WITH_WARNINGS"
            caveat = "policy field carries PIT caveat forward"
        rows.append(
            {
                "field_name": field,
                "field_group": group,
                "required_for_2332": True,
                "record_count": len(wrapper_rows),
                "present_count": present_count,
                "missing_count": missing_count,
                "invalid_count": invalid_count,
                "coverage_ratio": coverage_ratio,
                "validation_status": status,
                "blocking": blocking,
                "caveat": caveat,
                **_safety_subset(),
            }
        )
    return rows


def build_dynamic_dry_run_timestamp_alignment_matrix(
    wrapper_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    check_specs = [
        (
            "as_of_to_decision_latency",
            "decision_timestamp",
            _decision_after_as_of,
            "decision timestamp must not precede as-of timestamp",
        ),
        (
            "decision_to_validity_window",
            "valid_from",
            _valid_from_after_decision,
            "valid_from must be no earlier than decision timestamp",
        ),
        (
            "validity_window_order",
            "valid_until",
            _validity_window_ordered,
            "valid_until must be no earlier than valid_from",
        ),
        (
            "rebalance_timing",
            "rebalance_timestamp",
            _rebalance_after_decision,
            "rebalance timestamp must not precede decision timestamp",
        ),
    ]
    rows: list[dict[str, Any]] = []
    for check_id, field, validator, requirement in check_specs:
        present_rows = [row for row in wrapper_rows if not _is_missing(row.get(field))]
        invalid_count = sum(1 for row in wrapper_rows if not validator(row))
        blocking = invalid_count > 0 or len(present_rows) != len(wrapper_rows)
        rows.append(
            {
                "check_id": check_id,
                "timestamp_field": field,
                "record_count": len(wrapper_rows),
                "coverage_ratio": _ratio(len(present_rows), len(wrapper_rows)),
                "invalid_count": invalid_count,
                "alignment_status": "FAIL" if blocking else "READY_WITH_WARNINGS",
                "blocking": blocking,
                "requirement": requirement,
                "caveat": ""
                if blocking
                else "date-level next-session policy remains PIT approximation",
                **_safety_subset(),
            }
        )

    known_at_count = sum(
        1
        for row in wrapper_rows
        if str(row.get("known_at_policy")) == EXPECTED_KNOWN_AT_POLICY
    )
    derivation_modes = sorted(
        {
            str(row.get("timestamp_derivation_mode"))
            for row in wrapper_rows
            if not _is_missing(row.get("timestamp_derivation_mode"))
        }
    )
    rows.append(
        {
            "check_id": "known_at_policy",
            "timestamp_field": "known_at_policy",
            "record_count": len(wrapper_rows),
            "coverage_ratio": _ratio(known_at_count, len(wrapper_rows)),
            "invalid_count": len(wrapper_rows) - known_at_count,
            "alignment_status": "PASS" if known_at_count == len(wrapper_rows) else "FAIL",
            "blocking": known_at_count != len(wrapper_rows),
            "requirement": EXPECTED_KNOWN_AT_POLICY,
            "caveat": "known-at uses next-session policy instead of native intraday timestamp",
            **_safety_subset(),
        }
    )
    rows.append(
        {
            "check_id": "timestamp_derivation_mode",
            "timestamp_field": "timestamp_derivation_mode",
            "record_count": len(wrapper_rows),
            "coverage_ratio": 1.0 if derivation_modes else 0.0,
            "invalid_count": 0 if derivation_modes else len(wrapper_rows),
            "alignment_status": "READY_WITH_WARNINGS" if derivation_modes else "FAIL",
            "blocking": not bool(derivation_modes),
            "requirement": "timestamp derivation mode must be explicit",
            "derivation_modes": derivation_modes,
            "caveat": "non-native timestamp derivation must carry PIT caveat",
            **_safety_subset(),
        }
    )
    return rows


def build_dynamic_dry_run_risk_cap_alignment_matrix(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment: Mapping[str, Any],
) -> list[dict[str, Any]]:
    alignment_status = str(
        risk_cap_alignment.get("alignment_readiness_status") or "UNKNOWN"
    )
    overlap_count = int(risk_cap_alignment.get("overlap_record_count") or 0)
    report_warnings = _strings(risk_cap_alignment.get("alignment_warnings"))
    rows = [
        {
            "check_id": "risk_cap_trigger_series_available",
            "source_status": bool(risk_cap_alignment.get("risk_cap_trigger_series_available")),
            "record_count": len(wrapper_rows),
            "coverage_ratio": 1.0
            if risk_cap_alignment.get("risk_cap_trigger_series_available")
            else 0.0,
            "alignment_status": "PASS"
            if risk_cap_alignment.get("risk_cap_trigger_series_available")
            else "FAIL",
            "blocking": not bool(risk_cap_alignment.get("risk_cap_trigger_series_available")),
            "caveat": "",
            **_safety_subset(),
        },
        {
            "check_id": "risk_cap_date_overlap",
            "date_overlap_start": _string(risk_cap_alignment.get("date_overlap_start")),
            "date_overlap_end": _string(risk_cap_alignment.get("date_overlap_end")),
            "overlap_record_count": overlap_count,
            "record_count": len(wrapper_rows),
            "coverage_ratio": _ratio(overlap_count, len(wrapper_rows)),
            "alignment_status": "READY_WITH_WARNINGS"
            if overlap_count > 0
            else "FAIL",
            "blocking": overlap_count <= 0,
            "caveat": "2332 must re-evaluate overlap against validated market data",
            **_safety_subset(),
        },
        {
            "check_id": "risk_cap_timestamp_alignment",
            "decision_timestamp_alignment_status": _string(
                risk_cap_alignment.get("decision_timestamp_alignment_status")
            ),
            "validity_window_alignment_status": _string(
                risk_cap_alignment.get("validity_window_alignment_status")
            ),
            "rebalance_timing_alignment_status": _string(
                risk_cap_alignment.get("rebalance_timing_alignment_status")
            ),
            "alignment_status": "READY_WITH_WARNINGS"
            if alignment_status.endswith("WITH_WARNINGS")
            else alignment_status,
            "blocking": "BLOCKED" in alignment_status or alignment_status == "FAIL",
            "caveat": "; ".join(report_warnings)
            or "timestamp alignment remains caveated until 2332 dry-run",
            **_safety_subset(),
        },
        {
            "check_id": "risk_cap_asset_overlap",
            "asset_overlap_status": _string(
                risk_cap_alignment.get("asset_overlap_status")
            ),
            "horizon_overlap_status": _string(
                risk_cap_alignment.get("horizon_overlap_status")
            ),
            "alignment_status": "READY_WITH_WARNINGS",
            "blocking": False,
            "caveat": "asset and horizon overlap require 2332 dry-run recheck",
            **_safety_subset(),
        },
    ]
    return rows


def build_dynamic_dry_run_market_data_alignment_matrix(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    source_binding: Mapping[str, Any],
    static_dry_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    market_binding = mapping(source_binding.get("market_data_binding"))
    static_summary = mapping(static_dry_run.get("summary"))
    static_quality = mapping(static_dry_run.get("data_quality_report"))
    wrapper_assets = sorted(
        {
            str(row.get("target_asset"))
            for row in wrapper_rows
            if not _is_missing(row.get("target_asset"))
        }
    )
    market_assets = _strings(market_binding.get("target_assets"))
    missing_assets = sorted(set(wrapper_assets) - set(market_assets))
    wrapper_start, wrapper_end = _wrapper_date_range(wrapper_rows)
    market_start = _parse_date(market_binding.get("coverage_start")) or _parse_date(
        static_summary.get("simulation_start")
    )
    market_end = _parse_date(market_binding.get("coverage_end")) or _parse_date(
        static_summary.get("simulation_end")
    )
    overlap_start, overlap_end = _date_overlap(wrapper_start, wrapper_end, market_start, market_end)
    date_warning = bool(
        wrapper_start and market_start and wrapper_start < market_start
        or wrapper_end and market_end and wrapper_end > market_end
    )
    return [
        {
            "check_id": "market_asset_coverage",
            "wrapper_assets": wrapper_assets,
            "market_data_assets": market_assets,
            "missing_assets": missing_assets,
            "coverage_ratio": _ratio(
                len(wrapper_assets) - len(missing_assets),
                len(wrapper_assets),
            ),
            "alignment_status": "PASS" if not missing_assets else "READY_WITH_WARNINGS",
            "blocking": False,
            "caveat": ""
            if not missing_assets
            else "2332 must validate or map missing wrapper assets before simulation",
            **_safety_subset(),
        },
        {
            "check_id": "market_date_overlap",
            "wrapper_start": _date_string(wrapper_start),
            "wrapper_end": _date_string(wrapper_end),
            "market_coverage_start": _date_string(market_start),
            "market_coverage_end": _date_string(market_end),
            "overlap_start": _date_string(overlap_start),
            "overlap_end": _date_string(overlap_end),
            "alignment_status": "READY_WITH_WARNINGS"
            if overlap_start and overlap_end
            else "FAIL",
            "blocking": not (overlap_start and overlap_end),
            "caveat": "2332 must trim or validate date intersection"
            if date_warning
            else "",
            **_safety_subset(),
        },
        {
            "check_id": "market_data_quality_context",
            "source_binding_data_quality_status": _string(
                market_binding.get("data_quality_status")
            ),
            "static_dry_run_data_quality_status": _string(
                static_quality.get("data_quality_status")
            ),
            "alignment_status": "READY_WITH_WARNINGS",
            "blocking": False,
            "caveat": (
                "TRADING-2331 does not re-run aits validate-data; TRADING-2332 "
                "must enforce it before consuming cached market data"
            ),
            **_safety_subset(),
        },
    ]


def build_dynamic_dry_run_policy_compatibility_matrix(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    timestamp_remediation: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    simulation_policy: Mapping[str, Any],
    static_dry_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    summary = mapping(timestamp_remediation.get("summary"))
    source_binding_summary = mapping(source_binding.get("summary"))
    simulation_summary = mapping(simulation_policy.get("summary"))
    turnover_report = mapping(source_binding.get("turnover_rebalance_assumption"))
    market_binding = mapping(source_binding.get("market_data_binding"))
    return [
        _policy_row(
            "exposure_cap_policy",
            simulation_summary.get("status") or simulation_summary.get("artifact_role"),
            "exposure-cap policy/readiness context available",
            "PASS",
            "",
        ),
        _policy_row(
            "cooldown_policy",
            source_binding_summary.get("status") or source_binding_summary.get("artifact_role"),
            "source-bound cooldown/policy context available",
            "PASS",
            "",
        ),
        _policy_row(
            "latency_policy",
            summary.get("latency_policy") or _common_value(wrapper_rows, "latency_policy"),
            EXPECTED_LATENCY_POLICY,
            "PASS",
            "",
        ),
        _policy_row(
            "rebalance_policy",
            summary.get("rebalance_policy") or _common_value(wrapper_rows, "rebalance_policy"),
            EXPECTED_REBALANCE_POLICY,
            "PASS",
            "",
        ),
        _policy_row(
            "turnover_assumption",
            turnover_report.get("status") or turnover_report.get("artifact_role"),
            "turnover/rebalance assumption context available",
            "PASS",
            "",
        ),
        _policy_row(
            "simulation_calendar",
            market_binding.get("coverage_start")
            or source_binding_summary.get("actual_requested_date_range"),
            "simulation calendar and market-data coverage context available",
            "PASS",
            "",
        ),
        _policy_row(
            "risk_asset_classification",
            ",".join(
                sorted(
                    {
                        str(row.get("target_asset"))
                        for row in wrapper_rows
                        if not _is_missing(row.get("target_asset"))
                    }
                )
            ),
            "dynamic baseline target assets recorded for 2332 classification",
            "PASS",
            "",
        ),
    ]


def build_dynamic_dry_run_pit_caveat_acceptance_report(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    pit_caveat: Mapping[str, Any],
    known_at_report: Mapping[str, Any],
    risk_cap_alignment: Mapping[str, Any],
) -> dict[str, Any]:
    pit_policies = sorted(
        {
            str(row.get("pit_policy"))
            for row in wrapper_rows
            if not _is_missing(row.get("pit_policy"))
        }
    )
    derivation_modes = sorted(
        {
            str(row.get("timestamp_derivation_mode"))
            for row in wrapper_rows
            if not _is_missing(row.get("timestamp_derivation_mode"))
        }
    )
    strict_ready = bool(pit_caveat.get("strict_pit_ready")) and pit_policies == [
        "STRICT_PIT_READY"
    ]
    approximation_ready = bool(pit_caveat.get("pit_approximation_ready")) or (
        "PIT_APPROXIMATION_READY" in pit_policies
    )
    accepted = approximation_ready or strict_ready
    carry_forward = sorted(
        set(_strings(pit_caveat.get("known_at_caveats")))
        | set(_strings(risk_cap_alignment.get("alignment_warnings")))
        | {
            "TRADING-2332 must re-run data quality gate before consuming cached market data",
            "target_exposure remains a research baseline field only",
        }
    )
    return {
        "artifact_role": "dynamic_dry_run_pit_caveat_acceptance_report",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.pit_caveat_acceptance.v1",
        "strict_pit_ready": strict_ready,
        "pit_approximation_ready": approximation_ready,
        "pit_caveat_accepted": accepted,
        "acceptance_status": "PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_DRY_RUN_WITH_WARNINGS"
        if accepted and not strict_ready
        else "PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_DRY_RUN"
        if accepted
        else "PIT_CAVEAT_NOT_ACCEPTED",
        "wrapper_record_count": len(wrapper_rows),
        "pit_policies": pit_policies,
        "timestamp_derivation_modes": derivation_modes,
        "known_at_policy": _string(known_at_report.get("known_at_policy")),
        "lookahead_risk": _string(pit_caveat.get("lookahead_risk")),
        "revision_risk": _string(pit_caveat.get("revision_risk")),
        "allowed_usage": [
            "research_only_dynamic_dry_run",
            "source_bound_simulation_proxy",
            "exposure_cap_diagnostics",
        ],
        "blocked_usage": [
            "promotion",
            "paper_shadow",
            "production",
            "broker_action",
            "real_portfolio_decision",
        ],
        "carry_forward_caveats": carry_forward,
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_data_quality_precheck(
    *,
    source_binding: Mapping[str, Any],
    static_dry_run: Mapping[str, Any],
) -> dict[str, Any]:
    market_binding = mapping(source_binding.get("market_data_binding"))
    static_quality = mapping(static_dry_run.get("data_quality_report"))
    return {
        "artifact_role": "dynamic_dry_run_data_quality_precheck",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.data_quality_precheck.v1",
        "precheck_status": "PASS_WITH_WARNINGS",
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_gate_required": False,
        "data_quality_gate_executed": False,
        "aits_validate_data_executed": False,
        "2332_data_quality_gate_required": True,
        "2332_required_gate": (
            "aits validate-data or same validation code path before cached market "
            "data consumption"
        ),
        "source_binding_data_quality_status": _string(
            market_binding.get("data_quality_status")
        ),
        "static_dry_run_data_quality_status": _string(
            static_quality.get("data_quality_status")
        ),
        "source_binding_quality_warning_count": int(
            mapping(market_binding.get("data_quality_gate")).get("warning_count") or 0
        ),
        "static_dry_run_quality_warning_count": int(
            static_quality.get("warning_count") or 0
        ),
        "rationale": (
            "TRADING-2331 only reads prior artifacts; TRADING-2332 must enforce "
            "data quality before simulation"
        ),
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_input_contract(
    *,
    wrapper_rows: Sequence[Mapping[str, Any]],
    selected_source: Mapping[str, Any],
    pit_acceptance: Mapping[str, Any],
    data_quality_precheck: Mapping[str, Any],
) -> dict[str, Any]:
    wrapper_start, wrapper_end = _wrapper_date_range(wrapper_rows)
    return {
        "artifact_role": "dynamic_dry_run_input_contract",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.input_contract.v1",
        "contract_status": "READY_WITH_WARNINGS"
        if pit_acceptance.get("pit_caveat_accepted")
        else "BLOCKED",
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "source_family": _string(selected_source.get("source_family")),
        "source_path": _string(selected_source.get("source_path")),
        "source_artifact_hash": _string(selected_source.get("source_artifact_hash")),
        "baseline_schema_version": _string(selected_source.get("baseline_schema_version")),
        "wrapper_record_count": len(wrapper_rows),
        "wrapper_date_start": _date_string(wrapper_start),
        "wrapper_date_end": _date_string(wrapper_end),
        "required_wrapper_fields": list(WRAPPER_REQUIRED_FIELDS),
        "required_source_lineage_fields": list(WRAPPER_IDENTITY_FIELDS),
        "required_timestamp_policy": {
            "known_at_policy": EXPECTED_KNOWN_AT_POLICY,
            "latency_policy": EXPECTED_LATENCY_POLICY,
            "rebalance_policy": EXPECTED_REBALANCE_POLICY,
            "pit_caveat_required": True,
        },
        "data_quality_boundary": {
            "2331_data_quality_status": DATA_QUALITY_STATUS,
            "2331_aits_validate_data_executed": False,
            "2332_data_quality_gate_required": bool(
                data_quality_precheck.get("2332_data_quality_gate_required")
            ),
        },
        "forbidden_action_fields": [
            "target_weight",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
        ],
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_interpretation_boundary(
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "artifact_role": "dynamic_dry_run_interpretation_boundary",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
        "generated_at": generated_at.isoformat(),
        "research_only": True,
        "dry_run_readiness_only": True,
        "simulation_executed": False,
        "cached_market_data_consumed": False,
        "runtime_exposure_data_consumed": False,
        "portfolio_effect": "none",
        "production_effect": "none",
        "manual_review_only": True,
        "allowed_current_usage": "2332_readiness_routing_only",
        "not_evidence_of_exposure_cap_effectiveness": True,
        "not_trading_instruction": True,
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_gate_checklist(
    *,
    timestamp_summary: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
    wrapper_field_rows: Sequence[Mapping[str, Any]],
    timestamp_alignment_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment_rows: Sequence[Mapping[str, Any]],
    market_data_alignment_rows: Sequence[Mapping[str, Any]],
    policy_compatibility_rows: Sequence[Mapping[str, Any]],
    pit_acceptance: Mapping[str, Any],
    data_quality_precheck: Mapping[str, Any],
) -> dict[str, Any]:
    check_rows = [
        _gate_row(
            "2330_route",
            timestamp_summary.get("next_task") == UPSTREAM_NEXT_TASK
            and timestamp_summary.get("2331_allowed") is True,
            _string(timestamp_summary.get("next_task")),
            "",
        ),
        _gate_row(
            "wrapper_validation",
            str(wrapper_validation.get("validation_status")) in {"PASS", "PASS_WITH_WARNINGS"},
            _string(wrapper_validation.get("validation_status")),
            "PASS_WITH_WARNINGS allowed only with caveat carry-forward",
            warning=str(wrapper_validation.get("validation_status")) == "PASS_WITH_WARNINGS",
        ),
        _gate_row(
            "wrapper_required_fields",
            not _has_blocking_row(wrapper_field_rows),
            _overall_matrix_status(wrapper_field_rows, "validation_status"),
            "all required wrapper fields must be present and valid",
            warning=_has_warning_row(wrapper_field_rows, "validation_status"),
        ),
        _gate_row(
            "pit_caveat_acceptance",
            bool(pit_acceptance.get("pit_caveat_accepted")),
            _string(pit_acceptance.get("acceptance_status")),
            "PIT approximation accepted for research dry-run readiness only",
            warning=str(pit_acceptance.get("acceptance_status")).endswith("WITH_WARNINGS"),
        ),
        _matrix_gate_row(
            "timestamp_alignment",
            timestamp_alignment_rows,
            "timestamp alignment must not have blockers",
        ),
        _matrix_gate_row(
            "risk_cap_alignment",
            risk_cap_alignment_rows,
            "risk-cap alignment must not have blockers",
        ),
        _matrix_gate_row(
            "market_data_alignment",
            market_data_alignment_rows,
            "market-data alignment must not have blockers",
        ),
        _matrix_gate_row(
            "policy_compatibility",
            policy_compatibility_rows,
            "policy compatibility must not have blockers",
            status_key="compatibility_status",
        ),
        _gate_row(
            "data_quality_boundary",
            data_quality_precheck.get("precheck_status") in {"PASS", "PASS_WITH_WARNINGS"},
            _string(data_quality_precheck.get("precheck_status")),
            "TRADING-2332 must enforce data quality before consuming cached market data",
            warning=True,
        ),
        _gate_row(
            "safety_boundary",
            True,
            "PASS",
            "promotion, paper-shadow, production and broker action remain closed",
        ),
    ]
    blockers = [
        str(row["check_id"]) for row in check_rows if row.get("blocking") is True
    ]
    warnings = [
        str(row["check_id"])
        for row in check_rows
        if row.get("warning") is True and row.get("blocking") is not True
    ]
    gate_status = (
        "DYNAMIC_DRY_RUN_READINESS_BLOCKED"
        if blockers
        else "DYNAMIC_DRY_RUN_READY_WITH_PIT_CAVEAT"
    )
    return {
        "artifact_role": "dynamic_dry_run_gate_checklist",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.gate_checklist.v1",
        "gate_status": gate_status,
        "2332_allowed": not bool(blockers),
        "blockers": blockers,
        "warnings": warnings,
        "rows": check_rows,
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_2332_readiness_matrix(
    *,
    selected_source: Mapping[str, Any],
    wrapper_validation: Mapping[str, Any],
    gate_checklist: Mapping[str, Any],
) -> dict[str, Any]:
    wrapper_status = str(wrapper_validation.get("validation_status"))
    blockers = _strings(gate_checklist.get("blockers"))
    if wrapper_status == "FAIL":
        readiness_status = "DYNAMIC_DRY_RUN_BLOCKED_WRAPPER_REMEDIATION_REQUIRED"
        allowed = False
        next_task = NEXT_WRAPPER_REMEDIATION_TASK
    elif blockers:
        readiness_status = "DYNAMIC_DRY_RUN_BLOCKED_ALIGNMENT_OR_POLICY_REMEDIATION_REQUIRED"
        allowed = False
        next_task = _blocked_next_task(blockers)
    else:
        readiness_status = "DYNAMIC_DRY_RUN_READY_FOR_2332_WITH_PIT_CAVEAT"
        allowed = True
        next_task = NEXT_DRY_RUN_TASK
    return {
        "artifact_role": "dynamic_dry_run_2332_readiness_matrix",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.2332_readiness.v1",
        "baseline_id": _string(selected_source.get("baseline_id")),
        "source_id": _string(selected_source.get("source_id")),
        "wrapper_validation_status": wrapper_status,
        "gate_status": _string(gate_checklist.get("gate_status")),
        "readiness_status": readiness_status,
        "2332_allowed": allowed,
        "2332_blockers": blockers,
        "2332_warnings": _strings(gate_checklist.get("warnings")),
        "next_task": next_task,
        **_readiness_safety_subset(),
    }


def build_dynamic_dry_run_2332_task_route(
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    allowed = bool(readiness.get("2332_allowed"))
    next_task = _string(readiness.get("next_task")) or (
        NEXT_DRY_RUN_TASK if allowed else NEXT_WRAPPER_REMEDIATION_TASK
    )
    return {
        "artifact_role": "dynamic_dry_run_2332_task_route",
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "schema_version": f"{REPORT_TYPE}.2332_task_route.v1",
        "readiness_status": _string(readiness.get("readiness_status")),
        "2332_allowed": allowed,
        "next_task": next_task,
        "caveat": "PIT_CAVEAT_AND_MARKET_DATA_RECHECK_REQUIRED"
        if allowed
        else "REMEDIATION_REQUIRED_BEFORE_DYNAMIC_DRY_RUN",
        "simulation_executed": False,
        **_safety_subset(),
    }


def build_dynamic_dry_run_readiness_summary(
    *,
    generated_at: datetime,
    timestamp_remediation_dir: Path,
    source_remediation_dir: Path,
    dynamic_preparation_dir: Path,
    source_binding_dir: Path,
    simulation_policy_dir: Path,
    static_dry_run_dir: Path,
    wrapper_rows: Sequence[Mapping[str, Any]],
    wrapper_validation: Mapping[str, Any],
    pit_acceptance: Mapping[str, Any],
    gate_checklist: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "schema_version": f"{REPORT_TYPE}.v1",
        "title": "Dynamic Target Baseline Dry-Run Readiness With PIT Caveat",
        "status": STATUS
        if readiness.get("2332_allowed")
        else "DYNAMIC_TARGET_BASELINE_DRY_RUN_READINESS_BLOCKED",
        "mode": mode,
        "generated_at": generated_at.isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": _iso_date_value(ANCHOR_DATE),
        "default_backtest_start": _iso_date_value(DEFAULT_BACKTEST_START),
        "timestamp_remediation_dir": str(timestamp_remediation_dir),
        "source_remediation_dir": str(source_remediation_dir),
        "dynamic_preparation_dir": str(dynamic_preparation_dir),
        "source_binding_dir": str(source_binding_dir),
        "simulation_policy_dir": str(simulation_policy_dir),
        "static_dry_run_dir": str(static_dry_run_dir),
        "dynamic_dry_run_readiness_cli": True,
        "dry_run_readiness_only": True,
        "simulation_executed": False,
        "data_quality_status": DATA_QUALITY_STATUS,
        "data_quality_gate_required": False,
        "data_quality_gate_executed": False,
        "aits_validate_data_executed": False,
        "data_quality_gate_rationale": (
            "aits validate-data not applicable because TRADING-2331 only reads "
            "prior artifacts and does not consume cached market data"
        ),
        "wrapper_record_count": len(wrapper_rows),
        "wrapper_validation_status": _string(wrapper_validation.get("validation_status")),
        "pit_caveat_acceptance_status": _string(
            pit_acceptance.get("acceptance_status")
        ),
        "gate_status": _string(gate_checklist.get("gate_status")),
        "readiness_status": _string(readiness.get("readiness_status")),
        "2332_allowed": bool(readiness.get("2332_allowed")),
        "next_task": _string(task_route.get("next_task")),
        "gate_blocker_count": len(_strings(gate_checklist.get("blockers"))),
        "gate_warning_count": len(_strings(gate_checklist.get("warnings"))),
        "wrapper_field_validation_matrix_generated": True,
        "timestamp_alignment_matrix_generated": True,
        "risk_cap_alignment_matrix_generated": True,
        "market_data_alignment_matrix_generated": True,
        "policy_compatibility_matrix_generated": True,
        "input_contract_generated": True,
        "data_quality_precheck_generated": True,
        "interpretation_boundary_generated": True,
        "2332_readiness_matrix_generated": True,
        "2332_task_route_generated": True,
        **_readiness_safety_subset(),
    }


def write_dynamic_dry_run_readiness_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    gate_checklist: Mapping[str, Any],
    pit_acceptance: Mapping[str, Any],
    wrapper_field_rows: Sequence[Mapping[str, Any]],
    timestamp_alignment_rows: Sequence[Mapping[str, Any]],
    risk_cap_alignment_rows: Sequence[Mapping[str, Any]],
    market_data_alignment_rows: Sequence[Mapping[str, Any]],
    policy_compatibility_rows: Sequence[Mapping[str, Any]],
    input_contract: Mapping[str, Any],
    data_quality_precheck: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, str]:
    output_payloads = [
        summary,
        gate_checklist,
        pit_acceptance,
        *wrapper_field_rows,
        *timestamp_alignment_rows,
        *risk_cap_alignment_rows,
        *market_data_alignment_rows,
        *policy_compatibility_rows,
        input_contract,
        data_quality_precheck,
        interpretation_boundary,
        readiness,
        task_route,
    ]
    for index, payload in enumerate(output_payloads):
        _validate_safe_output(f"TRADING-2331 output {index}", payload)

    paths: dict[str, Path] = {
        "summary": output_dir / "dynamic_dry_run_readiness_summary.json",
        "gate_checklist": output_dir / "dynamic_dry_run_gate_checklist.json",
        "pit_caveat": output_dir
        / "dynamic_dry_run_pit_caveat_acceptance_report.json",
        "wrapper_field_json": output_dir
        / "dynamic_dry_run_wrapper_field_validation_matrix.json",
        "wrapper_field_csv": output_dir
        / "dynamic_dry_run_wrapper_field_validation_matrix.csv",
        "timestamp_alignment_json": output_dir
        / "dynamic_dry_run_timestamp_alignment_matrix.json",
        "timestamp_alignment_csv": output_dir
        / "dynamic_dry_run_timestamp_alignment_matrix.csv",
        "risk_cap_alignment_json": output_dir
        / "dynamic_dry_run_risk_cap_alignment_matrix.json",
        "risk_cap_alignment_csv": output_dir
        / "dynamic_dry_run_risk_cap_alignment_matrix.csv",
        "market_data_alignment_json": output_dir
        / "dynamic_dry_run_market_data_alignment_matrix.json",
        "market_data_alignment_csv": output_dir
        / "dynamic_dry_run_market_data_alignment_matrix.csv",
        "policy_compatibility_json": output_dir
        / "dynamic_dry_run_policy_compatibility_matrix.json",
        "policy_compatibility_csv": output_dir
        / "dynamic_dry_run_policy_compatibility_matrix.csv",
        "input_contract": output_dir / "dynamic_dry_run_input_contract.json",
        "data_quality_precheck": output_dir
        / "dynamic_dry_run_data_quality_precheck.json",
        "interpretation_boundary": output_dir
        / "dynamic_dry_run_interpretation_boundary.json",
        "readiness": output_dir / "dynamic_dry_run_2332_readiness_matrix.json",
        "task_route": output_dir / "dynamic_dry_run_2332_task_route.json",
        "report_doc": docs_root
        / "dynamic_target_baseline_dry_run_readiness_with_pit_caveat.md",
        "pit_caveat_doc": docs_root
        / "dynamic_dry_run_pit_caveat_acceptance_report.md",
        "input_contract_doc": docs_root / "dynamic_dry_run_input_contract.md",
        "route_doc": docs_root / "dynamic_dry_run_2332_readiness_route.md",
    }

    write_json(paths["summary"], dict(summary))
    write_json(paths["gate_checklist"], dict(gate_checklist))
    write_json(paths["pit_caveat"], dict(pit_acceptance))
    write_json(paths["wrapper_field_json"], {**dict(summary), "rows": list(wrapper_field_rows)})
    write_csv_rows(paths["wrapper_field_csv"], wrapper_field_rows)
    write_json(
        paths["timestamp_alignment_json"],
        {**dict(summary), "rows": list(timestamp_alignment_rows)},
    )
    write_csv_rows(paths["timestamp_alignment_csv"], timestamp_alignment_rows)
    write_json(
        paths["risk_cap_alignment_json"],
        {**dict(summary), "rows": list(risk_cap_alignment_rows)},
    )
    write_csv_rows(paths["risk_cap_alignment_csv"], risk_cap_alignment_rows)
    write_json(
        paths["market_data_alignment_json"],
        {**dict(summary), "rows": list(market_data_alignment_rows)},
    )
    write_csv_rows(paths["market_data_alignment_csv"], market_data_alignment_rows)
    write_json(
        paths["policy_compatibility_json"],
        {**dict(summary), "rows": list(policy_compatibility_rows)},
    )
    write_csv_rows(paths["policy_compatibility_csv"], policy_compatibility_rows)
    write_json(paths["input_contract"], dict(input_contract))
    write_json(paths["data_quality_precheck"], dict(data_quality_precheck))
    write_json(paths["interpretation_boundary"], dict(interpretation_boundary))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))

    write_markdown(
        paths["report_doc"],
        _render_readiness_report(
            summary,
            gate_checklist,
            pit_acceptance,
            readiness,
            task_route,
        ),
    )
    write_markdown(paths["pit_caveat_doc"], _render_pit_caveat_doc(pit_acceptance))
    write_markdown(paths["input_contract_doc"], _render_input_contract_doc(input_contract))
    write_markdown(paths["route_doc"], _render_2332_route_doc(readiness, task_route))
    return {key: str(path) for key, path in paths.items()}


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicTargetBaselineDryRunReadinessError(
            f"{label} missing required artifacts: {missing}"
        )
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        payloads[key] = _load_json_object(path, f"{label} {key}")
    return payloads


def _load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DynamicTargetBaselineDryRunReadinessError(
            f"{label} artifact is not valid JSON: {path}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise DynamicTargetBaselineDryRunReadinessError(
            f"{label} artifact must be a JSON object: {path}"
        )
    return dict(payload)


def _validate_timestamp_remediation_inputs(payloads: Mapping[str, Any]) -> None:
    for key, payload in payloads.items():
        _validate_safe_input(f"TRADING-2330 {key}", payload)
    summary = mapping(payloads.get("summary"))
    readiness = mapping(payloads.get("readiness"))
    task_route = mapping(payloads.get("task_route"))
    wrapper_rows = _rows_from_payload(payloads.get("wrapper"))
    if str(summary.get("next_task")) != UPSTREAM_NEXT_TASK:
        raise DynamicTargetBaselineDryRunReadinessError(
            "TRADING-2331 requires TRADING-2330 summary route to dry-run readiness"
        )
    if str(task_route.get("next_task")) != UPSTREAM_NEXT_TASK:
        raise DynamicTargetBaselineDryRunReadinessError(
            "TRADING-2331 requires TRADING-2330 task route to dry-run readiness"
        )
    if summary.get("2331_allowed") is not True or readiness.get("2331_allowed") is not True:
        raise DynamicTargetBaselineDryRunReadinessError(
            "TRADING-2331 requires TRADING-2330 2331_allowed=true"
        )
    if not wrapper_rows:
        raise DynamicTargetBaselineDryRunReadinessError(
            "TRADING-2331 requires TRADING-2330 timestamp-remediated wrapper rows"
        )
    for index, row in enumerate(wrapper_rows):
        missing = [
            field for field in WRAPPER_IDENTITY_FIELDS if _is_missing(row.get(field))
        ]
        if missing:
            raise DynamicTargetBaselineDryRunReadinessError(
                f"TRADING-2330 wrapper row {index} missing identity fields: {missing}"
            )
        _validate_safe_input(f"TRADING-2330 wrapper row {index}", row)
    if not mapping(payloads.get("safety_boundary")):
        raise DynamicTargetBaselineDryRunReadinessError(
            "TRADING-2331 requires TRADING-2330 safety boundary"
        )


def _validate_safe_input(name: str, payload: Mapping[str, Any]) -> None:
    try:
        validate_no_unsafe_fields(name, payload)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetBaselineDryRunReadinessError(str(exc)) from exc


def _validate_safe_output(name: str, payload: Mapping[str, Any]) -> None:
    try:
        validate_no_unsafe_fields(name, payload)
    except DynamicTargetBaselinePreparationError as exc:
        raise DynamicTargetBaselineDryRunReadinessError(str(exc)) from exc


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in records(mapping(payload).get("rows"))]


def _selected_source_identity(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    first = rows[0]
    return {
        "baseline_id": _string(first.get("baseline_id")),
        "source_id": _string(first.get("source_id")),
        "source_family": _string(first.get("source_family")),
        "source_path": _string(first.get("source_path")),
        "source_hash": _string(first.get("source_hash")),
        "source_artifact_hash": _string(first.get("source_artifact_hash")),
        "baseline_schema_version": _string(first.get("baseline_schema_version")),
    }


def _field_group(field: str) -> str:
    if field in WRAPPER_IDENTITY_FIELDS:
        return "identity"
    if field in WRAPPER_BASELINE_FIELDS:
        return "baseline"
    if field in WRAPPER_TIMESTAMP_FIELDS:
        return "timestamp"
    if field in WRAPPER_POLICY_FIELDS:
        return "policy"
    return "safety"


def _field_invalid(row: Mapping[str, Any], field: str) -> bool:
    value = row.get(field)
    if _field_missing(row, field):
        return False
    if field == "date":
        return _parse_date(value) is None
    if field in WRAPPER_TIMESTAMP_FIELDS:
        return _parse_datetime(value) is None
    if field in {"target_exposure", "risk_asset_exposure", "asset_weight", "cash_weight"}:
        try:
            float(value)
        except (TypeError, ValueError):
            return True
        return False
    if field in {"promotion_allowed", "paper_shadow_allowed", "production_allowed"}:
        return value is not False
    if field == "broker_action":
        return str(value).lower() != "none"
    if field == "known_at_policy":
        return str(value) != EXPECTED_KNOWN_AT_POLICY
    if field == "latency_policy":
        return str(value) != EXPECTED_LATENCY_POLICY
    if field == "rebalance_policy":
        return str(value) != EXPECTED_REBALANCE_POLICY
    return False


def _field_missing(row: Mapping[str, Any], field: str) -> bool:
    value = row.get(field)
    if field == "broker_action":
        return value is None or str(value).strip() == ""
    return _is_missing(value)


def _decision_after_as_of(row: Mapping[str, Any]) -> bool:
    as_of = _parse_datetime(row.get("as_of_timestamp"))
    decision = _parse_datetime(row.get("decision_timestamp"))
    return bool(as_of and decision and decision >= as_of)


def _valid_from_after_decision(row: Mapping[str, Any]) -> bool:
    decision = _parse_datetime(row.get("decision_timestamp"))
    valid_from = _parse_datetime(row.get("valid_from"))
    return bool(decision and valid_from and valid_from >= decision)


def _validity_window_ordered(row: Mapping[str, Any]) -> bool:
    valid_from = _parse_datetime(row.get("valid_from"))
    valid_until = _parse_datetime(row.get("valid_until"))
    return bool(valid_from and valid_until and valid_until >= valid_from)


def _rebalance_after_decision(row: Mapping[str, Any]) -> bool:
    decision = _parse_datetime(row.get("decision_timestamp"))
    rebalance = _parse_datetime(row.get("rebalance_timestamp"))
    return bool(decision and rebalance and rebalance >= decision)


def _policy_row(
    policy_id: str,
    observed: Any,
    expected: str,
    status: str,
    caveat: str,
) -> dict[str, Any]:
    blocking = status == "FAIL"
    return {
        "policy_id": policy_id,
        "observed_policy": _string(observed),
        "expected_policy": expected,
        "compatibility_status": status,
        "blocking": blocking,
        "caveat": caveat,
        **_safety_subset(),
    }


def _gate_row(
    check_id: str,
    passed: bool,
    evidence: str,
    caveat: str,
    *,
    warning: bool = False,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "check_status": "PASS_WITH_WARNINGS"
        if passed and warning
        else "PASS"
        if passed
        else "FAIL",
        "blocking": not passed,
        "warning": warning if passed else False,
        "evidence": evidence,
        "caveat": caveat,
        **_safety_subset(),
    }


def _matrix_gate_row(
    check_id: str,
    rows: Sequence[Mapping[str, Any]],
    caveat: str,
    *,
    status_key: str = "alignment_status",
) -> dict[str, Any]:
    blocking = _has_blocking_row(rows)
    warning = _has_warning_row(rows, status_key)
    return {
        "check_id": check_id,
        "check_status": "FAIL"
        if blocking
        else "PASS_WITH_WARNINGS"
        if warning
        else "PASS",
        "blocking": blocking,
        "warning": warning and not blocking,
        "evidence": _overall_matrix_status(rows, status_key),
        "caveat": caveat,
        **_safety_subset(),
    }


def _has_blocking_row(rows: Sequence[Mapping[str, Any]]) -> bool:
    return any(row.get("blocking") is True for row in rows)


def _has_warning_row(rows: Sequence[Mapping[str, Any]], status_key: str) -> bool:
    return any("WARNING" in str(row.get(status_key)) for row in rows)


def _overall_matrix_status(rows: Sequence[Mapping[str, Any]], status_key: str) -> str:
    if not rows:
        return "FAIL"
    statuses = {str(row.get(status_key)) for row in rows}
    if any(status == "FAIL" or "BLOCKED" in status for status in statuses):
        return "FAIL"
    if any("WARNING" in status for status in statuses):
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _blocked_next_task(blockers: Sequence[str]) -> str:
    if any("wrapper" in blocker for blocker in blockers):
        return NEXT_WRAPPER_REMEDIATION_TASK
    if any("pit" in blocker for blocker in blockers):
        return NEXT_PIT_CAVEAT_REMEDIATION_TASK
    if any("policy" in blocker for blocker in blockers):
        return NEXT_POLICY_REMEDIATION_TASK
    return NEXT_ALIGNMENT_REMEDIATION_TASK


def _common_value(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    values = {
        str(row.get(field))
        for row in rows
        if not _is_missing(row.get(field))
    }
    if len(values) == 1:
        return next(iter(values))
    return ",".join(sorted(values))


def _wrapper_date_range(rows: Sequence[Mapping[str, Any]]) -> tuple[date | None, date | None]:
    dates = [_parse_date(row.get("date")) for row in rows]
    valid_dates = [value for value in dates if value is not None]
    if not valid_dates:
        return None, None
    return min(valid_dates), max(valid_dates)


def _date_overlap(
    left_start: date | None,
    left_end: date | None,
    right_start: date | None,
    right_end: date | None,
) -> tuple[date | None, date | None]:
    if not (left_start and left_end and right_start and right_end):
        return None, None
    overlap_start = max(left_start, right_start)
    overlap_end = min(left_end, right_end)
    if overlap_start > overlap_end:
        return None, None
    return overlap_start, overlap_end


def _parse_datetime(value: Any) -> datetime | None:
    text = _string(value)
    if not text:
        return None
    if len(text) == 10:
        parsed_date = _parse_date(text)
        if parsed_date is None:
            return None
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _parse_date(value: Any) -> date | None:
    text = _string(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _date_string(value: date | None) -> str:
    return value.isoformat() if value else ""


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round_float(numerator / denominator)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in {"none", "null", "nan"}
    return False


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _iso_date_value(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return _string(value)


def _safety_subset() -> dict[str, Any]:
    return {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _readiness_safety_subset() -> dict[str, Any]:
    return {
        "research_only": True,
        "manual_review_only": True,
        "portfolio_effect": "none",
        "production_effect": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_readiness_report(
    summary: Mapping[str, Any],
    gate_checklist: Mapping[str, Any],
    pit_acceptance: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Target Baseline Dry-Run Readiness With PIT Caveat",
        "",
        f"- market_regime: `{summary.get('market_regime')}`",
        "- actual_requested_date_range: `prior research outputs only`",
        f"- wrapper_record_count: `{summary.get('wrapper_record_count')}`",
        f"- wrapper_validation_status: `{summary.get('wrapper_validation_status')}`",
        f"- PIT caveat: `{pit_acceptance.get('acceptance_status')}`",
        f"- gate_status: `{gate_checklist.get('gate_status')}`",
        f"- readiness_status: `{readiness.get('readiness_status')}`",
        f"- 2332_allowed: `{readiness.get('2332_allowed')}`",
        f"- next_task: `{task_route.get('next_task')}`",
        "",
        (
            "TRADING-2331 只做 2332 前置 readiness 检查，不执行 dynamic dry-run，"
            "不读取 cached market data，不生成交易指令。"
        ),
        "",
        "## Gate Checklist",
        "",
    ]
    for row in records(gate_checklist.get("rows")):
        lines.append(
            f"- `{row.get('check_id')}` status=`{row.get('check_status')}` "
            f"blocking=`{row.get('blocking')}`"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            (
                "`target_exposure` 仅为 research baseline field；PIT approximation、"
                "market-data gate 和 alignment caveat 必须由 TRADING-2332 carry forward。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _render_pit_caveat_doc(pit_acceptance: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Dry-Run PIT Caveat Acceptance Report",
        "",
        f"- acceptance_status: `{pit_acceptance.get('acceptance_status')}`",
        f"- strict_pit_ready: `{pit_acceptance.get('strict_pit_ready')}`",
        f"- pit_approximation_ready: `{pit_acceptance.get('pit_approximation_ready')}`",
        f"- allowed_usage: `{', '.join(_strings(pit_acceptance.get('allowed_usage')))}`",
        f"- blocked_usage: `{', '.join(_strings(pit_acceptance.get('blocked_usage')))}`",
        "",
        "## Carry-Forward Caveats",
        "",
    ]
    for caveat in _strings(pit_acceptance.get("carry_forward_caveats")):
        lines.append(f"- {caveat}")
    return "\n".join(lines) + "\n"


def _render_input_contract_doc(input_contract: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic Dry-Run Input Contract",
        "",
        f"- contract_status: `{input_contract.get('contract_status')}`",
        f"- baseline_id: `{input_contract.get('baseline_id')}`",
        f"- source_id: `{input_contract.get('source_id')}`",
        f"- wrapper_record_count: `{input_contract.get('wrapper_record_count')}`",
        (
            f"- wrapper_date_range: `{input_contract.get('wrapper_date_start')}.."
            f"{input_contract.get('wrapper_date_end')}`"
        ),
        "",
        (
            "TRADING-2332 必须消费该 contract，并在读取 cached market data 前执行 "
            "`aits validate-data` 或同源 data-quality gate。"
        ),
    ]
    return "\n".join(lines) + "\n"


def _render_2332_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Dry-Run 2332 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- 2332_allowed: `{readiness.get('2332_allowed')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            f"- caveat: `{task_route.get('caveat')}`",
            "",
            (
                "该 route 只允许进入 source-bound dynamic target baseline dry-run 准备；"
                "promotion、paper-shadow、production 和 broker action 保持关闭。"
            ),
        ]
    ) + "\n"
