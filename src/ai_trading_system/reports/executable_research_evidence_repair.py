from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import executable_research_binding as binding_reports
from ai_trading_system.reports import next_research_cycle as next_cycle

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
MARKET_REGIME = "ai_after_chatgpt"
AI_REGIME_START = "2022-12-01"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"

EVIDENCE_GAP_LEDGER_REPORT_TYPE = "executable_research_evidence_gap_ledger"
BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE = (
    "backfill_partial_root_cause_repair_plan"
)
VALIDATION_SUFFIX = "_validation"
EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE = (
    f"{EVIDENCE_GAP_LEDGER_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE = (
    f"{BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
LEDGER_READY_STATUS = "EXECUTABLE_RESEARCH_EVIDENCE_GAP_LEDGER_READY"
BACKFILL_REPAIRABLE = "BACKFILL_REPAIRABLE"
BACKFILL_PARTIALLY_REPAIRABLE = "BACKFILL_PARTIALLY_REPAIRABLE"
BACKFILL_NOT_REPAIRABLE_WITH_CURRENT_SPEC = (
    "BACKFILL_NOT_REPAIRABLE_WITH_CURRENT_SPEC"
)
BACKFILL_REPAIR_STATUSES: tuple[str, ...] = (
    BACKFILL_REPAIRABLE,
    BACKFILL_PARTIALLY_REPAIRABLE,
    BACKFILL_NOT_REPAIRABLE_WITH_CURRENT_SPEC,
)

REPORT_PREFIXES: dict[str, str] = {
    EVIDENCE_GAP_LEDGER_REPORT_TYPE: "executable_research_evidence_gap_ledger",
    EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE: (
        "executable_research_evidence_gap_ledger_validation"
    ),
    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: (
        "backfill_partial_root_cause_repair_plan"
    ),
    BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE: (
        "backfill_partial_root_cause_repair_plan_validation"
    ),
}

REQUIRED_SOURCE_REPORT_TYPES: tuple[str, ...] = (
    binding_reports.CONTRACT_REPORT_TYPE,
    binding_reports.SIGNAL_BINDING_REPORT_TYPE,
    binding_reports.WEIGHT_BINDING_REPORT_TYPE,
    binding_reports.SAFETY_AUDIT_REPORT_TYPE,
    next_cycle.BACKFILL_REPORT_TYPE,
    next_cycle.STRESS_REVIEW_REPORT_TYPE,
    next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
    next_cycle.VS_RETURNED_REPORT_TYPE,
    next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
    next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
    next_cycle.RESEARCH_GATE_REPORT_TYPE,
    next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
)

REQUIRED_GAP_CATEGORIES: tuple[str, ...] = (
    "backfill_coverage_gap",
    "signal_robustness_gap",
    "window_fragility_gap",
    "stress_failure_gap",
    "cost_benchmark_weakness_gap",
    "comparison_weakness_gap",
)

REQUIRED_BACKFILL_WINDOWS: tuple[str, ...] = (
    "normal_market_regime",
    "rapid_drawdown",
    "slow_drawdown",
    "high_volatility_sideways_market",
    "ai_semiconductor_correction",
    "false_risk_off_cluster",
)

REPAIRABILITY_TYPES: tuple[str, ...] = (
    "data_repairable",
    "binding_repairable",
    "candidate_spec_issue",
    "expected_limitation",
)


def default_evidence_repair_json_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.json"


def default_evidence_repair_markdown_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.md"


def latest_evidence_repair_json_path(report_type: str, output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, f"{REPORT_PREFIXES[report_type]}_", ".json")


def build_executable_research_evidence_gap_ledger_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    sources = _load_required_sources(as_of=as_of, reports_dir=reports_dir)
    payloads = {report_type: payload for report_type, _, payload in sources}
    source_artifacts = [
        _source_artifact(report_type, source_path, payload)
        for report_type, source_path, payload in sources
    ]
    gaps = _build_gap_rows(payloads)
    category_summary = _gap_category_summary(gaps)
    snapshot_summary = _mapping(
        payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE].get("summary")
    )
    gate_summary = _mapping(payloads[next_cycle.RESEARCH_GATE_REPORT_TYPE].get("summary"))
    requested_date_range = _text(
        snapshot_summary.get("requested_date_range"),
        _text(
            _mapping(payloads[next_cycle.BACKFILL_REPORT_TYPE].get("summary")).get(
                "requested_date_range"
            ),
            f"{next_cycle.AI_REGIME_START}..unspecified",
        ),
    )
    blocking_gap_count = len([row for row in gaps if row["blocking"] is True])
    redesign_gap_count = len(
        [row for row in gaps if row["requires_candidate_redesign"] is True]
    )
    summary = {
        "ledger_status": LEDGER_READY_STATUS,
        "source_cycle_snapshot_status": _text(
            payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE].get("status")
        ),
        "source_research_gate_decision": _text(
            gate_summary.get("research_gate_decision"),
            _text(payloads[next_cycle.RESEARCH_GATE_REPORT_TYPE].get("status")),
        ),
        "candidate_id": _text(snapshot_summary.get("candidate_id"), "MISSING"),
        "market_regime": MARKET_REGIME,
        "requested_date_range": requested_date_range,
        "source_artifact_count": len(source_artifacts),
        "gap_count": len(gaps),
        "blocking_gap_count": blocking_gap_count,
        "candidate_redesign_gap_count": redesign_gap_count,
        "gap_categories": list(category_summary),
        "backfill_status": _text(
            _mapping(payloads[next_cycle.BACKFILL_REPORT_TYPE].get("summary")).get(
                "candidate_backfill_status"
            ),
            _text(payloads[next_cycle.BACKFILL_REPORT_TYPE].get("status")),
        ),
        "stress_result": _text(payloads[next_cycle.STRESS_REVIEW_REPORT_TYPE].get("status")),
        "cost_benchmark_status": _text(
            payloads[next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE].get("status")
        ),
        "vs_returned_status": _text(
            payloads[next_cycle.VS_RETURNED_REPORT_TYPE].get("status")
        ),
        "signal_robustness_status": _text(
            payloads[next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE].get("status")
        ),
        "window_sensitivity_status": _text(
            payloads[next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE].get("status")
        ),
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        as_of=as_of,
        status=LEDGER_READY_STATUS,
        purpose=(
            "Record non-aggregated evidence gaps explaining why the executable "
            "research cycle remains NEEDS_MORE_EVIDENCE."
        ),
        input_artifacts={
            row["report_type"]: row["artifact_path"] for row in source_artifacts
        },
        output_decision=LEDGER_READY_STATUS,
        summary=summary,
        body={
            "source_artifacts": source_artifacts,
            "evidence_gaps": gaps,
            "gap_category_summary": category_summary,
            "classification_policy": {
                "required_gap_categories": list(REQUIRED_GAP_CATEGORIES),
                "blocking_gap_meaning": (
                    "Gap directly supports the current NEEDS_MORE_EVIDENCE gate "
                    "or prevents a research-promising interpretation."
                ),
                "candidate_redesign_meaning": (
                    "Gap may require changed candidate logic rather than only "
                    "data or binding repair."
                ),
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-471 已生成 executable research evidence gap ledger；"
                "当前候选仍需要补证据，不能进入 paper-shadow 或 production。"
            ),
            key_result=LEDGER_READY_STATUS,
            blocking_issues=(
                f"blocking_gaps={blocking_gap_count}; "
                f"candidate_redesign_gaps={redesign_gap_count}"
            ),
            warnings=(
                "partial backfill, weak stress/cost/benchmark, blocked signal "
                "robustness, fragile windows"
            ),
            next_action="run_trading_472_backfill_repair_plan",
        ),
        next_action="run_trading_472_backfill_repair_plan",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Ledger is diagnostic only and reads existing TRADING-470 artifacts.",
            "Ledger does not repair data, rerun backfill, or tune thresholds.",
            "Ledger cannot activate paper-shadow or write official target weights.",
        ],
        requested_date_range=requested_date_range,
    )


def validate_executable_research_evidence_gap_ledger_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    gaps = _records(payload.get("evidence_gaps"))
    source_artifacts = _records(payload.get("source_artifacts"))
    category_summary = _records(payload.get("gap_category_summary"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}
    gap_categories = {_text(row.get("gap_category")) for row in gaps}
    gap_ids = [_text(row.get("gap_id")) for row in gaps]

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        f"report_type must be {EVIDENCE_GAP_LEDGER_REPORT_TYPE}.",
        "regenerate_executable_research_evidence_gap_ledger",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Ledger must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "market_regime_visible",
        _text(payload.get("market_regime")) == MARKET_REGIME
        and _text(summary.get("market_regime")) == MARKET_REGIME,
        f"Ledger must disclose market_regime={MARKET_REGIME}.",
        "restore_ai_after_chatgpt_regime_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "requested_date_range_visible",
        bool(_text(payload.get("requested_date_range"))),
        "Ledger must disclose requested date range.",
        "restore_requested_date_range_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        set(REQUIRED_SOURCE_REPORT_TYPES) <= source_report_types,
        "Ledger must include every required TRADING-470 source report.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_gap_categories_present",
        set(REQUIRED_GAP_CATEGORIES) <= gap_categories,
        "Ledger must include every required evidence gap category.",
        "restore_gap_classification_rows",
    )
    _append_check(
        checks,
        blocking_issues,
        "gap_rows_present",
        bool(gaps),
        "Ledger must include non-aggregated gap rows.",
        "restore_evidence_gap_rows",
    )
    _append_check(
        checks,
        blocking_issues,
        "gap_ids_unique",
        len(gap_ids) == len(set(gap_ids)),
        "Each evidence gap must have a unique gap_id.",
        "deduplicate_gap_ids",
    )
    _append_check(
        checks,
        blocking_issues,
        "gap_required_fields_present",
        all(_gap_row_complete(row) for row in gaps),
        "Each gap row must include source/current/expected/root/fix/blocking/redesign fields.",
        "restore_gap_required_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocking_gaps_visible",
        _int(summary.get("blocking_gap_count")) == len(
            [row for row in gaps if row.get("blocking") is True]
        )
        and _int(summary.get("blocking_gap_count")) > 0,
        "Blocking gap count must match the ledger rows and be non-zero.",
        "restore_blocking_gap_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "redesign_gaps_visible",
        _int(summary.get("candidate_redesign_gap_count")) == len(
            [row for row in gaps if row.get("requires_candidate_redesign") is True]
        ),
        "Candidate redesign gap count must match the ledger rows.",
        "restore_candidate_redesign_gap_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "category_summary_consistent",
        _category_summary_consistent(gaps, category_summary),
        "Category summary counts must match the evidence gap rows.",
        "restore_category_summary_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Ledger must include Reader Brief fields.",
        "restore_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_locked",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/weights/broker/order/production mutation.",
        "restore_evidence_repair_safety_boundary",
    )
    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return _payload(
        report_type=EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-471 evidence gap ledger schema and safety boundary.",
        input_artifacts={EVIDENCE_GAP_LEDGER_REPORT_TYPE: _artifact_id(payload)},
        output_decision=status,
        summary={
            "validation_status": status,
            "source_report_type": report_type,
            "check_count": len(checks),
            "failed_check_count": len(blocking_issues),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking_issues,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"TRADING-471 evidence gap ledger validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_executable_research_evidence_gap_ledger"
                if status == FAIL_STATUS
                else "use_validated_gap_ledger_for_trading_472"
            ),
        ),
        next_action=(
            "repair_executable_research_evidence_gap_ledger"
            if status == FAIL_STATUS
            else "use_validated_gap_ledger_for_trading_472"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_backfill_partial_root_cause_repair_plan_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    backfill_path = next_cycle.default_next_research_cycle_json_path(
        next_cycle.BACKFILL_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    ledger_path = default_evidence_repair_json_path(
        EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    backfill_payload = _read_json_mapping(backfill_path)
    ledger_payload = _read_json_mapping(ledger_path)
    backfill_summary = _mapping(backfill_payload.get("summary"))
    window_diagnostics = _backfill_window_diagnostics(backfill_payload)
    repair_issue_summary = _repair_issue_summary(
        backfill_payload=backfill_payload,
        window_diagnostics=window_diagnostics,
    )
    repair_status = _overall_backfill_repair_status(repair_issue_summary)
    incomplete_windows = [
        row
        for row in window_diagnostics
        if _text(row.get("source_window_status")) != "READY"
    ]
    requested_date_range = _text(
        backfill_payload.get("requested_date_range"),
        _text(backfill_summary.get("requested_date_range"), "not_applicable"),
    )
    binding_repairable_count = len(
        [
            row
            for row in window_diagnostics
            if "binding_repairable" in _list_values(row.get("repairability"))
        ]
    )
    summary = {
        "repair_plan_status": repair_status,
        "source_backfill_status": _text(
            backfill_summary.get("candidate_backfill_status"),
            _text(backfill_payload.get("status"), "MISSING"),
        ),
        "source_backfill_artifact_id": _artifact_id(backfill_payload),
        "source_evidence_gap_ledger_artifact_id": _artifact_id(ledger_payload),
        "candidate_id": _text(backfill_summary.get("candidate_id"), "MISSING"),
        "market_regime": _text(backfill_summary.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_date_range,
        "required_window_count": len(REQUIRED_BACKFILL_WINDOWS),
        "source_window_count": len(_records(backfill_payload.get("backfill_windows"))),
        "incomplete_window_count": len(incomplete_windows),
        "data_repairable_window_count": _window_repairability_count(
            window_diagnostics,
            "data_repairable",
        ),
        "binding_repairable_window_count": binding_repairable_count,
        "candidate_spec_issue_window_count": _window_repairability_count(
            window_diagnostics,
            "candidate_spec_issue",
        ),
        "expected_limitation_window_count": _window_repairability_count(
            window_diagnostics,
            "expected_limitation",
        ),
        "missing_date_policy": "do_not_fabricate_unreported_dates",
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
        as_of=as_of,
        status=repair_status,
        purpose=(
            "Explain why the latest next-candidate backfill is partial and "
            "classify whether each incomplete window can be repaired."
        ),
        input_artifacts={
            next_cycle.BACKFILL_REPORT_TYPE: str(backfill_path),
            EVIDENCE_GAP_LEDGER_REPORT_TYPE: str(ledger_path),
        },
        output_decision=repair_status,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    next_cycle.BACKFILL_REPORT_TYPE,
                    backfill_path,
                    backfill_payload,
                ),
                _source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    ledger_path,
                    ledger_payload,
                ),
            ],
            "window_repair_diagnostics": window_diagnostics,
            "repair_issue_summary": repair_issue_summary,
            "classification_policy": {
                "required_windows": list(REQUIRED_BACKFILL_WINDOWS),
                "repairability_types": list(REPAIRABILITY_TYPES),
                "missing_date_policy": (
                    "If the source backfill reports only a window-level missing "
                    "historical signal series, the repair plan records "
                    "missing_dates=[] with missing_dates_status="
                    "not_enumerated_in_source_artifact instead of fabricating "
                    "daily gaps."
                ),
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-472 已生成 backfill partial root-cause repair plan；"
                "partial backfill 来自 historical signal/weight binding 不完整，"
                "不能据此宣称完整动态策略 backfill。"
            ),
            key_result=repair_status,
            blocking_issues=(
                f"incomplete_windows={len(incomplete_windows)}; "
                f"binding_repairable_windows={binding_repairable_count}"
            ),
            warnings=_issue_names(repair_issue_summary, "issue_id"),
            next_action="run_trading_473_signal_robustness_blocker_drilldown",
        ),
        next_action="run_trading_473_signal_robustness_blocker_drilldown",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Repair plan is read-only and does not rerun backfill.",
            "Missing daily signal dates are not fabricated when the source "
            "artifact only reports a window-level missing historical series.",
            "Repairability classification does not weaken signal completeness "
            "or promote paper-shadow eligibility.",
        ],
        requested_date_range=requested_date_range,
    )


def validate_backfill_partial_root_cause_repair_plan_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    diagnostics = _records(payload.get("window_repair_diagnostics"))
    issue_summary = _records(payload.get("repair_issue_summary"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}
    window_ids = {_text(row.get("window_id")) for row in diagnostics}

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
        f"report_type must be {BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE}.",
        "regenerate_backfill_partial_repair_plan",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        _text(payload.get("status")) in BACKFILL_REPAIR_STATUSES,
        "Repair plan status must use the governed TRADING-472 taxonomy.",
        "restore_backfill_repair_status_taxonomy",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Repair plan must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "market_regime_visible",
        _text(payload.get("market_regime")) == MARKET_REGIME
        and _text(summary.get("market_regime")) == MARKET_REGIME,
        f"Repair plan must disclose market_regime={MARKET_REGIME}.",
        "restore_ai_after_chatgpt_regime_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "requested_date_range_visible",
        bool(_text(payload.get("requested_date_range"))),
        "Repair plan must disclose requested date range.",
        "restore_requested_date_range_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            next_cycle.BACKFILL_REPORT_TYPE,
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        }
        <= source_report_types,
        "Repair plan must include source backfill and TRADING-471 ledger.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_windows_present",
        set(REQUIRED_BACKFILL_WINDOWS) <= window_ids,
        "Repair plan must include all required backfill windows.",
        "restore_required_window_diagnostics",
    )
    _append_check(
        checks,
        blocking_issues,
        "incomplete_windows_visible",
        _int(summary.get("incomplete_window_count"))
        == len(
            [
                row
                for row in diagnostics
                if _text(row.get("source_window_status")) != "READY"
            ]
        )
        and _int(summary.get("incomplete_window_count")) > 0,
        "Incomplete window count must match window diagnostics and be non-zero.",
        "restore_incomplete_window_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "window_diagnostic_fields_present",
        all(_backfill_window_diagnostic_complete(row) for row in diagnostics),
        "Each window diagnostic must include missing date/feature/signal/schema/"
        "coverage/binding and repairability fields.",
        "restore_window_diagnostic_required_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "repairability_values_valid",
        all(
            set(_list_values(row.get("repairability"))) <= set(REPAIRABILITY_TYPES)
            for row in diagnostics
        ),
        "Each repairability value must use the governed TRADING-472 taxonomy.",
        "restore_repairability_taxonomy",
    )
    _append_check(
        checks,
        blocking_issues,
        "issue_summary_present",
        bool(issue_summary),
        "Repair plan must include repair issue summary rows.",
        "restore_repair_issue_summary",
    )
    _append_check(
        checks,
        blocking_issues,
        "missing_dates_policy_visible",
        _text(summary.get("missing_date_policy"))
        == "do_not_fabricate_unreported_dates"
        and all(bool(_text(row.get("missing_dates_status"))) for row in diagnostics),
        "Repair plan must expose missing-date policy and status per window.",
        "restore_missing_date_policy",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Repair plan must include Reader Brief fields.",
        "restore_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_locked",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/weights/broker/order/production mutation.",
        "restore_evidence_repair_safety_boundary",
    )
    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return _payload(
        report_type=BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-472 backfill partial root-cause repair plan.",
        input_artifacts={BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: _artifact_id(payload)},
        output_decision=status,
        summary={
            "validation_status": status,
            "source_report_type": report_type,
            "check_count": len(checks),
            "failed_check_count": len(blocking_issues),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking_issues,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"TRADING-472 backfill repair plan validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_backfill_partial_root_cause_repair_plan"
                if status == FAIL_STATUS
                else "use_validated_backfill_repair_plan_for_trading_473"
            ),
        ),
        next_action=(
            "repair_backfill_partial_root_cause_repair_plan"
            if status == FAIL_STATUS
            else "use_validated_backfill_repair_plan_for_trading_473"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def write_evidence_repair_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_evidence_repair_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_evidence_repair_markdown(payload), encoding="utf-8")
    return output_path


def render_evidence_repair_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_title(_text(payload.get('report_type')))} {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- status: {_md_cell(payload.get('status'))}",
        f"- output_decision: {_md_cell(payload.get('output_decision'))}",
        f"- market_regime: {_md_cell(payload.get('market_regime'))}",
        f"- requested_date_range: {_md_cell(payload.get('requested_date_range'))}",
        f"- production_effect: {_md_cell(payload.get('production_effect'))}",
        f"- next_action: {_md_cell(payload.get('next_action'))}",
    ]
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            lines.append(f"- {key}: {_md_cell(value)}")
    lines.extend(["", "## Reader Brief", ""])
    for key, value in _mapping(payload.get("reader_brief")).items():
        lines.append(f"- {key}: {_md_cell(value)}")
    for title, key in _markdown_tables(_text(payload.get("report_type"))):
        lines.extend(_table_records(title, payload.get(key)))
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def _payload(
    *,
    report_type: str,
    as_of: date,
    status: str,
    purpose: str,
    input_artifacts: Mapping[str, Any],
    output_decision: str,
    summary: Mapping[str, Any],
    body: Mapping[str, Any],
    reader_brief: Mapping[str, Any],
    next_action: str,
    safety_boundary: Mapping[str, Any],
    limitations: Sequence[str],
    requested_date_range: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "market_regime": MARKET_REGIME,
        "ai_regime_start": AI_REGIME_START,
        "requested_date_range": requested_date_range,
        "purpose": purpose,
        "input_artifacts": dict(input_artifacts),
        "output_decision": output_decision,
        "summary": dict(summary),
        **dict(body),
        "reader_brief": dict(reader_brief),
        "safety_boundary": dict(safety_boundary),
        "limitations": list(limitations),
        "next_action": next_action,
        "methodology": {
            "collector_mode": "read_existing_trading_470_artifacts",
            "does_not_refresh_data": True,
            "does_not_run_backfill": True,
            "does_not_fabricate_data": True,
            "does_not_tune_thresholds": True,
            "does_not_create_paper_shadow_candidate": True,
            "does_not_approve_extended_shadow": True,
            "does_not_approve_live_trading": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_append_owner_decision": True,
            "does_not_mutate_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def _load_required_sources(
    *,
    as_of: date,
    reports_dir: Path,
) -> list[tuple[str, Path, dict[str, Any]]]:
    rows: list[tuple[str, Path, dict[str, Any]]] = []
    for report_type in REQUIRED_SOURCE_REPORT_TYPES:
        if report_type in binding_reports.REPORT_PREFIXES:
            path = binding_reports.default_executable_binding_json_path(
                report_type,
                reports_dir,
                as_of,
            )
        else:
            path = next_cycle.default_next_research_cycle_json_path(
                report_type,
                reports_dir,
                as_of,
            )
        rows.append((report_type, path, _read_json_mapping(path)))
    return rows


def _source_artifact(
    report_type: str,
    source_path: Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    return {
        "report_type": report_type,
        "artifact_id": _artifact_id(payload),
        "artifact_path": str(source_path),
        "status": _text(payload.get("status"), _text(summary.get("status"), "MISSING")),
        "next_action": _text(payload.get("next_action")),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
    }


def _build_gap_rows(payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    gaps.extend(_backfill_gap_rows(payloads[next_cycle.BACKFILL_REPORT_TYPE]))
    gaps.extend(_signal_gap_rows(payloads[next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE]))
    gaps.extend(_window_gap_rows(payloads[next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE]))
    gaps.extend(_stress_gap_rows(payloads[next_cycle.STRESS_REVIEW_REPORT_TYPE]))
    gaps.extend(
        _cost_benchmark_gap_rows(payloads[next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE])
    )
    gaps.extend(_comparison_gap_rows(payloads[next_cycle.VS_RETURNED_REPORT_TYPE]))
    return gaps


def _backfill_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for window in _records(payload.get("backfill_windows")):
        if _text(window.get("backfill_window_status")) == "COMPLETE":
            continue
        window_id = _text(window.get("window_id"), "unknown_window")
        missing = _list_values(window.get("missing_data_list"))
        rows.append(
            _gap(
                gap_id=f"backfill_coverage_{window_id}",
                gap_category="backfill_coverage_gap",
                source_report=next_cycle.BACKFILL_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"window_status={_text(window.get('backfill_window_status'))}; "
                    f"signal_completeness={_text(window.get('signal_completeness'))}; "
                    f"missing={';'.join(missing) if missing else 'none'}"
                ),
                expected_value=(
                    "window_status=COMPLETE with historical executable signal "
                    f"series and dynamic binding metrics for {window_id}"
                ),
                root_cause_category="missing_historical_signal_series",
                fix_type="repair_historical_signal_series_or_dynamic_binding",
                blocking=True,
                requires_candidate_redesign=False,
                evidence_reference={
                    "window_id": window_id,
                    "start": _text(window.get("start")),
                    "end": _text(window.get("end")),
                    "return_proxy": window.get("return_proxy"),
                    "drawdown_proxy": window.get("drawdown_proxy"),
                    "turnover": window.get("turnover"),
                },
            )
        )
    return rows


def _signal_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for check in _records(payload.get("signal_quality_checks")):
        if _text(check.get("status")) != "BLOCKING":
            continue
        check_id = _text(check.get("check_id"), "unknown_check")
        rows.append(
            _gap(
                gap_id=f"signal_robustness_{check_id}",
                gap_category="signal_robustness_gap",
                source_report=next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=f"{check_id}=BLOCKING; evidence={_text(check.get('evidence'))}",
                expected_value=(
                    f"{check_id}=PASS and overall signal robustness not blocked "
                    "without relaxing signal completeness rules"
                ),
                root_cause_category=_signal_root_cause(check_id),
                fix_type="repair_signal_binding_inputs",
                blocking=True,
                requires_candidate_redesign=False,
                evidence_reference={
                    "check_id": check_id,
                    "recommended_action": _text(check.get("recommended_action")),
                    "fail_closed": check.get("fail_closed") is True,
                    "signal_completeness_rules_relaxed": (
                        check.get("signal_completeness_rules_relaxed") is True
                    ),
                },
            )
        )
    return rows


def _window_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocking_ids = {
        _text(row.get("issue_id")) for row in _records(payload.get("blocking_issues"))
    }
    for split in _records(payload.get("window_splits")):
        split_status = _text(split.get("status"))
        if split_status in {"STABLE", "PASS"}:
            continue
        split_id = _text(split.get("window_split_id"), "unknown_split")
        weak = split_status == "WEAK"
        partial = split_status == "PARTIAL_STATIC_PROXY"
        rows.append(
            _gap(
                gap_id=f"window_fragility_{split_id}",
                gap_category="window_fragility_gap",
                source_report=next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"split_status={split_status}; "
                    f"average_return_proxy={split.get('average_return_proxy')}; "
                    f"worst_drawdown_proxy={split.get('worst_drawdown_proxy')}; "
                    f"evaluation={_text(split.get('evaluation'))}"
                ),
                expected_value=(
                    "split_status=WINDOW_STABLE evidence or defensible non-fragile "
                    "mixed evidence without HIGH overfit risk"
                ),
                root_cause_category=(
                    "drawdown_behavior_or_regime_dependence"
                    if weak
                    else "partial_static_proxy_window_evidence"
                    if partial
                    else "window_instability"
                ),
                fix_type=(
                    "redesign_drawdown_or_regime_handling"
                    if weak
                    else "complete_dynamic_binding_before_window_stability_claim"
                ),
                blocking=split_id in blocking_ids or weak or partial,
                requires_candidate_redesign=weak,
                evidence_reference={
                    "window_split_id": split_id,
                    "source_windows": _list_values(split.get("source_windows")),
                    "recommended_action": _text(split.get("recommended_action")),
                },
            )
        )
    return rows


def _stress_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scenario in _records(payload.get("scenario_reviews")):
        scenario_status = _text(scenario.get("scenario_status"))
        if scenario_status in {"PASS", "OK"}:
            continue
        scenario_id = _text(scenario.get("scenario_id"), "unknown_scenario")
        failed = scenario_status == "FAIL"
        rows.append(
            _gap(
                gap_id=f"stress_weakness_{scenario_id}",
                gap_category="stress_failure_gap",
                source_report=next_cycle.STRESS_REVIEW_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"scenario_status={scenario_status}; "
                    f"return_proxy={scenario.get('return_proxy')}; "
                    f"drawdown_proxy={scenario.get('drawdown_proxy')}; "
                    f"evaluation={_text(scenario.get('evaluation'))}"
                ),
                expected_value=(
                    "scenario_status=PASS under complete executable backfill, "
                    "or non-blocking mixed stress evidence"
                ),
                root_cause_category=(
                    "stress_drawdown_breach" if failed else "stress_warning_or_partial_proxy"
                ),
                fix_type=(
                    "redesign_drawdown_or_stress_handling"
                    if failed
                    else "complete_executable_backfill_before_stress_claim"
                ),
                blocking=failed,
                requires_candidate_redesign=failed,
                evidence_reference={
                    "scenario_id": scenario_id,
                    "turnover_proxy": scenario.get("turnover_proxy"),
                    "rotation_count": scenario.get("rotation_count"),
                    "recommended_action": _text(scenario.get("recommended_action")),
                },
            )
        )
    return rows


def _cost_benchmark_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = _mapping(payload.get("summary"))
    rows: list[dict[str, Any]] = []
    if _text(summary.get("cost_survival_status")) != "COST_SURVIVAL_PASS":
        rows.append(
            _gap(
                gap_id="cost_benchmark_cost_survival",
                gap_category="cost_benchmark_weakness_gap",
                source_report=next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"cost_survival_status={_text(summary.get('cost_survival_status'))}; "
                    f"turnover_penalty={summary.get('turnover_penalty')}; "
                    f"net_proxy_result={_text(summary.get('net_proxy_result'))}"
                ),
                expected_value=(
                    "cost_survival_status=COST_SURVIVAL_PASS with complete "
                    "dynamic backfill and durable net proxy margin"
                ),
                root_cause_category="cost_survival_warning_from_partial_proxy",
                fix_type="reduce_turnover_or_complete_cost_evidence",
                blocking=_text(payload.get("status")) == "COST_BENCHMARK_REVIEW_WEAK",
                requires_candidate_redesign=False,
                evidence_reference={
                    "turnover_proxy": summary.get("turnover_proxy"),
                    "aggregate_return_proxy": summary.get("aggregate_return_proxy"),
                },
            )
        )
    for benchmark in _records(payload.get("benchmark_reviews")):
        benchmark_status = _text(benchmark.get("benchmark_relative_status"))
        if benchmark_status == "BENCHMARK_OUTPERFORMS":
            continue
        baseline_id = _text(benchmark.get("baseline_id"), "unknown_baseline")
        underperforms = benchmark_status == "BENCHMARK_UNDERPERFORMS"
        rows.append(
            _gap(
                gap_id=f"cost_benchmark_{baseline_id}",
                gap_category="cost_benchmark_weakness_gap",
                source_report=next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"benchmark_relative_status={benchmark_status}; "
                    f"candidate_delta_vs_baseline="
                    f"{benchmark.get('candidate_delta_vs_baseline')}; "
                    f"minimum_outperformance_threshold="
                    f"{benchmark.get('minimum_outperformance_threshold')}"
                ),
                expected_value=(
                    "benchmark_relative_status=BENCHMARK_OUTPERFORMS or "
                    "review-supported mixed evidence above the minimum threshold"
                ),
                root_cause_category=(
                    "benchmark_underperformance"
                    if underperforms
                    else "insufficient_benchmark_outperformance_margin"
                ),
                fix_type="redesign_benchmark_relative_behavior",
                blocking=_text(payload.get("status")) == "COST_BENCHMARK_REVIEW_WEAK",
                requires_candidate_redesign=underperforms,
                evidence_reference={
                    "baseline_id": baseline_id,
                    "candidate_return_proxy": benchmark.get("candidate_return_proxy"),
                    "baseline_return_proxy": benchmark.get("baseline_return_proxy"),
                },
            )
        )
    return rows


def _comparison_gap_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for comparison in _records(payload.get("comparison_rows")):
        comparison_status = _text(comparison.get("comparison_status"))
        if comparison_status == "IMPROVED_OVER_RETURNED_CANDIDATE":
            continue
        metric_id = _text(comparison.get("metric_id"), "unknown_metric")
        hard_statuses = {
            "REGRESSED_VS_REUSABLE_EVIDENCE",
            "REPEATS_FAILURE_MODE",
            "NO_IMPROVEMENT",
            "WORSE_THAN_RETURNED_CANDIDATE",
        }
        hard_gap = comparison_status in hard_statuses
        rows.append(
            _gap(
                gap_id=f"comparison_weakness_{metric_id}",
                gap_category="comparison_weakness_gap",
                source_report=next_cycle.VS_RETURNED_REPORT_TYPE,
                source_artifact_id=_artifact_id(payload),
                current_value=(
                    f"comparison_status={comparison_status}; "
                    f"new_candidate_evidence={_text(comparison.get('new_candidate_evidence'))}"
                ),
                expected_value=(
                    "comparison_status=IMPROVED_OVER_RETURNED_CANDIDATE or "
                    "explicit evidence that the prior failure mode no longer applies"
                ),
                root_cause_category=_comparison_root_cause(comparison_status),
                fix_type=(
                    "redesign_candidate_hypothesis"
                    if hard_gap
                    else "complete_comparison_evidence"
                ),
                blocking=hard_gap,
                requires_candidate_redesign=hard_gap,
                evidence_reference={
                    "metric_id": metric_id,
                    "returned_failure_mode_id": _text(
                        comparison.get("returned_failure_mode_id")
                    ),
                    "interpretation": _text(comparison.get("interpretation")),
                },
            )
        )
    return rows


def _gap(
    *,
    gap_id: str,
    gap_category: str,
    source_report: str,
    source_artifact_id: str,
    current_value: str,
    expected_value: str,
    root_cause_category: str,
    fix_type: str,
    blocking: bool,
    requires_candidate_redesign: bool,
    evidence_reference: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "gap_id": gap_id,
        "gap_category": gap_category,
        "source_report": source_report,
        "source_artifact_id": source_artifact_id,
        "current_value": current_value,
        "expected_value": expected_value,
        "root_cause_category": root_cause_category,
        "fix_type": fix_type,
        "blocking": blocking,
        "requires_candidate_redesign": requires_candidate_redesign,
        "evidence_reference": dict(evidence_reference),
        "production_effect": PRODUCTION_EFFECT,
    }


def _gap_category_summary(gaps: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for category in REQUIRED_GAP_CATEGORIES:
        category_rows = [
            row for row in gaps if _text(row.get("gap_category")) == category
        ]
        rows.append(
            {
                "gap_category": category,
                "gap_count": len(category_rows),
                "blocking_gap_count": len(
                    [row for row in category_rows if row.get("blocking") is True]
                ),
                "candidate_redesign_gap_count": len(
                    [
                        row
                        for row in category_rows
                        if row.get("requires_candidate_redesign") is True
                    ]
                ),
                "source_reports": sorted(
                    {
                        _text(row.get("source_report"))
                        for row in category_rows
                        if _text(row.get("source_report"))
                    }
                ),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _category_summary_consistent(
    gaps: Sequence[Mapping[str, Any]],
    category_summary: Sequence[Mapping[str, Any]],
) -> bool:
    by_category = {
        _text(row.get("gap_category")): row for row in category_summary
    }
    for category in REQUIRED_GAP_CATEGORIES:
        rows = [row for row in gaps if _text(row.get("gap_category")) == category]
        summary = by_category.get(category)
        if not summary:
            return False
        if _int(summary.get("gap_count")) != len(rows):
            return False
        if _int(summary.get("blocking_gap_count")) != len(
            [row for row in rows if row.get("blocking") is True]
        ):
            return False
        if _int(summary.get("candidate_redesign_gap_count")) != len(
            [row for row in rows if row.get("requires_candidate_redesign") is True]
        ):
            return False
    return True


def _gap_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "gap_id",
        "gap_category",
        "source_report",
        "source_artifact_id",
        "current_value",
        "expected_value",
        "root_cause_category",
        "fix_type",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and isinstance(row.get("blocking"), bool)
        and isinstance(row.get("requires_candidate_redesign"), bool)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _backfill_window_diagnostics(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    partial_reason_ids = _partial_reason_ids(payload)
    rows: list[dict[str, Any]] = []
    windows_by_id = {
        _text(row.get("window_id")): row
        for row in _records(payload.get("backfill_windows"))
        if _text(row.get("window_id"))
    }
    for window_id in REQUIRED_BACKFILL_WINDOWS:
        source = windows_by_id.get(window_id, {"window_id": window_id})
        status = _text(source.get("backfill_window_status"), "MISSING")
        missing_data = _list_values(source.get("missing_data_list"))
        window_issues = [*missing_data]
        if status != "READY":
            window_issues.extend(partial_reason_ids)
        repairability = sorted(
            {
                _classify_backfill_issue(issue_id)["repairability"]
                for issue_id in window_issues
                if _text(issue_id)
            }
        )
        binding_issues = _binding_execution_issues(window_id, window_issues)
        schema_issues = [
            issue_id for issue_id in window_issues if "schema" in issue_id.lower()
        ]
        coverage_issues = [
            issue_id
            for issue_id in window_issues
            if issue_id.startswith(("insufficient_", "missing_price", "price_coverage"))
        ]
        feature_issues = [
            issue_id
            for issue_id in window_issues
            if "feature" in issue_id.lower() or issue_id.startswith("missing_input:")
        ]
        signal_outputs = [
            issue_id
            for issue_id in window_issues
            if "signal" in issue_id.lower() or issue_id.startswith("historical_")
        ]
        missing_dates_status = _missing_dates_status(
            status=status,
            missing_data=missing_data,
        )
        rows.append(
            {
                "window_id": window_id,
                "start": _text(source.get("start")),
                "end": _text(source.get("end")),
                "source_window_status": status,
                "source_signal_completeness": _text(
                    source.get("signal_completeness"),
                    "MISSING",
                ),
                "missing_dates": [],
                "missing_dates_status": missing_dates_status,
                "missing_feature_inputs": feature_issues,
                "missing_signal_outputs": signal_outputs,
                "schema_issue": "; ".join(schema_issues) if schema_issues else "none_reported",
                "insufficient_market_coverage": (
                    coverage_issues if coverage_issues else []
                ),
                "binding_execution_issue": binding_issues,
                "repairability": repairability,
                "repair_path": _window_repair_path(repairability, binding_issues),
                "issue_ids": sorted(set(window_issues)),
                "source_missing_data_list": missing_data,
                "source_partial_reasons": partial_reason_ids,
                "return_proxy_available": source.get("return_proxy") is not None,
                "drawdown_proxy_available": source.get("drawdown_proxy") is not None,
                "price_observation_count": _int(source.get("price_observation_count")),
                "return_observation_count": _int(source.get("return_observation_count")),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _partial_reason_ids(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    raw = payload.get("partial_reasons")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for item in raw:
            if isinstance(item, Mapping):
                reasons.append(_text(item.get("issue_id")))
            else:
                reasons.append(_text(item))
    return sorted({reason for reason in reasons if reason})


def _binding_execution_issues(
    window_id: str,
    issue_ids: Sequence[str],
) -> list[str]:
    binding_terms = (
        "binding",
        "historical_signal_series",
        "single_point_signal",
        "single_point_weight",
        "dynamic_binding",
        "weight_series",
        "signal_series",
    )
    issues = [
        issue_id
        for issue_id in issue_ids
        if any(term in issue_id for term in binding_terms)
    ]
    if f"historical_signal_series:{window_id}" in issue_ids:
        issues.append("missing_window_historical_signal_binding_output")
    return sorted(set(issues))


def _missing_dates_status(*, status: str, missing_data: Sequence[str]) -> str:
    if status == "READY":
        return "not_applicable"
    if any(item.startswith("historical_signal_series:") for item in missing_data):
        return "not_enumerated_in_source_artifact"
    if missing_data:
        return "not_reported_by_source_artifact"
    return "none_reported"


def _window_repair_path(
    repairability: Sequence[str],
    binding_issues: Sequence[str],
) -> str:
    values = set(repairability)
    if "candidate_spec_issue" in values:
        return "revise_candidate_spec_before_repeating_backfill"
    if "binding_repairable" in values and binding_issues:
        return "extend_binding_to_historical_signal_and_weight_series"
    if "data_repairable" in values:
        return "repair_required_backfill_input_data_then_rerun_backfill"
    if "expected_limitation" in values:
        return "document_limitation_and_do_not_claim_complete_backfill"
    return "no_repair_needed_for_ready_window"


def _repair_issue_summary(
    *,
    backfill_payload: Mapping[str, Any],
    window_diagnostics: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues: dict[str, set[str]] = {}
    for row in window_diagnostics:
        for issue_id in _list_values(row.get("issue_ids")):
            issues.setdefault(issue_id, set()).add(_text(row.get("window_id")))
    if not issues:
        for issue_id in _partial_reason_ids(backfill_payload):
            issues.setdefault(issue_id, set())
    rows = []
    for issue_id, windows in sorted(issues.items()):
        classification = _classify_backfill_issue(issue_id)
        rows.append(
            {
                "issue_id": issue_id,
                "affected_windows": sorted(windows),
                "affected_window_count": len(windows),
                "root_cause_category": classification["root_cause_category"],
                "repairability": classification["repairability"],
                "repair_path": classification["repair_path"],
                "source_report": next_cycle.BACKFILL_REPORT_TYPE,
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _classify_backfill_issue(issue_id: str) -> dict[str, str]:
    normalized = issue_id.lower()
    if "candidate_spec" in normalized or "invalid_candidate" in normalized:
        return {
            "repairability": "candidate_spec_issue",
            "root_cause_category": "candidate_spec_issue",
            "repair_path": "revise_candidate_spec_before_repeating_backfill",
        }
    if "schema" in normalized:
        return {
            "repairability": "binding_repairable",
            "root_cause_category": "schema_or_binding_contract_mismatch",
            "repair_path": "repair_binding_schema_to_match_contract",
        }
    if (
        "binding" in normalized
        or "signal_series" in normalized
        or "weight_series" in normalized
        or "historical_signal" in normalized
        or "single_point" in normalized
    ):
        return {
            "repairability": "binding_repairable",
            "root_cause_category": "historical_dynamic_binding_unavailable",
            "repair_path": "extend_binding_to_historical_signal_and_weight_series",
        }
    if (
        "price" in normalized
        or "market_coverage" in normalized
        or "data_quality" in normalized
        or "validated_data_quality" in normalized
        or "feature" in normalized
    ):
        return {
            "repairability": "data_repairable",
            "root_cause_category": "required_backfill_input_data_gap",
            "repair_path": "repair_required_backfill_input_data_then_rerun_backfill",
        }
    return {
        "repairability": "expected_limitation",
        "root_cause_category": "source_artifact_limitation",
        "repair_path": "document_limitation_and_do_not_claim_complete_backfill",
    }


def _overall_backfill_repair_status(
    issue_summary: Sequence[Mapping[str, Any]],
) -> str:
    repairability = {
        _text(row.get("repairability")) for row in issue_summary if _text(row.get("repairability"))
    }
    if not repairability:
        return BACKFILL_REPAIRABLE
    if repairability <= {"data_repairable", "binding_repairable"}:
        return BACKFILL_REPAIRABLE
    if "candidate_spec_issue" in repairability:
        return BACKFILL_NOT_REPAIRABLE_WITH_CURRENT_SPEC
    return BACKFILL_PARTIALLY_REPAIRABLE


def _window_repairability_count(
    diagnostics: Sequence[Mapping[str, Any]],
    repairability: str,
) -> int:
    return len(
        [
            row
            for row in diagnostics
            if repairability in _list_values(row.get("repairability"))
        ]
    )


def _backfill_window_diagnostic_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "window_id",
        "source_window_status",
        "missing_dates",
        "missing_dates_status",
        "missing_feature_inputs",
        "missing_signal_outputs",
        "schema_issue",
        "insufficient_market_coverage",
        "binding_execution_issue",
        "repairability",
        "repair_path",
    )
    return (
        all(key in row for key in required)
        and isinstance(row.get("missing_dates"), list)
        and isinstance(row.get("missing_feature_inputs"), list)
        and isinstance(row.get("missing_signal_outputs"), list)
        and isinstance(row.get("insufficient_market_coverage"), list)
        and isinstance(row.get("binding_execution_issue"), list)
        and isinstance(row.get("repairability"), list)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _reader_brief(
    *,
    summary: str,
    key_result: str,
    blocking_issues: str,
    warnings: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "summary": summary,
        "key_result": key_result,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "safety_boundary": (
            "research-only evidence repair diagnostics; no paper-shadow activation, "
            "no extended shadow, no live trading, no official target weights, "
            "no broker/order, no owner decision append, production_effect=none."
        ),
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "executable_research_evidence_repair_reports_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "paper_shadow_candidate_created": False,
        "paper_shadow_activation_allowed": False,
        "normal_paper_shadow_resumed": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_decision_appended": False,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
    }


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("paper_shadow_candidate_created") is False
        and safety.get("paper_shadow_activation_allowed") is False
        and safety.get("normal_paper_shadow_resumed") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("owner_decision_appended") is False
        and safety.get("production_state_mutated") is False
    )


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _read_json_mapping(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(raw)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_values(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _date_from_payload(payload: Mapping[str, Any]) -> date:
    try:
        return date.fromisoformat(_text(payload.get("as_of")))
    except ValueError:
        return date.today()


def _artifact_id(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"), "artifact")
    as_of = _text(payload.get("as_of"), "unknown")
    return f"{report_type}:{as_of}"


def _issue_names(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    values = [_text(row.get(key)) for row in rows if _text(row.get(key))]
    return "; ".join(values) if values else "none"


def _signal_root_cause(check_id: str) -> str:
    mapping = {
        "partial_signal_series": "partial_signal_series",
        "stale_signal_series": "stale_latest_signal_series",
        "market_coverage_gap": "partial_market_coverage",
        "missing_feature_columns": "missing_feature_columns",
        "schema_version_mismatch": "schema_mismatch",
    }
    return mapping.get(check_id, "signal_binding_fail_closed_condition")


def _comparison_root_cause(comparison_status: str) -> str:
    mapping = {
        "REGRESSED_VS_REUSABLE_EVIDENCE": "regressed_vs_reusable_evidence",
        "REPEATS_FAILURE_MODE": "repeated_returned_candidate_failure_mode",
        "NO_IMPROVEMENT": "no_measurable_improvement",
        "MIXED": "mixed_or_incomplete_comparison_evidence",
        "WORSE_THAN_RETURNED_CANDIDATE": "worse_than_returned_candidate",
    }
    return mapping.get(comparison_status, "comparison_weakness")


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}$")
    candidates: list[tuple[date, Path]] = []
    if not output_dir.exists():
        return None
    for path in output_dir.iterdir():
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            candidates.append((date.fromisoformat(match.group(1)), path))
        except ValueError:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _markdown_tables(report_type: str) -> list[tuple[str, str]]:
    if report_type == EVIDENCE_GAP_LEDGER_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Evidence Gaps", "evidence_gaps"),
            ("Gap Category Summary", "gap_category_summary"),
        ]
    if report_type == EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Window Repair Diagnostics", "window_repair_diagnostics"),
            ("Repair Issue Summary", "repair_issue_summary"),
        ]
    if report_type == BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    return []


def _table_records(title: str, value: Any) -> list[str]:
    rows = _records(value)
    if not rows:
        return ["", f"## {title}", "", "No rows."]
    keys = list(rows[0].keys())[:9]
    lines = [
        "",
        f"## {title}",
        "",
        "|" + "|".join(keys) + "|",
        "|" + "|".join(["---"] * len(keys)) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(_md_cell(row.get(key)) for key in keys) + "|")
    return lines


def _title(report_type: str) -> str:
    return report_type.replace("_", " ").title()


def _md_cell(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    elif value is None:
        text = ""
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br/>")


__all__ = [
    "BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE",
    "BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE",
    "BACKFILL_REPAIRABLE",
    "BACKFILL_PARTIALLY_REPAIRABLE",
    "BACKFILL_NOT_REPAIRABLE_WITH_CURRENT_SPEC",
    "BACKFILL_REPAIR_STATUSES",
    "EVIDENCE_GAP_LEDGER_REPORT_TYPE",
    "EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE",
    "REPORT_PREFIXES",
    "REPAIRABILITY_TYPES",
    "REQUIRED_BACKFILL_WINDOWS",
    "VALIDATION_SUFFIX",
    "build_backfill_partial_root_cause_repair_plan_payload",
    "build_executable_research_evidence_gap_ledger_payload",
    "default_evidence_repair_json_path",
    "default_evidence_repair_markdown_path",
    "latest_evidence_repair_json_path",
    "render_evidence_repair_markdown",
    "validate_backfill_partial_root_cause_repair_plan_payload",
    "validate_executable_research_evidence_gap_ledger_payload",
    "write_evidence_repair_json",
    "write_evidence_repair_markdown",
]
