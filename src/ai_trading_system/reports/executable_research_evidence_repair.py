from __future__ import annotations

import csv
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
SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE = (
    "signal_robustness_blocker_drilldown"
)
WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE = "window_fragility_attribution"
STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE = "stress_weakness_attribution"
COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE = (
    "cost_benchmark_weakness_attribution"
)
CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE = "candidate_redesign_hypothesis_v2"
CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE = "candidate_v2_spec_freeze"
CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE = (
    "candidate_v2_executable_binding_update"
)
CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE = "candidate_v2_mini_backfill"
CANDIDATE_V2_MINI_GATE_REPORT_TYPE = "candidate_v2_mini_gate"
VALIDATION_SUFFIX = "_validation"
EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE = (
    f"{EVIDENCE_GAP_LEDGER_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE = (
    f"{BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE = (
    f"{SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE = (
    f"{WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE = (
    f"{STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE = (
    f"{COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE = (
    f"{CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE = (
    f"{CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE = (
    f"{CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE = (
    f"{CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE}{VALIDATION_SUFFIX}"
)
CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE = (
    f"{CANDIDATE_V2_MINI_GATE_REPORT_TYPE}{VALIDATION_SUFFIX}"
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
SIGNAL_ROBUSTNESS_REPAIRABLE = "SIGNAL_ROBUSTNESS_REPAIRABLE"
SIGNAL_ROBUSTNESS_NEEDS_CANDIDATE_REDESIGN = (
    "SIGNAL_ROBUSTNESS_NEEDS_CANDIDATE_REDESIGN"
)
SIGNAL_ROBUSTNESS_NOT_REPAIRABLE = "SIGNAL_ROBUSTNESS_NOT_REPAIRABLE"
SIGNAL_ROBUSTNESS_DRILLDOWN_STATUSES: tuple[str, ...] = (
    SIGNAL_ROBUSTNESS_REPAIRABLE,
    SIGNAL_ROBUSTNESS_NEEDS_CANDIDATE_REDESIGN,
    SIGNAL_ROBUSTNESS_NOT_REPAIRABLE,
)
WINDOW_FRAGILITY_ATTRIBUTION_READY = "WINDOW_FRAGILITY_ATTRIBUTION_READY"
STRESS_WEAKNESS_ATTRIBUTION_READY = "STRESS_WEAKNESS_ATTRIBUTION_READY"
COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY = (
    "COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY"
)
CANDIDATE_REDESIGN_HYPOTHESIS_READY = "CANDIDATE_REDESIGN_HYPOTHESIS_READY"
CANDIDATE_V2_SPEC_FREEZE_READY = "CANDIDATE_V2_SPEC_FREEZE_READY"
CANDIDATE_V2_EXECUTABLE_BINDING_READY = "CANDIDATE_V2_EXECUTABLE_BINDING_READY"
CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS = (
    "CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS"
)
CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED = "CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED"
CANDIDATE_V2_EXECUTABLE_BINDING_STATUSES: tuple[str, ...] = (
    CANDIDATE_V2_EXECUTABLE_BINDING_READY,
    CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS,
    CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED,
)
V2_MINI_BACKFILL_PROMISING = "V2_MINI_BACKFILL_PROMISING"
V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE = "V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE"
V2_MINI_BACKFILL_WEAK = "V2_MINI_BACKFILL_WEAK"
V2_MINI_BACKFILL_BLOCKED = "V2_MINI_BACKFILL_BLOCKED"
V2_MINI_BACKFILL_STATUSES: tuple[str, ...] = (
    V2_MINI_BACKFILL_PROMISING,
    V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE,
    V2_MINI_BACKFILL_WEAK,
    V2_MINI_BACKFILL_BLOCKED,
)
# TRADING-480 uses sign-only research triage before TRADING-481 gate calibration;
# this is not a tradable acceptance threshold.
V2_MINI_RETURN_WEAKNESS_CUTOFF = 0.0
V2_PROCEED_TO_FULL_BACKFILL = "V2_PROCEED_TO_FULL_BACKFILL"
V2_NEEDS_REDESIGN = "V2_NEEDS_REDESIGN"
V2_REJECT_RESEARCH_CANDIDATE = "V2_REJECT_RESEARCH_CANDIDATE"
V2_BLOCKED = "V2_BLOCKED"
V2_MINI_GATE_STATUSES: tuple[str, ...] = (
    V2_PROCEED_TO_FULL_BACKFILL,
    V2_NEEDS_REDESIGN,
    V2_REJECT_RESEARCH_CANDIDATE,
    V2_BLOCKED,
)
STRESS_DESIGN_JUDGMENTS: tuple[str, ...] = (
    "REDESIGN_REQUIRED",
    "REJECT_CURRENT_CANDIDATE",
    "REPAIR_EVIDENCE_BEFORE_DECISION",
    "STRESS_WEAKNESS_ACCEPTABLE",
)
COST_BENCHMARK_DESIGN_JUDGMENTS: tuple[str, ...] = (
    "REDESIGN_REQUIRED",
    "REJECT_CURRENT_CANDIDATE",
    "REPAIR_EVIDENCE_BEFORE_DECISION",
    "COST_BENCHMARK_WEAKNESS_ACCEPTABLE",
)
CANDIDATE_REDESIGN_PRIORITIES: tuple[str, ...] = ("P0", "P1", "P2")
WINDOW_FRAGILITY_JUDGMENTS: tuple[str, ...] = (
    "OVERFIT_RISK",
    "UNDER_OBSERVED",
    "MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED",
    "ACCEPTABLE_FOR_FURTHER_RESEARCH",
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
    SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: "signal_robustness_blocker_drilldown",
    SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE: (
        "signal_robustness_blocker_drilldown_validation"
    ),
    WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE: "window_fragility_attribution",
    WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE: (
        "window_fragility_attribution_validation"
    ),
    STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE: "stress_weakness_attribution",
    STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE: (
        "stress_weakness_attribution_validation"
    ),
    COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE: (
        "cost_benchmark_weakness_attribution"
    ),
    COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE: (
        "cost_benchmark_weakness_attribution_validation"
    ),
    CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE: "candidate_redesign_hypothesis_v2",
    CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE: (
        "candidate_redesign_hypothesis_v2_validation"
    ),
    CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE: "candidate_v2_spec_freeze",
    CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE: (
        "candidate_v2_spec_freeze_validation"
    ),
    CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE: (
        "candidate_v2_executable_binding_update"
    ),
    CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE: (
        "candidate_v2_executable_binding_update_validation"
    ),
    CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE: "candidate_v2_mini_backfill",
    CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE: (
        "candidate_v2_mini_backfill_validation"
    ),
    CANDIDATE_V2_MINI_GATE_REPORT_TYPE: "candidate_v2_mini_gate",
    CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE: "candidate_v2_mini_gate_validation",
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

SIGNAL_BLOCKER_CAUSES: tuple[str, ...] = (
    "missing_feature_columns",
    "stale_signal_series",
    "schema_mismatch",
    "partial_market_coverage",
    "empty_signal_window",
    "binding_fail_closed_condition",
    "invalid_candidate_assumptions",
)

WINDOW_FRAGILITY_ATTRIBUTION_CATEGORIES: tuple[str, ...] = (
    "regime_dependence",
    "overfit_threshold",
    "signal_instability",
    "turnover_concentration",
    "drawdown_behavior",
    "benchmark_relative_weakness",
    "cost_sensitivity",
)

REQUIRED_WINDOW_SPLITS: tuple[str, ...] = (
    "early_window",
    "middle_window",
    "recent_window",
    "stress_heavy_window",
    "calm_market_window",
)

REQUIRED_STRESS_SCENARIOS: tuple[str, ...] = (
    "rapid_drawdown",
    "slow_drawdown",
    "v_shaped_recovery",
    "high_volatility_sideways_market",
    "false_risk_off_cluster",
    "ai_semiconductor_correction",
)

REQUIRED_COST_SCENARIOS: tuple[str, ...] = ("zero", "low", "medium", "high")

REQUIRED_BENCHMARK_BASELINES: tuple[str, ...] = (
    "static_allocation",
    "no_trade",
    "qqq_only",
    "spy_only",
    "equal_weight_etf",
)

REQUIRED_REDESIGN_TARGETS: tuple[str, ...] = (
    "signal_robustness_repair",
    "lower_turnover",
    "window_stability",
    "stress_handling",
    "benchmark_relative_behavior",
    "cost_survival",
)

V2_SIGNAL_BINDING_VERSION = "candidate_v2_signal_binding_v1"
V2_WEIGHT_BINDING_VERSION = "candidate_v2_research_weight_binding_v1"
DEFAULT_V2_FEATURE_PATH = PROJECT_ROOT / "data" / "etf_portfolio" / "features.csv"
V2_REQUIRED_SIGNAL_SYMBOLS: tuple[str, ...] = ("QQQ", "SMH", "SOXX", "SPY")
V2_AI_SIGNAL_SYMBOLS: tuple[str, ...] = ("QQQ", "SMH", "SOXX")
V2_TURNOVER_GUARD_WINDOWS: tuple[str, ...] = (
    "slow_drawdown",
    "high_volatility_sideways_market",
    "false_risk_off_cluster",
)
V2_MINI_BACKFILL_WINDOWS: tuple[str, ...] = (
    "normal_market_regime",
    "slow_drawdown",
    "high_volatility_sideways_market",
    "false_risk_off_cluster",
)
V2_PRICE_SYMBOLS: tuple[str, ...] = ("QQQ", "SMH", "SOXX", "SPY")
DEFAULT_V2_PRICE_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"


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


def build_signal_robustness_blocker_drilldown_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    paths = _signal_drilldown_source_paths(reports_dir=reports_dir, as_of=as_of)
    signal_review = _read_json_mapping(paths[next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE])
    signal_binding = _read_json_mapping(paths[binding_reports.SIGNAL_BINDING_REPORT_TYPE])
    repair_plan = _read_json_mapping(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE])
    ledger = _read_json_mapping(paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE])
    summary = _mapping(signal_review.get("summary"))
    blocker_rows = _signal_blocker_rows(
        signal_review=signal_review,
        signal_binding=signal_binding,
        repair_plan=repair_plan,
        paths=paths,
    )
    non_blocking_checks = _signal_non_blocking_checks(signal_review)
    drilldown_status = _overall_signal_drilldown_status(blocker_rows)
    repairable_without_rule_relaxation = (
        bool(blocker_rows)
        and all(row.get("repairable_without_rule_relaxation") is True for row in blocker_rows)
    )
    requested_date_range = _text(
        signal_review.get("requested_date_range"),
        _text(summary.get("requested_date_range"), "not_applicable"),
    )
    payload_summary = {
        "signal_drilldown_status": drilldown_status,
        "source_signal_robustness_status": _text(
            summary.get("signal_robustness_status"),
            _text(signal_review.get("status"), "MISSING"),
        ),
        "source_signal_binding_status": _text(
            summary.get("source_signal_binding_status"),
            _text(signal_binding.get("status"), "MISSING"),
        ),
        "source_backfill_repair_status": _text(repair_plan.get("status"), "MISSING"),
        "candidate_id": _text(
            _mapping(signal_binding.get("summary")).get("candidate_id"),
            _text(summary.get("candidate_id"), "MISSING"),
        ),
        "market_regime": _text(signal_review.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_date_range,
        "source_blocking_check_count": _int(summary.get("blocking_check_count")),
        "blocker_count": len(blocker_rows),
        "repairable_blocker_count": len(
            [
                row
                for row in blocker_rows
                if row.get("repairable_without_rule_relaxation") is True
            ]
        ),
        "candidate_redesign_blocker_count": len(
            [row for row in blocker_rows if row.get("requires_candidate_redesign") is True]
        ),
        "not_repairable_blocker_count": len(
            [row for row in blocker_rows if row.get("not_repairable") is True]
        ),
        "repairable_without_rule_relaxation": repairable_without_rule_relaxation,
        "signal_completeness_rules_relaxed": any(
            row.get("signal_completeness_rules_relaxed") is True for row in blocker_rows
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
        report_type=SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
        as_of=as_of,
        status=drilldown_status,
        purpose=(
            "Drill down why signal robustness is blocked and identify whether "
            "the candidate can be repaired without weakening completeness rules."
        ),
        input_artifacts={report_type: str(path) for report_type, path in paths.items()},
        output_decision=drilldown_status,
        summary=payload_summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
                    paths[next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE],
                    signal_review,
                ),
                _source_artifact(
                    binding_reports.SIGNAL_BINDING_REPORT_TYPE,
                    paths[binding_reports.SIGNAL_BINDING_REPORT_TYPE],
                    signal_binding,
                ),
                _source_artifact(
                    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
                    paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE],
                    repair_plan,
                ),
                _source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE],
                    ledger,
                ),
            ],
            "signal_blockers": blocker_rows,
            "non_blocking_signal_checks": non_blocking_checks,
            "classification_policy": {
                "blocker_causes": list(SIGNAL_BLOCKER_CAUSES),
                "status_taxonomy": list(SIGNAL_ROBUSTNESS_DRILLDOWN_STATUSES),
                "repairable_without_rule_relaxation_meaning": (
                    "The blocker can be resolved by repairing signal/binding/data "
                    "inputs while keeping fail-closed completeness rules intact."
                ),
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-473 已生成 signal robustness blocker drilldown；"
                "当前 blocker 可通过修复 historical signal/binding coverage 处理，"
                "不得放松 completeness rules。"
            ),
            key_result=drilldown_status,
            blocking_issues=_issue_names(blocker_rows, "blocker_id"),
            warnings=(
                "signal completeness rules remain fail-closed; no paper-shadow eligibility"
            ),
            next_action="run_trading_474_window_fragility_attribution",
        ),
        next_action="run_trading_474_window_fragility_attribution",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Drilldown is read-only and does not refresh signal inputs.",
            "Historical signal series are not fabricated.",
            "Signal completeness rules are not weakened or relaxed.",
        ],
        requested_date_range=requested_date_range,
    )


def validate_signal_robustness_blocker_drilldown_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    blockers = _records(payload.get("signal_blockers"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
        f"report_type must be {SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE}.",
        "regenerate_signal_robustness_blocker_drilldown",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        _text(payload.get("status")) in SIGNAL_ROBUSTNESS_DRILLDOWN_STATUSES,
        "Signal drilldown status must use the governed TRADING-473 taxonomy.",
        "restore_signal_drilldown_status_taxonomy",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Signal drilldown must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "market_regime_visible",
        _text(payload.get("market_regime")) == MARKET_REGIME
        and _text(summary.get("market_regime")) == MARKET_REGIME,
        f"Signal drilldown must disclose market_regime={MARKET_REGIME}.",
        "restore_ai_after_chatgpt_regime_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "requested_date_range_visible",
        bool(_text(payload.get("requested_date_range"))),
        "Signal drilldown must disclose requested date range.",
        "restore_requested_date_range_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
            binding_reports.SIGNAL_BINDING_REPORT_TYPE,
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        }
        <= source_report_types,
        "Signal drilldown must include signal review, signal binding, repair plan, ledger.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_rows_present",
        bool(blockers),
        "Signal drilldown must include rows for blocking signal checks.",
        "restore_signal_blocker_rows",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_count_consistent",
        _int(summary.get("blocker_count")) == len(blockers)
        and _int(summary.get("blocker_count")) > 0,
        "Blocker count must match signal_blockers and be non-zero.",
        "restore_signal_blocker_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_fields_present",
        all(_signal_blocker_row_complete(row) for row in blockers),
        "Each blocker row must include artifact, field, expected/actual, repair path.",
        "restore_signal_blocker_required_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_causes_valid",
        all(_text(row.get("blocker_cause")) in SIGNAL_BLOCKER_CAUSES for row in blockers),
        "Each blocker cause must use the governed TRADING-473 taxonomy.",
        "restore_signal_blocker_cause_taxonomy",
    )
    _append_check(
        checks,
        blocking_issues,
        "rules_not_relaxed",
        summary.get("signal_completeness_rules_relaxed") is False
        and all(row.get("signal_completeness_rules_relaxed") is False for row in blockers),
        "Signal completeness rules must not be relaxed.",
        "restore_fail_closed_signal_completeness_rules",
    )
    _append_check(
        checks,
        blocking_issues,
        "repair_decision_consistent",
        _signal_repair_decision_consistent(payload),
        "Signal drilldown status must match blocker repairability flags.",
        "restore_signal_repairability_decision",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Signal drilldown must include Reader Brief fields.",
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
        report_type=SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-473 signal robustness blocker drilldown.",
        input_artifacts={SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: _artifact_id(payload)},
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
            summary=f"TRADING-473 signal drilldown validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_signal_robustness_blocker_drilldown"
                if status == FAIL_STATUS
                else "use_validated_signal_drilldown_for_trading_474"
            ),
        ),
        next_action=(
            "repair_signal_robustness_blocker_drilldown"
            if status == FAIL_STATUS
            else "use_validated_signal_drilldown_for_trading_474"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_window_fragility_attribution_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    paths = _window_fragility_source_paths(reports_dir=reports_dir, as_of=as_of)
    window_payload = _read_json_mapping(paths[next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE])
    signal_drilldown = _read_json_mapping(paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE])
    repair_plan = _read_json_mapping(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE])
    ledger = _read_json_mapping(paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE])
    source_summary = _mapping(window_payload.get("summary"))
    split_rows = _window_fragility_rows(
        window_payload=window_payload,
        signal_drilldown=signal_drilldown,
        repair_plan=repair_plan,
    )
    failure_modes = _window_failure_modes(split_rows, source_summary)
    judgment = _window_fragility_judgment(split_rows, source_summary)
    acceptable = judgment == "ACCEPTABLE_FOR_FURTHER_RESEARCH"
    requested_date_range = _text(
        window_payload.get("requested_date_range"),
        _text(source_summary.get("requested_date_range"), "not_applicable"),
    )
    fragile_windows = [
        row["window_split_id"] for row in split_rows if row["fragility_class"] == "fragile"
    ]
    stable_windows = [
        row["window_split_id"] for row in split_rows if row["fragility_class"] == "stable"
    ]
    under_observed_windows = [
        row["window_split_id"]
        for row in split_rows
        if row["fragility_class"] == "under_observed"
    ]
    summary = {
        "window_fragility_attribution_status": WINDOW_FRAGILITY_ATTRIBUTION_READY,
        "source_window_sensitivity_status": _text(
            source_summary.get("window_sensitivity_status"),
            _text(window_payload.get("status"), "MISSING"),
        ),
        "source_signal_drilldown_status": _text(signal_drilldown.get("status"), "MISSING"),
        "source_backfill_repair_status": _text(repair_plan.get("status"), "MISSING"),
        "candidate_id": _text(
            _mapping(signal_drilldown.get("summary")).get("candidate_id"),
            "MISSING",
        ),
        "market_regime": _text(window_payload.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_date_range,
        "split_count": len(split_rows),
        "fragile_window_count": len(fragile_windows),
        "stable_window_count": len(stable_windows),
        "under_observed_window_count": len(under_observed_windows),
        "overfit_risk": _text(source_summary.get("overfit_risk"), "UNKNOWN"),
        "performance_dispersion": source_summary.get("performance_dispersion"),
        "drawdown_behavior_dispersion": source_summary.get(
            "drawdown_behavior_dispersion"
        ),
        "turnover_dispersion": source_summary.get("turnover_dispersion"),
        "fragility_judgment": judgment,
        "acceptable_for_further_research": acceptable,
        "acceptance_condition": (
            "repair_dynamic_signal_binding_and_redesign_drawdown_or_stress_handling"
            if not acceptable
            else "window_fragility_not_blocking"
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
        report_type=WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
        as_of=as_of,
        status=WINDOW_FRAGILITY_ATTRIBUTION_READY,
        purpose=(
            "Attribute why the next candidate is window-fragile and decide "
            "whether fragility is acceptable for further research."
        ),
        input_artifacts={report_type: str(path) for report_type, path in paths.items()},
        output_decision=judgment,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
                    paths[next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE],
                    window_payload,
                ),
                _source_artifact(
                    SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
                    paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE],
                    signal_drilldown,
                ),
                _source_artifact(
                    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
                    paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE],
                    repair_plan,
                ),
                _source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE],
                    ledger,
                ),
            ],
            "window_attributions": split_rows,
            "fragile_windows": fragile_windows,
            "stable_windows": stable_windows,
            "under_observed_windows": under_observed_windows,
            "failure_modes": failure_modes,
            "classification_policy": {
                "required_window_splits": list(REQUIRED_WINDOW_SPLITS),
                "attribution_categories": list(WINDOW_FRAGILITY_ATTRIBUTION_CATEGORIES),
                "judgment_taxonomy": list(WINDOW_FRAGILITY_JUDGMENTS),
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-474 已生成 window fragility attribution；"
                "当前 fragility 同时包含 HIGH overfit/drawdown 风险和 "
                "partial static proxy 的 under-observed evidence。"
            ),
            key_result=judgment,
            blocking_issues=_issue_names(failure_modes, "failure_mode_id"),
            warnings="not acceptable for further research without repair/redesign",
            next_action="run_trading_475_stress_weakness_attribution",
        ),
        next_action="run_trading_475_stress_weakness_attribution",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Attribution is read-only and does not rerun backfill.",
            "Partial static proxy windows are not treated as complete stability evidence.",
            "No thresholds are tuned to hide fragile windows.",
        ],
        requested_date_range=requested_date_range,
    )


def validate_window_fragility_attribution_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    rows = _records(payload.get("window_attributions"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}
    split_ids = {_text(row.get("window_split_id")) for row in rows}

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
        f"report_type must be {WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE}.",
        "regenerate_window_fragility_attribution",
    )
    _append_check(
        checks,
        blocking_issues,
        "status",
        _text(payload.get("status")) == WINDOW_FRAGILITY_ATTRIBUTION_READY,
        f"status must be {WINDOW_FRAGILITY_ATTRIBUTION_READY}.",
        "restore_window_fragility_attribution_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Window attribution must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "market_regime_visible",
        _text(payload.get("market_regime")) == MARKET_REGIME
        and _text(summary.get("market_regime")) == MARKET_REGIME,
        f"Window attribution must disclose market_regime={MARKET_REGIME}.",
        "restore_ai_after_chatgpt_regime_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "requested_date_range_visible",
        bool(_text(payload.get("requested_date_range"))),
        "Window attribution must disclose requested date range.",
        "restore_requested_date_range_disclosure",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        }
        <= source_report_types,
        "Window attribution must include window review, signal drilldown, repair plan, ledger.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_splits_present",
        set(REQUIRED_WINDOW_SPLITS) <= split_ids,
        "Window attribution must include early/middle/recent/stress-heavy/calm splits.",
        "restore_required_window_splits",
    )
    _append_check(
        checks,
        blocking_issues,
        "attribution_rows_complete",
        all(_window_attribution_row_complete(row) for row in rows),
        "Each window attribution row must include required category fields.",
        "restore_window_attribution_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "failure_modes_present",
        bool(_records(payload.get("failure_modes"))),
        "Window attribution must include failure modes.",
        "restore_window_failure_modes",
    )
    _append_check(
        checks,
        blocking_issues,
        "judgment_valid",
        _text(summary.get("fragility_judgment")) in WINDOW_FRAGILITY_JUDGMENTS,
        "Fragility judgment must use the governed taxonomy.",
        "restore_window_fragility_judgment",
    )
    _append_check(
        checks,
        blocking_issues,
        "window_counts_consistent",
        _window_counts_consistent(payload),
        "Fragile/stable/under-observed counts must match attribution rows.",
        "restore_window_fragility_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Window attribution must include Reader Brief fields.",
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
        report_type=WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-474 window fragility attribution.",
        input_artifacts={WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE: _artifact_id(payload)},
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
            summary=f"TRADING-474 window attribution validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_window_fragility_attribution"
                if status == FAIL_STATUS
                else "use_validated_window_attribution_for_trading_475"
            ),
        ),
        next_action=(
            "repair_window_fragility_attribution"
            if status == FAIL_STATUS
            else "use_validated_window_attribution_for_trading_475"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_stress_weakness_attribution_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    paths = _stress_attribution_source_paths(reports_dir=reports_dir, as_of=as_of)
    stress_payload = _read_json_mapping(paths[next_cycle.STRESS_REVIEW_REPORT_TYPE])
    window_attribution = _read_json_mapping(paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE])
    signal_drilldown = _read_json_mapping(paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE])
    repair_plan = _read_json_mapping(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE])
    ledger = _read_json_mapping(paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE])
    stress_summary = _mapping(stress_payload.get("summary"))
    scenario_rows = _stress_scenario_attribution_rows(stress_payload)
    root_causes = _stress_root_causes(scenario_rows, stress_summary)
    design_judgment = _stress_design_judgment(scenario_rows, stress_summary)
    requested_date_range = _text(
        stress_payload.get("requested_date_range"),
        _text(stress_summary.get("requested_date_range"), "not_applicable"),
    )
    failed_rows = [
        row for row in scenario_rows if row["scenario_status"] in {"FAIL", "MISSING"}
    ]
    warning_rows = [row for row in scenario_rows if row["scenario_status"] == "WARNING"]
    summary = {
        "stress_weakness_attribution_status": STRESS_WEAKNESS_ATTRIBUTION_READY,
        "source_stress_result": _text(
            stress_summary.get("stress_result"),
            _text(stress_payload.get("status"), "MISSING"),
        ),
        "source_window_fragility_judgment": _text(
            _mapping(window_attribution.get("summary")).get("fragility_judgment"),
            _text(window_attribution.get("output_decision"), "MISSING"),
        ),
        "source_signal_drilldown_status": _text(signal_drilldown.get("status"), "MISSING"),
        "source_backfill_repair_status": _text(repair_plan.get("status"), "MISSING"),
        "candidate_id": _text(stress_summary.get("candidate_id"), "MISSING"),
        "market_regime": _text(stress_payload.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_date_range,
        "required_scenario_count": len(REQUIRED_STRESS_SCENARIOS),
        "source_scenario_count": len(_records(stress_payload.get("scenario_reviews"))),
        "failed_scenario_count": len(failed_rows),
        "warning_scenario_count": len(warning_rows),
        "root_cause_count": len(root_causes),
        "design_judgment": design_judgment,
        "redesign_required": design_judgment == "REDESIGN_REQUIRED",
        "reject_current_candidate": design_judgment == "REJECT_CURRENT_CANDIDATE",
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        as_of=as_of,
        status=STRESS_WEAKNESS_ATTRIBUTION_READY,
        purpose=(
            "Attribute why the next candidate stress review is weak and decide "
            "whether the candidate should be redesigned or rejected."
        ),
        input_artifacts={report_type: str(path) for report_type, path in paths.items()},
        output_decision=design_judgment,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    next_cycle.STRESS_REVIEW_REPORT_TYPE,
                    paths[next_cycle.STRESS_REVIEW_REPORT_TYPE],
                    stress_payload,
                ),
                _source_artifact(
                    WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
                    paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE],
                    window_attribution,
                ),
                _source_artifact(
                    SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
                    paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE],
                    signal_drilldown,
                ),
                _source_artifact(
                    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
                    paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE],
                    repair_plan,
                ),
                _source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE],
                    ledger,
                ),
            ],
            "stress_scenario_attributions": scenario_rows,
            "stress_weakness_root_causes": root_causes,
            "candidate_design_implications": _stress_design_implications(
                design_judgment,
                root_causes,
            ),
            "classification_policy": {
                "required_stress_scenarios": list(REQUIRED_STRESS_SCENARIOS),
                "design_judgment_taxonomy": list(STRESS_DESIGN_JUDGMENTS),
                "does_not_tune_thresholds": True,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-475 已生成 stress weakness attribution；"
                "slow drawdown 是 fail scenario，其他 stress rows 多为 warning 或 coverage gap。"
            ),
            key_result=design_judgment,
            blocking_issues=_issue_names(root_causes, "root_cause_id"),
            warnings="partial static proxy remains a stress evidence limitation",
            next_action="run_trading_476_cost_benchmark_weakness_attribution",
        ),
        next_action="run_trading_476_cost_benchmark_weakness_attribution",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Attribution is read-only and does not rerun stress or backfill.",
            "No thresholds are tuned to hide stress failure.",
            "Benchmark behavior is not fabricated when the stress source does not isolate it.",
        ],
        requested_date_range=requested_date_range,
    )


def validate_stress_weakness_attribution_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    rows = _records(payload.get("stress_scenario_attributions"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}
    scenario_ids = {_text(row.get("scenario_id")) for row in rows}

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        f"report_type must be {STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE}.",
        "regenerate_stress_weakness_attribution",
    )
    _append_check(
        checks,
        blocking_issues,
        "status",
        _text(payload.get("status")) == STRESS_WEAKNESS_ATTRIBUTION_READY,
        f"status must be {STRESS_WEAKNESS_ATTRIBUTION_READY}.",
        "restore_stress_weakness_attribution_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Stress attribution must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            next_cycle.STRESS_REVIEW_REPORT_TYPE,
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        }
        <= source_report_types,
        "Stress attribution must include stress/window/signal/repair/ledger sources.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_scenarios_present",
        set(REQUIRED_STRESS_SCENARIOS) <= scenario_ids,
        "Stress attribution must include all required stress scenarios.",
        "restore_required_stress_scenarios",
    )
    _append_check(
        checks,
        blocking_issues,
        "scenario_rows_complete",
        all(_stress_scenario_row_complete(row) for row in rows),
        (
            "Each stress scenario row must include behavior, benchmark, drawdown, "
            "rotation, turnover fields."
        ),
        "restore_stress_scenario_attribution_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "root_causes_present",
        bool(_records(payload.get("stress_weakness_root_causes"))),
        "Stress attribution must include root causes.",
        "restore_stress_root_causes",
    )
    _append_check(
        checks,
        blocking_issues,
        "design_judgment_valid",
        _text(summary.get("design_judgment")) in STRESS_DESIGN_JUDGMENTS,
        "Stress design judgment must use the governed taxonomy.",
        "restore_stress_design_judgment",
    )
    _append_check(
        checks,
        blocking_issues,
        "counts_consistent",
        _stress_counts_consistent(payload),
        "Stress scenario counts must match attribution rows.",
        "restore_stress_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Stress attribution must include Reader Brief fields.",
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
        report_type=STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-475 stress weakness attribution.",
        input_artifacts={STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE: _artifact_id(payload)},
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
            summary=f"TRADING-475 stress attribution validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_stress_weakness_attribution"
                if status == FAIL_STATUS
                else "use_validated_stress_attribution_for_trading_476"
            ),
        ),
        next_action=(
            "repair_stress_weakness_attribution"
            if status == FAIL_STATUS
            else "use_validated_stress_attribution_for_trading_476"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_cost_benchmark_weakness_attribution_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    paths = _cost_benchmark_attribution_source_paths(
        reports_dir=reports_dir,
        as_of=as_of,
    )
    cost_payload = _read_json_mapping(paths[next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE])
    cost_source = _read_json_mapping(paths["cost_sensitivity_framework"])
    benchmark_source = _read_json_mapping(paths["benchmark_baseline_control"])
    stress_attribution = _read_json_mapping(paths[STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE])
    window_attribution = _read_json_mapping(paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE])
    repair_plan = _read_json_mapping(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE])
    ledger = _read_json_mapping(paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE])
    cost_summary = _mapping(cost_payload.get("summary"))
    cost_rows = _cost_weakness_rows(cost_payload, cost_source)
    benchmark_rows = _benchmark_weakness_rows(cost_payload, benchmark_source)
    root_causes = _cost_benchmark_root_causes(
        cost_rows,
        benchmark_rows,
        cost_summary,
        cost_source,
        benchmark_source,
    )
    design_judgment = _cost_benchmark_design_judgment(root_causes)
    requested_date_range = _text(
        cost_payload.get("requested_date_range"),
        _text(cost_summary.get("requested_date_range"), "not_applicable"),
    )
    summary = {
        "cost_benchmark_weakness_attribution_status": (
            COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY
        ),
        "source_cost_benchmark_status": _text(cost_payload.get("status"), "MISSING"),
        "source_cost_survival_status": _text(
            cost_summary.get("cost_survival_status"),
            "MISSING",
        ),
        "source_benchmark_relative_status": _text(
            cost_summary.get("benchmark_relative_status"),
            "MISSING",
        ),
        "source_cost_sensitivity_status": _text(
            cost_source.get("cost_sensitivity_status"),
            "MISSING",
        ),
        "source_benchmark_baseline_status": _text(
            benchmark_source.get("benchmark_baseline_status"),
            "MISSING",
        ),
        "source_stress_design_judgment": _text(
            _mapping(stress_attribution.get("summary")).get("design_judgment"),
            "MISSING",
        ),
        "source_window_fragility_judgment": _text(
            _mapping(window_attribution.get("summary")).get("fragility_judgment"),
            "MISSING",
        ),
        "source_backfill_repair_status": _text(repair_plan.get("status"), "MISSING"),
        "candidate_id": _text(
            cost_summary.get("candidate_id"),
            _text(cost_source.get("candidate"), "MISSING"),
        ),
        "market_regime": _text(cost_payload.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_date_range,
        "cost_scenario_count": len(cost_rows),
        "benchmark_baseline_count": len(benchmark_rows),
        "cost_weakness_count": len(
            [row for row in cost_rows if row["cost_weakness_reason"] != "none"]
        ),
        "benchmark_weakness_count": len(
            [
                row
                for row in benchmark_rows
                if row["benchmark_weakness_reason"] != "none"
            ]
        ),
        "root_cause_count": len(root_causes),
        "design_judgment": design_judgment,
        "fixable_by_candidate_redesign": design_judgment == "REDESIGN_REQUIRED",
        "reject_current_candidate": design_judgment == "REJECT_CURRENT_CANDIDATE",
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        as_of=as_of,
        status=COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY,
        purpose=(
            "Attribute why the executable cost/benchmark review remains weak and "
            "whether weakness is fixable by candidate redesign."
        ),
        input_artifacts={report_type: str(path) for report_type, path in paths.items()},
        output_decision=design_judgment,
        summary=summary,
        body={
            "source_artifacts": [
                _cost_benchmark_source_artifact(
                    next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
                    paths[next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE],
                    cost_payload,
                ),
                _cost_benchmark_source_artifact(
                    "cost_sensitivity_framework",
                    paths["cost_sensitivity_framework"],
                    cost_source,
                ),
                _cost_benchmark_source_artifact(
                    "benchmark_baseline_control",
                    paths["benchmark_baseline_control"],
                    benchmark_source,
                ),
                _cost_benchmark_source_artifact(
                    STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
                    paths[STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE],
                    stress_attribution,
                ),
                _cost_benchmark_source_artifact(
                    WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
                    paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE],
                    window_attribution,
                ),
                _cost_benchmark_source_artifact(
                    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
                    paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE],
                    repair_plan,
                ),
                _cost_benchmark_source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE],
                    ledger,
                ),
            ],
            "cost_scenario_attributions": cost_rows,
            "benchmark_baseline_attributions": benchmark_rows,
            "cost_benchmark_root_causes": root_causes,
            "candidate_design_implications": _cost_benchmark_design_implications(
                design_judgment,
                root_causes,
            ),
            "classification_policy": {
                "required_cost_scenarios": list(REQUIRED_COST_SCENARIOS),
                "required_benchmark_baselines": list(REQUIRED_BENCHMARK_BASELINES),
                "design_judgment_taxonomy": list(COST_BENCHMARK_DESIGN_JUDGMENTS),
                "uses_source_thresholds_only": True,
                "does_not_tune_thresholds": True,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-476 已生成 cost/benchmark weakness attribution；"
                "weakness 来自 weak gross/net improvement、benchmark margin 不足和 "
                "partial static proxy limitation。"
            ),
            key_result=design_judgment,
            blocking_issues=_issue_names(root_causes, "root_cause_id"),
            warnings="partial static proxy remains a benchmark interpretation limitation",
            next_action="run_trading_477_candidate_redesign_hypothesis_v2",
        ),
        next_action="run_trading_477_candidate_redesign_hypothesis_v2",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Attribution is read-only and does not rerun cost, benchmark, or backfill.",
            "No thresholds are tuned to hide cost or benchmark weakness.",
            (
                "Defensive and recovery behavior are not fabricated when source rows "
                "do not isolate them."
            ),
        ],
        requested_date_range=requested_date_range,
    )


def validate_cost_benchmark_weakness_attribution_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    cost_rows = _records(payload.get("cost_scenario_attributions"))
    benchmark_rows = _records(payload.get("benchmark_baseline_attributions"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_keys = {_text(row.get("source_key")) for row in source_artifacts}
    cost_ids = {_text(row.get("scenario_id")) for row in cost_rows}
    baseline_ids = {_text(row.get("baseline_id")) for row in benchmark_rows}

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        f"report_type must be {COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE}.",
        "regenerate_cost_benchmark_weakness_attribution",
    )
    _append_check(
        checks,
        blocking_issues,
        "status",
        _text(payload.get("status")) == COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY,
        f"status must be {COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY}.",
        "restore_cost_benchmark_attribution_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Cost/benchmark attribution must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
            "cost_sensitivity_framework",
            "benchmark_baseline_control",
            STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        }
        <= source_keys,
        "Cost/benchmark attribution must include review, source policy, and repair sources.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_cost_scenarios_present",
        set(REQUIRED_COST_SCENARIOS) <= cost_ids,
        "Cost attribution must include zero/low/medium/high scenarios.",
        "restore_required_cost_scenarios",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_benchmarks_present",
        set(REQUIRED_BENCHMARK_BASELINES) <= baseline_ids,
        "Benchmark attribution must include required baseline rows.",
        "restore_required_benchmark_baselines",
    )
    _append_check(
        checks,
        blocking_issues,
        "cost_rows_complete",
        all(_cost_scenario_attribution_row_complete(row) for row in cost_rows),
        "Each cost row must include turnover, drag, gross/net proxy, and reason fields.",
        "restore_cost_scenario_attribution_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "benchmark_rows_complete",
        all(_benchmark_attribution_row_complete(row) for row in benchmark_rows),
        "Each benchmark row must include baseline status, delta, and reason fields.",
        "restore_benchmark_attribution_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "root_causes_present",
        bool(_records(payload.get("cost_benchmark_root_causes"))),
        "Cost/benchmark attribution must include root causes.",
        "restore_cost_benchmark_root_causes",
    )
    _append_check(
        checks,
        blocking_issues,
        "design_judgment_valid",
        _text(summary.get("design_judgment")) in COST_BENCHMARK_DESIGN_JUDGMENTS,
        "Cost/benchmark design judgment must use the governed taxonomy.",
        "restore_cost_benchmark_design_judgment",
    )
    _append_check(
        checks,
        blocking_issues,
        "counts_consistent",
        _cost_benchmark_counts_consistent(payload),
        "Cost/benchmark counts must match attribution rows.",
        "restore_cost_benchmark_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Cost/benchmark attribution must include Reader Brief fields.",
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
        report_type=COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-476 cost/benchmark weakness attribution.",
        input_artifacts={
            COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE: _artifact_id(payload)
        },
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
            summary=f"TRADING-476 cost/benchmark attribution validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_cost_benchmark_weakness_attribution"
                if status == FAIL_STATUS
                else "use_validated_cost_benchmark_attribution_for_trading_477"
            ),
        ),
        next_action=(
            "repair_cost_benchmark_weakness_attribution"
            if status == FAIL_STATUS
            else "use_validated_cost_benchmark_attribution_for_trading_477"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not rerun source reports."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_candidate_redesign_hypothesis_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    paths = _candidate_redesign_source_paths(reports_dir=reports_dir, as_of=as_of)
    ledger = _read_json_mapping(paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE])
    repair_plan = _read_json_mapping(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE])
    signal_drilldown = _read_json_mapping(paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE])
    window_attribution = _read_json_mapping(paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE])
    stress_attribution = _read_json_mapping(paths[STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE])
    cost_attribution = _read_json_mapping(
        paths[COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE]
    )
    hypotheses = _candidate_redesign_hypotheses(
        signal_drilldown=signal_drilldown,
        window_attribution=window_attribution,
        stress_attribution=stress_attribution,
        cost_attribution=cost_attribution,
        repair_plan=repair_plan,
        ledger=ledger,
    )
    target_coverage = _redesign_target_coverage(hypotheses)
    priority_counts = {
        priority: len(
            [row for row in hypotheses if _text(row.get("priority")) == priority]
        )
        for priority in CANDIDATE_REDESIGN_PRIORITIES
    }
    summary = {
        "candidate_redesign_hypothesis_status": CANDIDATE_REDESIGN_HYPOTHESIS_READY,
        "source_ledger_status": _text(ledger.get("status"), "MISSING"),
        "source_repair_plan_status": _text(repair_plan.get("status"), "MISSING"),
        "source_signal_drilldown_status": _text(signal_drilldown.get("status"), "MISSING"),
        "source_window_attribution_status": _text(
            window_attribution.get("status"),
            "MISSING",
        ),
        "source_stress_design_judgment": _text(
            _mapping(stress_attribution.get("summary")).get("design_judgment"),
            "MISSING",
        ),
        "source_cost_benchmark_design_judgment": _text(
            _mapping(cost_attribution.get("summary")).get("design_judgment"),
            "MISSING",
        ),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _text(
            cost_attribution.get("requested_date_range"),
            _text(stress_attribution.get("requested_date_range"), "not_applicable"),
        ),
        "hypothesis_count": len(hypotheses),
        "p0_hypothesis_count": priority_counts["P0"],
        "p1_hypothesis_count": priority_counts["P1"],
        "p2_hypothesis_count": priority_counts["P2"],
        "target_coverage_count": len(target_coverage),
        "required_target_count": len(REQUIRED_REDESIGN_TARGETS),
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
        as_of=as_of,
        status=CANDIDATE_REDESIGN_HYPOTHESIS_READY,
        purpose=(
            "Generate research-only v2 redesign hypotheses from TRADING-471~476 "
            "evidence repair findings."
        ),
        input_artifacts={report_type: str(path) for report_type, path in paths.items()},
        output_decision="HYPOTHESES_READY_FOR_TRADING_478_SPEC_FREEZE_REVIEW",
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    EVIDENCE_GAP_LEDGER_REPORT_TYPE,
                    paths[EVIDENCE_GAP_LEDGER_REPORT_TYPE],
                    ledger,
                ),
                _source_artifact(
                    BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
                    paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE],
                    repair_plan,
                ),
                _source_artifact(
                    SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
                    paths[SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE],
                    signal_drilldown,
                ),
                _source_artifact(
                    WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
                    paths[WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE],
                    window_attribution,
                ),
                _source_artifact(
                    STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
                    paths[STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE],
                    stress_attribution,
                ),
                _source_artifact(
                    COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
                    paths[COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE],
                    cost_attribution,
                ),
            ],
            "candidate_redesign_hypotheses": hypotheses,
            "target_coverage": target_coverage,
            "selection_boundary": {
                "selects_final_spec": False,
                "implements_binding": False,
                "runs_backfill": False,
                "paper_shadow_activation_allowed": False,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-477 已生成 v2 redesign hypotheses；"
                "P0 聚焦 signal repair、stress/window drawdown guard 和 "
                "turnover/cost/benchmark guard。"
            ),
            key_result="HYPOTHESES_READY_FOR_TRADING_478_SPEC_FREEZE_REVIEW",
            blocking_issues="none",
            warnings="hypotheses are not frozen specs and are not paper-shadow eligible",
            next_action="run_trading_478_candidate_v2_spec_freeze",
        ),
        next_action="run_trading_478_candidate_v2_spec_freeze",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Hypotheses are research planning artifacts only.",
            "This report does not freeze a v2 spec or implement executable binding.",
            "This report does not run backfill or activate paper-shadow.",
        ],
        requested_date_range=_text(summary.get("requested_date_range"), "not_applicable"),
    )


def validate_candidate_redesign_hypothesis_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    hypotheses = _records(payload.get("candidate_redesign_hypotheses"))
    source_artifacts = _records(payload.get("source_artifacts"))
    source_report_types = {_text(row.get("report_type")) for row in source_artifacts}
    target_ids = {
        target
        for row in hypotheses
        for target in _list_values(row.get("target_areas"))
    }

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
        f"report_type must be {CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE}.",
        "regenerate_candidate_redesign_hypotheses",
    )
    _append_check(
        checks,
        blocking_issues,
        "status",
        _text(payload.get("status")) == CANDIDATE_REDESIGN_HYPOTHESIS_READY,
        f"status must be {CANDIDATE_REDESIGN_HYPOTHESIS_READY}.",
        "restore_candidate_redesign_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Candidate redesign hypotheses must keep production_effect=none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        {
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        }
        <= source_report_types,
        "Hypothesis report must include all TRADING-471~476 source artifacts.",
        "restore_required_source_loading",
    )
    _append_check(
        checks,
        blocking_issues,
        "hypotheses_present",
        bool(hypotheses),
        "Hypothesis report must include candidate redesign hypotheses.",
        "restore_candidate_redesign_hypotheses",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_targets_covered",
        set(REQUIRED_REDESIGN_TARGETS) <= target_ids,
        "Hypotheses must cover all required redesign target areas.",
        "restore_required_redesign_target_coverage",
    )
    _append_check(
        checks,
        blocking_issues,
        "hypothesis_rows_complete",
        all(_candidate_hypothesis_row_complete(row) for row in hypotheses),
        "Each hypothesis must include logic changes, validation method, and stop condition.",
        "restore_hypothesis_required_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "priority_taxonomy",
        all(_text(row.get("priority")) in CANDIDATE_REDESIGN_PRIORITIES for row in hypotheses),
        "Hypothesis priorities must be P0/P1/P2.",
        "restore_hypothesis_priority_taxonomy",
    )
    _append_check(
        checks,
        blocking_issues,
        "counts_consistent",
        _candidate_hypothesis_counts_consistent(payload),
        "Hypothesis counts must match rows.",
        "restore_hypothesis_counts",
    )
    _append_check(
        checks,
        blocking_issues,
        "selection_boundary_locked",
        _candidate_selection_boundary_valid(payload.get("selection_boundary")),
        (
            "Hypothesis report must not select spec, implement binding, run backfill, "
            "or activate shadow."
        ),
        "restore_hypothesis_selection_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Candidate redesign hypotheses must include Reader Brief fields.",
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
        report_type=CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-477 candidate redesign hypotheses.",
        input_artifacts={CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE: _artifact_id(payload)},
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
            summary=f"TRADING-477 candidate redesign validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_candidate_redesign_hypotheses"
                if status == FAIL_STATUS
                else "use_validated_hypotheses_for_trading_478"
            ),
        ),
        next_action=(
            "repair_candidate_redesign_hypotheses"
            if status == FAIL_STATUS
            else "use_validated_hypotheses_for_trading_478"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not select a final v2 spec."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_candidate_v2_spec_freeze_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    hypothesis_path = default_evidence_repair_json_path(
        CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    hypothesis_payload = _read_json_mapping(hypothesis_path)
    hypotheses = _records(hypothesis_payload.get("candidate_redesign_hypotheses"))
    selected = _select_candidate_v2_hypothesis(hypotheses)
    frozen_spec = _candidate_v2_frozen_spec(selected)
    differences = _candidate_v2_differences_from_trading_470(selected)
    summary = {
        "candidate_v2_spec_freeze_status": CANDIDATE_V2_SPEC_FREEZE_READY,
        "source_hypothesis_status": _text(hypothesis_payload.get("status"), "MISSING"),
        "selected_hypothesis_id": _text(selected.get("hypothesis_id")),
        "selected_hypothesis_priority": _text(selected.get("priority")),
        "selected_hypothesis_target_count": len(_list_values(selected.get("target_areas"))),
        "candidate_id": _text(frozen_spec.get("candidate_id")),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _text(
            hypothesis_payload.get("requested_date_range"),
            "not_applicable",
        ),
        "difference_count": len(differences),
        "stop_condition_count": len(_list_values(frozen_spec.get("stop_conditions"))),
        "paper_shadow_eligible": False,
        "binding_implemented": False,
        "backfill_run": False,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
        as_of=as_of,
        status=CANDIDATE_V2_SPEC_FREEZE_READY,
        purpose="Freeze a research-only v2 candidate spec from the selected P0 hypothesis.",
        input_artifacts={
            CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE: str(hypothesis_path)
        },
        output_decision="V2_SPEC_FROZEN_FOR_TRADING_479_BINDING_UPDATE",
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
                    hypothesis_path,
                    hypothesis_payload,
                )
            ],
            "selected_hypothesis": selected,
            "frozen_candidate_spec": frozen_spec,
            "differences_from_trading_470_candidate": differences,
            "freeze_boundary": {
                "research_only_spec": True,
                "paper_shadow_eligible": False,
                "implements_binding": False,
                "runs_backfill": False,
                "generates_official_target_weights": False,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=(
                "TRADING-478 已冻结 research-only v2 spec；"
                f"selected={_text(selected.get('hypothesis_id'))}。"
            ),
            key_result="V2_SPEC_FROZEN_FOR_TRADING_479_BINDING_UPDATE",
            blocking_issues="none",
            warnings="frozen spec is not paper-shadow eligible and has no binding yet",
            next_action="run_trading_479_candidate_v2_executable_binding_update",
        ),
        next_action="run_trading_479_candidate_v2_executable_binding_update",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Spec freeze is research-only and does not implement executable binding.",
            "Spec freeze does not run mini or full backfill.",
            "Spec freeze is not paper-shadow eligible.",
        ],
        requested_date_range=_text(summary.get("requested_date_range"), "not_applicable"),
    )


def validate_candidate_v2_spec_freeze_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    selected = _mapping(payload.get("selected_hypothesis"))
    frozen_spec = _mapping(payload.get("frozen_candidate_spec"))
    differences = _records(payload.get("differences_from_trading_470_candidate"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
        f"report_type must be {CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE}.",
        "regenerate_candidate_v2_spec_freeze",
    )
    _append_check(
        checks,
        blocking_issues,
        "status",
        _text(payload.get("status")) == CANDIDATE_V2_SPEC_FREEZE_READY,
        f"status must be {CANDIDATE_V2_SPEC_FREEZE_READY}.",
        "restore_candidate_v2_spec_freeze_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "selected_hypothesis_p0",
        _text(selected.get("priority")) == "P0",
        "Frozen spec must select a P0 hypothesis.",
        "restore_p0_hypothesis_selection",
    )
    _append_check(
        checks,
        blocking_issues,
        "frozen_spec_complete",
        _candidate_v2_spec_complete(frozen_spec),
        (
            "Frozen spec must include candidate id, inputs, assumptions, "
            "expectations, windows, and stops."
        ),
        "restore_candidate_v2_spec_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "differences_present",
        bool(differences) and all(_candidate_v2_difference_complete(row) for row in differences),
        "Frozen spec must state how v2 differs from the TRADING-470 candidate.",
        "restore_v2_difference_rows",
    )
    _append_check(
        checks,
        blocking_issues,
        "freeze_boundary_locked",
        _candidate_v2_freeze_boundary_valid(payload.get("freeze_boundary")),
        "Spec freeze must not implement binding, run backfill, or create shadow eligibility.",
        "restore_v2_freeze_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "summary_boundary_locked",
        summary.get("paper_shadow_eligible") is False
        and summary.get("binding_implemented") is False
        and summary.get("backfill_run") is False,
        "Summary must keep paper-shadow, binding, and backfill disabled.",
        "restore_v2_summary_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_text(_mapping(payload.get("reader_brief")).get("key_result"))),
        "Candidate v2 spec freeze must include Reader Brief fields.",
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
        report_type=CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=status,
        purpose="Validate TRADING-478 candidate v2 spec freeze.",
        input_artifacts={CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE: _artifact_id(payload)},
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
            summary=f"TRADING-478 candidate v2 spec freeze validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_candidate_v2_spec_freeze"
                if status == FAIL_STATUS
                else "use_validated_v2_spec_for_trading_479"
            ),
        ),
        next_action=(
            "repair_candidate_v2_spec_freeze"
            if status == FAIL_STATUS
            else "use_validated_v2_spec_for_trading_479"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only and does not implement v2 binding."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_candidate_v2_executable_binding_update_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    feature_path: Path = DEFAULT_V2_FEATURE_PATH,
    weight_policy_path: Path = binding_reports.DEFAULT_WEIGHT_BINDING_POLICY_PATH,
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec_path = default_evidence_repair_json_path(
        CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    backfill_path = next_cycle.default_next_research_cycle_json_path(
        next_cycle.BACKFILL_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    spec_payload = _read_json_mapping(spec_path)
    backfill_payload = _read_json_mapping(backfill_path)
    spec = _mapping(spec_payload.get("frozen_candidate_spec"))
    candidate_id = _text(spec.get("candidate_id"), "MISSING")
    requested_range = _text(
        spec_payload.get("requested_date_range"),
        _text(
            _mapping(backfill_payload.get("summary")).get("requested_date_range"),
            f"{AI_REGIME_START}..unspecified",
        ),
    )
    data_quality = _v2_data_quality_gate(data_quality_gate)
    binding_windows = _candidate_v2_binding_windows(spec, backfill_payload)
    blocking_reasons = _candidate_v2_binding_blocking_reasons(
        spec_payload=spec_payload,
        spec=spec,
        data_quality=data_quality,
        feature_path=feature_path,
        binding_windows=binding_windows,
    )
    warning_reasons: list[str] = []
    signal_rows: list[dict[str, Any]] = []
    signal_coverage: list[dict[str, Any]] = []
    if not blocking_reasons:
        signal_rows, signal_coverage, signal_warnings = _candidate_v2_signal_rows(
            candidate_id=candidate_id,
            feature_path=feature_path,
            binding_windows=binding_windows,
        )
        warning_reasons.extend(signal_warnings)
        if not signal_rows:
            blocking_reasons.append("empty_v2_signal_series")
    policy = _candidate_v2_weight_policy(weight_policy_path)
    weight_rows: list[dict[str, Any]] = []
    if not blocking_reasons:
        weight_rows, weight_blocking = _candidate_v2_weight_rows(
            candidate_id=candidate_id,
            signal_rows=signal_rows,
            policy=policy,
        )
        blocking_reasons.extend(weight_blocking)
    warning_reasons.extend(_candidate_v2_binding_warnings(data_quality, signal_coverage))
    safety_audit = _candidate_v2_binding_safety_audit(
        signal_rows=signal_rows,
        weight_rows=weight_rows,
        spec=spec,
        data_quality=data_quality,
        blocking_reasons=blocking_reasons,
        warning_reasons=warning_reasons,
    )
    safety_status = _text(safety_audit.get("safety_audit_status"))
    if blocking_reasons or safety_status == binding_reports.SAFETY_BLOCKED:
        status = CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED
    elif warning_reasons or safety_status == binding_reports.SAFETY_WARNING:
        status = CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS
    else:
        status = CANDIDATE_V2_EXECUTABLE_BINDING_READY
    latest_signal = _latest_by_date(signal_rows, "signal_date")
    latest_weight = _latest_by_date(weight_rows, "signal_date")
    latest_weight_object = _mapping(latest_weight.get("hypothetical_research_weight"))
    average_turnover = (
        round(
            sum(_float(row.get("turnover_proxy")) for row in weight_rows)
            / len(weight_rows),
            6,
        )
        if weight_rows
        else None
    )
    context_coverage = _candidate_v2_context_coverage(signal_coverage)
    summary = {
        "candidate_v2_executable_binding_status": status,
        "source_spec_status": _text(spec_payload.get("status")),
        "candidate_id": candidate_id,
        "signal_binding_status": (
            "V2_SIGNAL_BINDING_COMPLETE"
            if signal_rows
            else "V2_SIGNAL_BINDING_BLOCKED"
        ),
        "signal_row_count": len(signal_rows),
        "covered_validation_context_count": len(
            [row for row in context_coverage if row.get("covered") is True]
        ),
        "required_validation_context_count": len(context_coverage),
        "latest_signal_date": _text(latest_signal.get("signal_date")),
        "latest_signal_score": latest_signal.get("signal_score"),
        "research_weight_binding_status": (
            "V2_RESEARCH_WEIGHT_BINDING_COMPLETE"
            if weight_rows
            else "V2_RESEARCH_WEIGHT_BINDING_BLOCKED"
        ),
        "weight_row_count": len(weight_rows),
        "latest_turnover_proxy": latest_weight.get("turnover_proxy"),
        "average_turnover_proxy": average_turnover,
        "latest_hypothetical_weight": _mapping(latest_weight_object.get("weights")),
        "safety_audit_status": safety_status,
        "safety_audit_allows_mini_backfill": safety_status
        in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING},
        "data_quality_status": _text(data_quality.get("status")),
        "data_quality_passed": data_quality.get("passed") is True,
        "blocking_reason": _join_reasons(blocking_reasons),
        "warning_reason": _join_reasons(warning_reasons),
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "paper_shadow_eligible": False,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Implement the frozen candidate v2 research-only signal binding, "
            "hypothetical research weight binding, and executable binding safety "
            "audit without producing paper-shadow, official weights, broker/order, "
            "or production outputs."
        ),
        input_artifacts={
            CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE: str(spec_path),
            next_cycle.BACKFILL_REPORT_TYPE: str(backfill_path),
            "feature_cache": str(feature_path),
            "weight_binding_policy": str(weight_policy_path),
            "data_quality_report": _text(data_quality.get("report_path")),
        },
        output_decision=status,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE, spec_path, spec_payload),
                _source_artifact(next_cycle.BACKFILL_REPORT_TYPE, backfill_path, backfill_payload),
            ],
            "v2_signal_binding": {
                "candidate_id": candidate_id,
                "binding_version": V2_SIGNAL_BINDING_VERSION,
                "source_spec_version": _text(spec.get("spec_version")),
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "hypothetical_research_weight_produced": False,
                "backfill_metrics_produced": False,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": "none",
                "order_effect": "none",
            },
            "v2_candidate_signal_series": signal_rows,
            "v2_signal_context_coverage": context_coverage,
            "v2_research_weight_binding": {
                "candidate_id": candidate_id,
                "binding_version": V2_WEIGHT_BINDING_VERSION,
                "source_signal_binding_version": V2_SIGNAL_BINDING_VERSION,
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "paper_shadow_activation_produced": False,
                "broker_order_produced": False,
                "backfill_metrics_produced": False,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": "none",
                "order_effect": "none",
            },
            "v2_hypothetical_research_weight_series": weight_rows,
            "v2_executable_binding_safety_audit": safety_audit,
            "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
            "warning_reasons": list(dict.fromkeys(warning_reasons)),
            "data_quality_gate": data_quality,
            "candidate_v2_spec": dict(spec),
            "binding_windows": binding_windows,
        },
        reader_brief=_reader_brief(
            summary=(
                f"Candidate v2 executable binding update is {status}; "
                f"signal rows={len(signal_rows)}, weight rows={len(weight_rows)}."
            ),
            key_result=status,
            blocking_issues=_join_reasons(blocking_reasons) or "none",
            warnings=_join_reasons(warning_reasons) or "none",
            next_action=(
                "repair_candidate_v2_executable_binding"
                if status == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED
                else "run_trading_480_candidate_v2_mini_backfill"
            ),
        ),
        next_action=(
            "repair_candidate_v2_executable_binding"
            if status == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED
            else "run_trading_480_candidate_v2_mini_backfill"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "candidate_v2_executable_binding_update",
            "signal_binding_implemented": bool(signal_rows),
            "hypothetical_research_weights_generated": bool(weight_rows),
            "backfill_metrics_generated": False,
            "safety_audit_status": safety_status,
        },
        limitations=[
            "TRADING-479 implements research-only v2 binding, not backfill metrics.",
            "Hypothetical weights are for later research evaluation only.",
            "Safety audit failure blocks TRADING-480 mini backfill.",
        ],
        requested_date_range=requested_range,
    )


def validate_candidate_v2_executable_binding_update_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    status = _text(payload.get("status"))
    summary = _mapping(payload.get("summary"))
    signal_binding = _mapping(payload.get("v2_signal_binding"))
    weight_binding = _mapping(payload.get("v2_research_weight_binding"))
    signal_rows = _records(payload.get("v2_candidate_signal_series"))
    weight_rows = _records(payload.get("v2_hypothetical_research_weight_series"))
    safety_audit = _mapping(payload.get("v2_executable_binding_safety_audit"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
        f"report_type must be {CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE}.",
        "regenerate_candidate_v2_executable_binding_update",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status in CANDIDATE_V2_EXECUTABLE_BINDING_STATUSES,
        "Candidate v2 executable binding status must be recognized.",
        "restore_candidate_v2_binding_status",
    )
    _append_check(
        checks,
        blocking,
        "source_spec_ready",
        _text(summary.get("source_spec_status")) == CANDIDATE_V2_SPEC_FREEZE_READY,
        "TRADING-479 must read a ready TRADING-478 spec freeze artifact.",
        "rerun_candidate_v2_spec_freeze",
    )
    _append_check(
        checks,
        blocking,
        "research_only_metadata",
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and signal_binding.get("research_only") is True
        and weight_binding.get("research_only") is True
        and signal_binding.get("official_target_weights") is False
        and weight_binding.get("official_target_weights") is False,
        "Signal and weight bindings must be research-only and not official weights.",
        "restore_candidate_v2_binding_metadata",
    )
    _append_check(
        checks,
        blocking,
        "non_static_signal_series_when_ready",
        status == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED or len(signal_rows) > 1,
        "Ready v2 binding must repair the static one-row signal limitation.",
        "regenerate_historical_v2_signal_series",
    )
    _append_check(
        checks,
        blocking,
        "weight_series_present_when_ready",
        status == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED or bool(weight_rows),
        "Ready v2 binding must include hypothetical research weight rows.",
        "regenerate_v2_weight_binding",
    )
    _append_check(
        checks,
        blocking,
        "weight_outputs_research_only",
        all(_candidate_v2_weight_row_safe(row) for row in weight_rows),
        "All v2 weight rows must carry research-only metadata.",
        "restore_research_only_weight_metadata",
    )
    _append_check(
        checks,
        blocking,
        "safety_audit_not_failed_when_ready",
        status == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED
        or _text(safety_audit.get("safety_audit_status"))
        in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING},
        "Ready v2 binding requires pass or acceptable warning safety audit.",
        "repair_candidate_v2_safety_audit",
    )
    _append_check(
        checks,
        blocking,
        "data_quality_visible",
        bool(_text(summary.get("data_quality_status"))),
        "Data quality gate status must be visible in summary.",
        "run_aits_validate_data_before_binding",
    )
    _append_check(
        checks,
        blocking,
        "no_execution_surface",
        summary.get("paper_shadow_activation_allowed") is False
        and summary.get("official_target_weights") is False
        and summary.get("broker_order_allowed") is False
        and summary.get("owner_decision_appended") is False
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        (
            "TRADING-479 must not create paper-shadow, official weights, "
            "broker/order, or production output."
        ),
        "remove_execution_surface",
    )
    _append_check(
        checks,
        blocking,
        "reader_brief",
        _reader_brief_complete(payload),
        "Reader Brief fields must be populated.",
        "restore_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/official weights/broker/order/production.",
        "restore_safety_boundary",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-479 candidate v2 executable binding update.",
        input_artifacts={
            CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE: _artifact_id(payload)
        },
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "source_status": status,
            "signal_row_count": len(signal_rows),
            "weight_row_count": len(weight_rows),
            "safety_audit_status": _text(safety_audit.get("safety_audit_status")),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=(
                f"{CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE} validation is "
                f"{validation_status}."
            ),
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_candidate_v2_executable_binding"
                if validation_status == FAIL_STATUS
                else "use_validated_candidate_v2_executable_binding_update"
            ),
        ),
        next_action=(
            "repair_candidate_v2_executable_binding"
            if validation_status == FAIL_STATUS
            else "use_validated_candidate_v2_executable_binding_update"
        ),
        safety_boundary=_safety_boundary()
        | {"mode": "candidate_v2_executable_binding_update_validation"},
        limitations=["Validation is read-only and does not run mini backfill."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_candidate_v2_mini_backfill_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    prices_path: Path = DEFAULT_V2_PRICE_PATH,
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    binding_path = default_evidence_repair_json_path(
        CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    binding_payload = _read_json_mapping(binding_path)
    binding_summary = _mapping(binding_payload.get("summary"))
    candidate_id = _text(binding_summary.get("candidate_id"), "MISSING")
    requested_range = _text(
        binding_payload.get("requested_date_range"),
        f"{AI_REGIME_START}..unspecified",
    )
    data_quality = _v2_data_quality_gate(data_quality_gate)
    blocking_reasons = _candidate_v2_mini_backfill_blocking_reasons(
        binding_payload=binding_payload,
        data_quality=data_quality,
        prices_path=prices_path,
    )
    warning_reasons = list(_list_values(binding_payload.get("warning_reasons")))
    mini_windows: list[dict[str, Any]] = []
    price_warnings: list[str] = []
    if not blocking_reasons:
        price_history, price_warnings = _candidate_v2_price_history(
            prices_path=prices_path,
            symbols=V2_PRICE_SYMBOLS,
        )
        if price_warnings:
            blocking_reasons.extend(price_warnings)
        else:
            mini_windows = _candidate_v2_mini_backfill_windows(
                binding_payload=binding_payload,
                price_history=price_history,
            )
    aggregate = _candidate_v2_mini_aggregate(mini_windows)
    if blocking_reasons:
        status = V2_MINI_BACKFILL_BLOCKED
    else:
        status = _candidate_v2_mini_status(mini_windows, aggregate)
    if status == V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE:
        warning_reasons.append("representative_window_evidence_incomplete_or_mixed")
    if data_quality.get("warning_count"):
        warning_reasons.append("data_quality_gate_passed_with_warnings")
    summary = {
        "candidate_v2_mini_backfill_status": status,
        "source_binding_status": _text(binding_payload.get("status")),
        "source_safety_audit_status": _text(binding_summary.get("safety_audit_status")),
        "candidate_id": candidate_id,
        "mini_window_count": len(mini_windows),
        "required_mini_window_count": len(V2_MINI_BACKFILL_WINDOWS),
        "completed_window_count": len(
            [
                row
                for row in mini_windows
                if _text(row.get("mini_backfill_window_status")) == "COMPLETE"
            ]
        ),
        "partial_window_count": len(
            [
                row
                for row in mini_windows
                if _text(row.get("mini_backfill_window_status")) == "PARTIAL"
            ]
        ),
        "aggregate_return_proxy": aggregate.get("aggregate_return_proxy"),
        "aggregate_drawdown_proxy": aggregate.get("aggregate_drawdown_proxy"),
        "turnover_proxy": aggregate.get("turnover_proxy"),
        "rotation_count": aggregate.get("rotation_count"),
        "false_risk_off_count": aggregate.get("false_risk_off_count"),
        "signal_completeness": aggregate.get("signal_completeness"),
        "signal_completeness_ratio": aggregate.get("signal_completeness_ratio"),
        "data_quality_status": _text(data_quality.get("status")),
        "data_quality_passed": data_quality.get("passed") is True,
        "blocking_reason": _join_reasons(blocking_reasons),
        "warning_reason": _join_reasons(warning_reasons),
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Run a compact research-only candidate v2 mini backfill across "
            "representative normal, drawdown, high-volatility sideways, and "
            "false risk-off windows before any full backfill."
        ),
        input_artifacts={
            CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE: str(binding_path),
            "prices": str(prices_path),
            "data_quality_report": _text(data_quality.get("report_path")),
        },
        output_decision=status,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(
                    CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
                    binding_path,
                    binding_payload,
                )
            ],
            "mini_backfill_windows": mini_windows,
            "aggregate_metrics": aggregate,
            "cost_proxy_inputs": _candidate_v2_mini_cost_proxy_inputs(mini_windows),
            "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
            "warning_reasons": list(dict.fromkeys(warning_reasons)),
            "data_quality_gate": data_quality,
        },
        reader_brief=_reader_brief(
            summary=(
                f"Candidate v2 mini backfill is {status}; "
                f"windows={len(mini_windows)}/{len(V2_MINI_BACKFILL_WINDOWS)}."
            ),
            key_result=status,
            blocking_issues=_join_reasons(blocking_reasons) or "none",
            warnings=_join_reasons(warning_reasons) or "none",
            next_action=(
                "repair_candidate_v2_mini_backfill_inputs"
                if status == V2_MINI_BACKFILL_BLOCKED
                else "run_trading_481_candidate_v2_mini_gate"
            ),
        ),
        next_action=(
            "repair_candidate_v2_mini_backfill_inputs"
            if status == V2_MINI_BACKFILL_BLOCKED
            else "run_trading_481_candidate_v2_mini_gate"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "candidate_v2_mini_backfill",
            "mini_backfill_executed": status != V2_MINI_BACKFILL_BLOCKED,
            "full_backfill_executed": False,
            "paper_shadow_outputs_generated": False,
            "official_target_weights_generated": False,
            "broker_order_generated": False,
            "backfill_scope": "compact_representative_windows_only",
        },
        limitations=[
            "TRADING-480 is a compact mini backfill, not a full research backfill.",
            "V-shaped recovery remains outside this compact mini-backfill window set.",
            "Mini-backfill status does not permit paper-shadow activation.",
        ],
        requested_date_range=requested_range,
    )


def validate_candidate_v2_mini_backfill_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    status = _text(payload.get("status"))
    summary = _mapping(payload.get("summary"))
    windows = _records(payload.get("mini_backfill_windows"))
    aggregate = _mapping(payload.get("aggregate_metrics"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
        f"report_type must be {CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE}.",
        "regenerate_candidate_v2_mini_backfill",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status in V2_MINI_BACKFILL_STATUSES,
        "Candidate v2 mini backfill status must be recognized.",
        "restore_candidate_v2_mini_backfill_status",
    )
    _append_check(
        checks,
        blocking,
        "source_binding_safety_allows_backfill",
        status == V2_MINI_BACKFILL_BLOCKED
        or _text(summary.get("source_safety_audit_status"))
        in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING},
        "Mini backfill requires pass or acceptable warning source safety audit.",
        "rerun_or_repair_candidate_v2_binding_safety_audit",
    )
    _append_check(
        checks,
        blocking,
        "required_windows_present_when_not_blocked",
        status == V2_MINI_BACKFILL_BLOCKED
        or {row.get("window_id") for row in windows} == set(V2_MINI_BACKFILL_WINDOWS),
        "Mini backfill must cover the compact representative window set.",
        "restore_candidate_v2_mini_window_set",
    )
    _append_check(
        checks,
        blocking,
        "metrics_present_when_not_blocked",
        status == V2_MINI_BACKFILL_BLOCKED
        or (
            aggregate.get("aggregate_return_proxy") is not None
            and aggregate.get("aggregate_drawdown_proxy") is not None
            and aggregate.get("turnover_proxy") is not None
        ),
        "Mini backfill metrics must include return, drawdown, and turnover proxies.",
        "restore_candidate_v2_mini_metrics",
    )
    _append_check(
        checks,
        blocking,
        "cost_proxy_inputs_present",
        status == V2_MINI_BACKFILL_BLOCKED
        or bool(_records(payload.get("cost_proxy_inputs"))),
        "Mini backfill must expose cost proxy inputs.",
        "restore_cost_proxy_inputs",
    )
    _append_check(
        checks,
        blocking,
        "data_quality_visible",
        bool(_text(summary.get("data_quality_status"))),
        "Data quality gate status must be visible.",
        "run_aits_validate_data_before_mini_backfill",
    )
    _append_check(
        checks,
        blocking,
        "no_execution_surface",
        summary.get("paper_shadow_activation_allowed") is False
        and summary.get("official_target_weights") is False
        and summary.get("broker_order_allowed") is False
        and summary.get("owner_decision_appended") is False
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Mini backfill must not create execution, paper-shadow, or production output.",
        "remove_execution_surface",
    )
    _append_check(
        checks,
        blocking,
        "reader_brief",
        _reader_brief_complete(payload),
        "Reader Brief fields must be populated.",
        "restore_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/official weights/broker/order/production.",
        "restore_safety_boundary",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-480 candidate v2 mini backfill.",
        input_artifacts={CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE: _artifact_id(payload)},
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "source_status": status,
            "mini_window_count": len(windows),
            "aggregate_return_proxy": aggregate.get("aggregate_return_proxy"),
            "aggregate_drawdown_proxy": aggregate.get("aggregate_drawdown_proxy"),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE} validation is {validation_status}.",
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_candidate_v2_mini_backfill"
                if validation_status == FAIL_STATUS
                else "use_validated_candidate_v2_mini_backfill"
            ),
        ),
        next_action=(
            "repair_candidate_v2_mini_backfill"
            if validation_status == FAIL_STATUS
            else "use_validated_candidate_v2_mini_backfill"
        ),
        safety_boundary=_safety_boundary()
        | {"mode": "candidate_v2_mini_backfill_validation"},
        limitations=["Validation is read-only and does not run full backfill."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def build_candidate_v2_mini_gate_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec_path = default_evidence_repair_json_path(
        CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    binding_path = default_evidence_repair_json_path(
        CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    mini_path = default_evidence_repair_json_path(
        CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    mini_validation_path = default_evidence_repair_json_path(
        CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    signal_path = default_evidence_repair_json_path(
        SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    cost_path = default_evidence_repair_json_path(
        COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    spec_payload = _read_json_mapping(spec_path)
    binding_payload = _read_json_mapping(binding_path)
    mini_payload = _read_json_mapping(mini_path)
    mini_validation_payload = _read_json_mapping(mini_validation_path)
    signal_payload = _read_json_mapping(signal_path)
    cost_payload = _read_json_mapping(cost_path)
    spec = _mapping(spec_payload.get("frozen_candidate_spec"))
    candidate_id = _text(
        _mapping(mini_payload.get("summary")).get("candidate_id"),
        _text(spec.get("candidate_id"), "MISSING"),
    )
    data_quality = _v2_data_quality_gate(data_quality_gate)
    signal_quick_check = _candidate_v2_mini_gate_signal_quick_check(
        binding_payload=binding_payload,
        signal_payload=signal_payload,
        mini_payload=mini_payload,
    )
    turnover_cost_quick_check = _candidate_v2_mini_gate_turnover_cost_quick_check(
        mini_payload=mini_payload,
        cost_payload=cost_payload,
    )
    positive_evidence = _candidate_v2_mini_gate_positive_evidence(
        mini_payload=mini_payload,
        signal_quick_check=signal_quick_check,
        turnover_cost_quick_check=turnover_cost_quick_check,
    )
    negative_evidence = _candidate_v2_mini_gate_negative_evidence(
        binding_payload=binding_payload,
        mini_payload=mini_payload,
        signal_quick_check=signal_quick_check,
        turnover_cost_quick_check=turnover_cost_quick_check,
    )
    gate_checks = _candidate_v2_mini_gate_input_checks(
        spec_payload=spec_payload,
        binding_payload=binding_payload,
        mini_payload=mini_payload,
        mini_validation_payload=mini_validation_payload,
        data_quality=data_quality,
        signal_quick_check=signal_quick_check,
        turnover_cost_quick_check=turnover_cost_quick_check,
    )
    decision, full_backfill_allowed, blocked_reason = _candidate_v2_mini_gate_decision(
        gate_checks=gate_checks,
        mini_payload=mini_payload,
    )
    mini_summary = _mapping(mini_payload.get("summary"))
    binding_summary = _mapping(binding_payload.get("summary"))
    requested_range = _text(
        mini_payload.get("requested_date_range"),
        _text(spec_payload.get("requested_date_range"), f"{AI_REGIME_START}..unspecified"),
    )
    summary = {
        "candidate_v2_mini_gate_decision": decision,
        "candidate_id": candidate_id,
        "source_spec_status": _text(spec_payload.get("status")),
        "source_binding_status": _text(binding_payload.get("status")),
        "source_mini_backfill_status": _text(mini_payload.get("status")),
        "source_mini_validation_status": _text(mini_validation_payload.get("status")),
        "source_safety_audit_status": _text(binding_summary.get("safety_audit_status")),
        "signal_quick_check_status": _text(signal_quick_check.get("status")),
        "turnover_cost_quick_check_status": _text(turnover_cost_quick_check.get("status")),
        "data_quality_status": _text(data_quality.get("status")),
        "data_quality_passed": data_quality.get("passed") is True,
        "source_mini_data_quality_status": _text(mini_summary.get("data_quality_status")),
        "full_backfill_allowed": full_backfill_allowed,
        "full_backfill_blocked_reason": blocked_reason,
        "strongest_positive_count": len(positive_evidence),
        "strongest_negative_count": len(negative_evidence),
        "research_only": True,
        "manual_review_only": True,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CANDIDATE_V2_MINI_GATE_REPORT_TYPE,
        as_of=as_of,
        status=decision,
        purpose=(
            "Decide whether candidate v2 deserves a full backfill after the "
            "compact mini backfill, without permitting paper-shadow or execution."
        ),
        input_artifacts={
            CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE: str(spec_path),
            CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE: str(binding_path),
            CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE: str(mini_path),
            CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE: str(
                mini_validation_path
            ),
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: str(signal_path),
            COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE: str(cost_path),
            "data_quality_report": _text(data_quality.get("report_path")),
        },
        output_decision=decision,
        summary=summary,
        body={
            "source_artifacts": [
                _source_artifact(CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE, spec_path, spec_payload),
                _source_artifact(
                    CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
                    binding_path,
                    binding_payload,
                ),
                _source_artifact(CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE, mini_path, mini_payload),
                _source_artifact(
                    CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE,
                    mini_validation_path,
                    mini_validation_payload,
                ),
                _source_artifact(
                    SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
                    signal_path,
                    signal_payload,
                ),
                _source_artifact(
                    COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
                    cost_path,
                    cost_payload,
                ),
            ],
            "gate_input_checks": gate_checks,
            "signal_robustness_quick_check": signal_quick_check,
            "turnover_cost_quick_check": turnover_cost_quick_check,
            "strongest_positive_evidence": positive_evidence,
            "strongest_negative_evidence": negative_evidence,
            "data_quality_gate": data_quality,
        },
        reader_brief=_reader_brief(
            summary=(
                f"Candidate v2 mini gate decision is {decision}; "
                f"full_backfill_allowed={full_backfill_allowed}."
            ),
            key_result=decision,
            blocking_issues=blocked_reason or "none",
            warnings=_join_reasons([_text(mini_summary.get("warning_reason"))]) or "none",
            next_action=(
                "run_trading_482_candidate_v2_full_backfill"
                if full_backfill_allowed
                else "redesign_or_stop_candidate_v2_before_full_backfill"
            ),
        ),
        next_action=(
            "run_trading_482_candidate_v2_full_backfill"
            if full_backfill_allowed
            else "redesign_or_stop_candidate_v2_before_full_backfill"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "candidate_v2_mini_gate",
            "full_backfill_allowed": full_backfill_allowed,
            "full_backfill_executed": False,
            "paper_shadow_outputs_generated": False,
            "official_target_weights_generated": False,
            "broker_order_generated": False,
        },
        limitations=[
            "TRADING-481 is a gate decision only and does not run full backfill.",
            "A weak mini-backfill status blocks TRADING-482 full backfill.",
            "The gate does not approve paper-shadow, extended shadow, or live trading.",
        ],
        requested_date_range=requested_range,
    )


def validate_candidate_v2_mini_gate_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    status = _text(payload.get("status"))
    summary = _mapping(payload.get("summary"))
    gate_checks = _records(payload.get("gate_input_checks"))
    positive = _records(payload.get("strongest_positive_evidence"))
    negative = _records(payload.get("strongest_negative_evidence"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == CANDIDATE_V2_MINI_GATE_REPORT_TYPE,
        f"report_type must be {CANDIDATE_V2_MINI_GATE_REPORT_TYPE}.",
        "regenerate_candidate_v2_mini_gate",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status in V2_MINI_GATE_STATUSES,
        "Candidate v2 mini gate decision must be recognized.",
        "restore_candidate_v2_mini_gate_status",
    )
    _append_check(
        checks,
        blocking,
        "gate_checks_present",
        bool(gate_checks),
        "Mini gate must expose source input checks.",
        "restore_candidate_v2_mini_gate_checks",
    )
    _append_check(
        checks,
        blocking,
        "weak_mini_blocks_full_backfill",
        _text(summary.get("source_mini_backfill_status")) != V2_MINI_BACKFILL_WEAK
        or (
            status != V2_PROCEED_TO_FULL_BACKFILL
            and summary.get("full_backfill_allowed") is False
        ),
        "Weak mini backfill must block full backfill.",
        "restore_weak_mini_hard_stop",
    )
    _append_check(
        checks,
        blocking,
        "proceed_requires_promising_validated_inputs",
        status != V2_PROCEED_TO_FULL_BACKFILL
        or (
            _text(summary.get("source_mini_backfill_status"))
            == V2_MINI_BACKFILL_PROMISING
            and _text(summary.get("source_mini_validation_status")) == PASS_STATUS
            and _text(summary.get("source_safety_audit_status"))
            in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING}
            and summary.get("data_quality_passed") is True
        ),
        (
            "Proceed decision requires promising mini backfill, validation pass, "
            "safety pass/warning, and data quality pass."
        ),
        "restore_candidate_v2_proceed_requirements",
    )
    _append_check(
        checks,
        blocking,
        "evidence_lists_present",
        bool(positive) and bool(negative),
        "Mini gate must include strongest positive and negative evidence.",
        "restore_candidate_v2_gate_evidence",
    )
    _append_check(
        checks,
        blocking,
        "quick_checks_present",
        bool(_mapping(payload.get("signal_robustness_quick_check")))
        and bool(_mapping(payload.get("turnover_cost_quick_check"))),
        "Mini gate must include signal robustness and turnover/cost quick checks.",
        "restore_candidate_v2_quick_checks",
    )
    _append_check(
        checks,
        blocking,
        "no_execution_surface",
        summary.get("paper_shadow_activation_allowed") is False
        and summary.get("official_target_weights") is False
        and summary.get("broker_order_allowed") is False
        and summary.get("owner_decision_appended") is False
        and _text(summary.get("production_effect")) == PRODUCTION_EFFECT,
        "Mini gate must not create execution, paper-shadow, or production output.",
        "remove_execution_surface",
    )
    _append_check(
        checks,
        blocking,
        "reader_brief",
        _reader_brief_complete(payload),
        "Reader Brief fields must be populated.",
        "restore_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/official weights/broker/order/production.",
        "restore_safety_boundary",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-481 candidate v2 mini gate.",
        input_artifacts={CANDIDATE_V2_MINI_GATE_REPORT_TYPE: _artifact_id(payload)},
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "source_status": status,
            "full_backfill_allowed": summary.get("full_backfill_allowed") is True,
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{CANDIDATE_V2_MINI_GATE_REPORT_TYPE} validation is {validation_status}.",
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_candidate_v2_mini_gate"
                if validation_status == FAIL_STATUS
                else "use_validated_candidate_v2_mini_gate"
            ),
        ),
        next_action=(
            "repair_candidate_v2_mini_gate"
            if validation_status == FAIL_STATUS
            else "use_validated_candidate_v2_mini_gate"
        ),
        safety_boundary=_safety_boundary()
        | {"mode": "candidate_v2_mini_gate_validation"},
        limitations=["Validation is read-only and does not run full backfill."],
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
            "does_not_run_backfill": report_type != CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
            "does_not_run_full_backfill": True,
            "runs_compact_mini_backfill": (
                report_type == CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE
            ),
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


def _signal_drilldown_source_paths(*, reports_dir: Path, as_of: date) -> dict[str, Path]:
    return {
        next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE: (
            next_cycle.default_next_research_cycle_json_path(
                next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        binding_reports.SIGNAL_BINDING_REPORT_TYPE: (
            binding_reports.default_executable_binding_json_path(
                binding_reports.SIGNAL_BINDING_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: default_evidence_repair_json_path(
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        EVIDENCE_GAP_LEDGER_REPORT_TYPE: default_evidence_repair_json_path(
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
    }


def _signal_blocker_rows(
    *,
    signal_review: Mapping[str, Any],
    signal_binding: Mapping[str, Any],
    repair_plan: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> list[dict[str, Any]]:
    rows = []
    for check in _records(signal_review.get("signal_quality_checks")):
        if _text(check.get("status")) not in {"BLOCKING", "FAIL", "FAILED", "ERROR"}:
            continue
        check_id = _text(check.get("check_id"), "unknown_signal_check")
        cause = _signal_blocker_cause(check_id)
        field_values = _signal_failed_field_values(
            check_id=check_id,
            check=check,
            signal_review=signal_review,
            signal_binding=signal_binding,
            repair_plan=repair_plan,
        )
        rules_relaxed = check.get("signal_completeness_rules_relaxed") is True
        invalid_assumption = cause == "invalid_candidate_assumptions"
        rows.append(
            {
                "blocker_id": f"signal_robustness_{check_id}",
                "blocker_cause": cause,
                "source_check_id": check_id,
                "exact_input_artifact": str(
                    paths[next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE]
                ),
                "supporting_input_artifacts": _signal_supporting_artifacts(
                    cause,
                    paths,
                ),
                "failed_field": field_values["failed_field"],
                "expected_value": field_values["expected_value"],
                "actual_value": field_values["actual_value"],
                "evidence": _text(check.get("evidence")),
                "fail_closed": check.get("fail_closed") is True,
                "signal_completeness_rules_relaxed": rules_relaxed,
                "repair_path": _signal_repair_path(cause),
                "repairable_without_rule_relaxation": (
                    not rules_relaxed and not invalid_assumption
                ),
                "requires_candidate_redesign": invalid_assumption,
                "not_repairable": rules_relaxed,
                "recommended_action": _text(check.get("recommended_action")),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _signal_non_blocking_checks(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for check in _records(payload.get("signal_quality_checks")):
        if _text(check.get("status")) in {"BLOCKING", "FAIL", "FAILED", "ERROR"}:
            continue
        rows.append(
            {
                "check_id": _text(check.get("check_id")),
                "blocker_cause": _signal_blocker_cause(_text(check.get("check_id"))),
                "status": _text(check.get("status")),
                "evidence": _text(check.get("evidence")),
                "fail_closed": check.get("fail_closed") is True,
                "signal_completeness_rules_relaxed": (
                    check.get("signal_completeness_rules_relaxed") is True
                ),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _signal_blocker_cause(check_id: str) -> str:
    mapping = {
        "missing_feature_columns": "missing_feature_columns",
        "stale_signal_series": "stale_signal_series",
        "schema_version_mismatch": "schema_mismatch",
        "schema_mismatch": "schema_mismatch",
        "market_coverage_gap": "partial_market_coverage",
        "empty_signal_window": "empty_signal_window",
        "partial_signal_series": "binding_fail_closed_condition",
        "invalid_candidate_assumptions": "invalid_candidate_assumptions",
    }
    return mapping.get(check_id, "binding_fail_closed_condition")


def _signal_failed_field_values(
    *,
    check_id: str,
    check: Mapping[str, Any],
    signal_review: Mapping[str, Any],
    signal_binding: Mapping[str, Any],
    repair_plan: Mapping[str, Any],
) -> dict[str, str]:
    review_summary = _mapping(signal_review.get("summary"))
    binding_summary = _mapping(signal_binding.get("summary"))
    repair_summary = _mapping(repair_plan.get("summary"))
    status = _text(check.get("status"))
    evidence = _text(check.get("evidence"))
    if check_id == "partial_signal_series":
        return {
            "failed_field": "signal_quality_checks[partial_signal_series].status",
            "expected_value": (
                "PASS; historical signal series covers frozen validation windows "
                "without static-proxy completeness gaps"
            ),
            "actual_value": (
                f"{status}; signal_row_count={review_summary.get('signal_row_count')}; "
                f"backfill_signal_completeness="
                f"{review_summary.get('backfill_signal_completeness')}"
            ),
        }
    if check_id == "stale_signal_series":
        return {
            "failed_field": "signal_quality_checks[stale_signal_series].status",
            "expected_value": (
                "PASS; signal dates are inside frozen validation windows and "
                "support historical backfill coverage"
            ),
            "actual_value": (
                f"{status}; latest_signal_date="
                f"{binding_summary.get('latest_signal_date')}; "
                f"warning_reason={binding_summary.get('warning_reason')}"
            ),
        }
    if check_id == "market_coverage_gap":
        return {
            "failed_field": "signal_quality_checks[market_coverage_gap].status",
            "expected_value": (
                "PASS; missing_data_count=0 and every required backfill window "
                "has historical signal coverage"
            ),
            "actual_value": (
                f"{status}; evidence={evidence}; "
                f"incomplete_window_count="
                f"{repair_summary.get('incomplete_window_count')}; "
                f"binding_repairable_window_count="
                f"{repair_summary.get('binding_repairable_window_count')}"
            ),
        }
    if check_id == "missing_feature_columns":
        return {
            "failed_field": "signal_quality_checks[missing_feature_columns].status",
            "expected_value": "PASS; required feature and signal columns are present",
            "actual_value": f"{status}; evidence={evidence}",
        }
    if check_id in {"schema_version_mismatch", "schema_mismatch"}:
        return {
            "failed_field": "signal_quality_checks[schema_version_mismatch].status",
            "expected_value": "PASS; schema and feature versions match binding policy",
            "actual_value": f"{status}; evidence={evidence}",
        }
    return {
        "failed_field": f"signal_quality_checks[{check_id}].status",
        "expected_value": "PASS without relaxing signal completeness rules",
        "actual_value": f"{status}; evidence={evidence}",
    }


def _signal_supporting_artifacts(
    cause: str,
    paths: Mapping[str, Path],
) -> list[str]:
    artifacts = [str(paths[binding_reports.SIGNAL_BINDING_REPORT_TYPE])]
    if cause == "partial_market_coverage":
        artifacts.append(str(paths[BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE]))
    return artifacts


def _signal_repair_path(cause: str) -> str:
    mapping = {
        "missing_feature_columns": "repair_required_feature_columns_then_recheck",
        "stale_signal_series": (
            "extend_binding_to_historical_signal_series_without_rule_relaxation"
        ),
        "schema_mismatch": "align_signal_binding_schema_with_policy_then_recheck",
        "partial_market_coverage": (
            "repair_historical_signal_coverage_for_required_backfill_windows"
        ),
        "empty_signal_window": "repair_signal_generation_inputs_before_recheck",
        "binding_fail_closed_condition": (
            "extend_binding_to_historical_signal_series_without_rule_relaxation"
        ),
        "invalid_candidate_assumptions": (
            "redesign_candidate_assumptions_before_signal_recheck"
        ),
    }
    return mapping.get(cause, "repair_signal_binding_inputs_then_recheck")


def _overall_signal_drilldown_status(
    blockers: Sequence[Mapping[str, Any]],
) -> str:
    if any(row.get("not_repairable") is True for row in blockers):
        return SIGNAL_ROBUSTNESS_NOT_REPAIRABLE
    if any(row.get("requires_candidate_redesign") is True for row in blockers):
        return SIGNAL_ROBUSTNESS_NEEDS_CANDIDATE_REDESIGN
    return SIGNAL_ROBUSTNESS_REPAIRABLE


def _signal_blocker_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "blocker_id",
        "blocker_cause",
        "source_check_id",
        "exact_input_artifact",
        "failed_field",
        "expected_value",
        "actual_value",
        "repair_path",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and isinstance(row.get("fail_closed"), bool)
        and isinstance(row.get("signal_completeness_rules_relaxed"), bool)
        and isinstance(row.get("repairable_without_rule_relaxation"), bool)
        and isinstance(row.get("requires_candidate_redesign"), bool)
        and isinstance(row.get("not_repairable"), bool)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _signal_repair_decision_consistent(payload: Mapping[str, Any]) -> bool:
    blockers = _records(payload.get("signal_blockers"))
    expected_status = _overall_signal_drilldown_status(blockers)
    summary = _mapping(payload.get("summary"))
    return (
        _text(payload.get("status")) == expected_status
        and _int(summary.get("blocker_count")) == len(blockers)
        and _int(summary.get("not_repairable_blocker_count"))
        == len([row for row in blockers if row.get("not_repairable") is True])
        and _int(summary.get("candidate_redesign_blocker_count"))
        == len([row for row in blockers if row.get("requires_candidate_redesign") is True])
    )


def _window_fragility_source_paths(*, reports_dir: Path, as_of: date) -> dict[str, Path]:
    return {
        next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE: (
            next_cycle.default_next_research_cycle_json_path(
                next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: default_evidence_repair_json_path(
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: default_evidence_repair_json_path(
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        EVIDENCE_GAP_LEDGER_REPORT_TYPE: default_evidence_repair_json_path(
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
    }


def _window_fragility_rows(
    *,
    window_payload: Mapping[str, Any],
    signal_drilldown: Mapping[str, Any],
    repair_plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_summary = _mapping(window_payload.get("summary"))
    signal_repairable = (
        _text(signal_drilldown.get("status")) == SIGNAL_ROBUSTNESS_REPAIRABLE
    )
    repair_summary = _mapping(repair_plan.get("summary"))
    rows = []
    splits_by_id = {
        _text(row.get("window_split_id")): row
        for row in _records(window_payload.get("window_splits"))
        if _text(row.get("window_split_id"))
    }
    for split_id in REQUIRED_WINDOW_SPLITS:
        split = splits_by_id.get(split_id, {"window_split_id": split_id, "status": "MISSING"})
        split_status = _text(split.get("status"), "MISSING")
        fragility_class = _window_fragility_class(split_status)
        attribution = _window_attribution_flags(
            split=split,
            source_summary=source_summary,
            signal_repairable=signal_repairable,
            repair_summary=repair_summary,
        )
        rows.append(
            {
                "window_split_id": split_id,
                "source_windows": _list_values(split.get("source_windows")),
                "source_status": split_status,
                "fragility_class": fragility_class,
                "average_return_proxy": split.get("average_return_proxy"),
                "worst_drawdown_proxy": split.get("worst_drawdown_proxy"),
                "average_turnover_proxy": split.get("average_turnover_proxy"),
                "false_flip_proxy": split.get("false_flip_proxy"),
                "rotation_proxy": split.get("rotation_proxy"),
                "evaluation": _text(split.get("evaluation")),
                "recommended_action": _text(split.get("recommended_action")),
                "regime_dependence": attribution["regime_dependence"],
                "overfit_threshold": attribution["overfit_threshold"],
                "signal_instability": attribution["signal_instability"],
                "turnover_concentration": attribution["turnover_concentration"],
                "drawdown_behavior": attribution["drawdown_behavior"],
                "benchmark_relative_weakness": attribution[
                    "benchmark_relative_weakness"
                ],
                "cost_sensitivity": attribution["cost_sensitivity"],
                "primary_failure_mode": _window_primary_failure_mode(
                    split_status,
                    attribution,
                ),
                "acceptable_for_current_research": fragility_class == "stable",
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _window_fragility_class(status: str) -> str:
    if status in {"PASS", "OK", "STABLE", "READY"}:
        return "stable"
    if status == "PARTIAL_STATIC_PROXY":
        return "under_observed"
    return "fragile"


def _window_attribution_flags(
    *,
    split: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    signal_repairable: bool,
    repair_summary: Mapping[str, Any],
) -> dict[str, str]:
    split_id = _text(split.get("window_split_id"))
    split_status = _text(split.get("status"))
    source_windows = set(_list_values(split.get("source_windows")))
    evaluation = _text(split.get("evaluation")).lower()
    stress_windows = {
        "rapid_drawdown",
        "slow_drawdown",
        "ai_semiconductor_correction",
    }
    return {
        "regime_dependence": (
            "attributed"
            if source_windows & stress_windows
            or split_id in {"stress_heavy_window", "recent_window"}
            else "not_primary"
        ),
        "overfit_threshold": (
            "attributed"
            if _text(source_summary.get("overfit_risk")) == "HIGH"
            else "not_primary"
        ),
        "signal_instability": (
            "under_observed_dynamic_binding_repairable"
            if split_status == "PARTIAL_STATIC_PROXY" or signal_repairable
            else "not_primary"
        ),
        "turnover_concentration": (
            "not_attributed_turnover_dispersion_zero"
            if _float(source_summary.get("turnover_dispersion")) == 0.0
            else "attributed"
        ),
        "drawdown_behavior": (
            "attributed"
            if split_status == "WEAK" or "drawdown" in evaluation
            else "not_primary"
        ),
        "benchmark_relative_weakness": "not_isolated_by_window_sensitivity_source",
        "cost_sensitivity": (
            "not_window_specific_turnover_constant"
            if _float(source_summary.get("turnover_dispersion")) == 0.0
            else "potential_cost_sensitivity"
        ),
        "backfill_repair_context": _text(
            repair_summary.get("repair_plan_status"),
            _text(repair_summary.get("source_backfill_status")),
        ),
    }


def _window_primary_failure_mode(
    status: str,
    attribution: Mapping[str, str],
) -> str:
    if attribution.get("drawdown_behavior") == "attributed":
        return "drawdown_behavior_failure"
    if attribution.get("signal_instability") == "under_observed_dynamic_binding_repairable":
        return "under_observed_static_proxy"
    if attribution.get("overfit_threshold") == "attributed":
        return "overfit_threshold_risk"
    if status in {"PASS", "OK", "STABLE", "READY"}:
        return "none"
    return "window_instability"


def _window_failure_modes(
    rows: Sequence[Mapping[str, Any]],
    source_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    modes: dict[str, set[str]] = {}
    for row in rows:
        mode = _text(row.get("primary_failure_mode"))
        if not mode or mode == "none":
            continue
        modes.setdefault(mode, set()).add(_text(row.get("window_split_id")))
    if _text(source_summary.get("overfit_risk")) == "HIGH":
        modes.setdefault("high_overfit_risk", set()).update(
            _text(row.get("window_split_id")) for row in rows
        )
    result = []
    for mode, split_ids in sorted(modes.items()):
        result.append(
            {
                "failure_mode_id": mode,
                "affected_window_splits": sorted(split_ids),
                "affected_split_count": len(split_ids),
                "interpretation": _window_failure_interpretation(mode),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return result


def _window_failure_interpretation(mode: str) -> str:
    mapping = {
        "drawdown_behavior_failure": (
            "Weak recent or stress-heavy splits show drawdown behavior that "
            "cannot be treated as merely under-observed."
        ),
        "under_observed_static_proxy": (
            "Split metrics exist but rely on partial static binding evidence."
        ),
        "overfit_threshold_risk": "Window sensitivity source reports high overfit risk.",
        "high_overfit_risk": (
            "High overfit risk applies across the current window sensitivity review."
        ),
    }
    return mapping.get(mode, "Window instability requires further attribution.")


def _window_fragility_judgment(
    rows: Sequence[Mapping[str, Any]],
    source_summary: Mapping[str, Any],
) -> str:
    has_fragile = any(_text(row.get("fragility_class")) == "fragile" for row in rows)
    has_under_observed = any(
        _text(row.get("fragility_class")) == "under_observed" for row in rows
    )
    high_overfit = _text(source_summary.get("overfit_risk")) == "HIGH"
    if (has_fragile or high_overfit) and has_under_observed:
        return "MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED"
    if has_fragile or high_overfit:
        return "OVERFIT_RISK"
    if has_under_observed:
        return "UNDER_OBSERVED"
    return "ACCEPTABLE_FOR_FURTHER_RESEARCH"


def _window_attribution_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "window_split_id",
        "source_status",
        "fragility_class",
        "regime_dependence",
        "overfit_threshold",
        "signal_instability",
        "turnover_concentration",
        "drawdown_behavior",
        "benchmark_relative_weakness",
        "cost_sensitivity",
        "primary_failure_mode",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _window_counts_consistent(payload: Mapping[str, Any]) -> bool:
    rows = _records(payload.get("window_attributions"))
    summary = _mapping(payload.get("summary"))
    fragile = len([row for row in rows if row.get("fragility_class") == "fragile"])
    stable = len([row for row in rows if row.get("fragility_class") == "stable"])
    under_observed = len(
        [row for row in rows if row.get("fragility_class") == "under_observed"]
    )
    return (
        _int(summary.get("fragile_window_count")) == fragile
        and _int(summary.get("stable_window_count")) == stable
        and _int(summary.get("under_observed_window_count")) == under_observed
    )


def _stress_attribution_source_paths(*, reports_dir: Path, as_of: date) -> dict[str, Path]:
    return {
        next_cycle.STRESS_REVIEW_REPORT_TYPE: (
            next_cycle.default_next_research_cycle_json_path(
                next_cycle.STRESS_REVIEW_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE: default_evidence_repair_json_path(
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: default_evidence_repair_json_path(
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: default_evidence_repair_json_path(
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        EVIDENCE_GAP_LEDGER_REPORT_TYPE: default_evidence_repair_json_path(
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
    }


def _stress_scenario_attribution_rows(
    stress_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_by_id = {
        _text(row.get("scenario_id")): row
        for row in _records(stress_payload.get("scenario_reviews"))
        if _text(row.get("scenario_id"))
    }
    rows = []
    for scenario_id in REQUIRED_STRESS_SCENARIOS:
        source = source_by_id.get(scenario_id)
        if source is None:
            rows.append(_missing_stress_scenario_row(scenario_id))
            continue
        rows.append(_stress_scenario_row(source))
    return rows


def _missing_stress_scenario_row(scenario_id: str) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "scenario_status": "MISSING",
        "candidate_behavior": "not_reported_by_source_stress_review",
        "expected_behavior": "scenario row present or explicitly marked not applicable",
        "benchmark_behavior": "not_isolated_by_stress_source",
        "drawdown_mismatch": "not_measured",
        "rotation_flip_issue": "not_measured",
        "turnover_impact": "not_measured",
        "root_cause_category": "required_stress_scenario_missing",
        "candidate_design_implication": "complete_required_stress_scenario_coverage",
        "redesign_required": False,
        "reject_candidate": False,
        "recommended_action": "add_missing_required_stress_scenario_before_claiming_coverage",
        "production_effect": PRODUCTION_EFFECT,
    }


def _stress_scenario_row(source: Mapping[str, Any]) -> dict[str, Any]:
    scenario_id = _text(source.get("scenario_id"))
    status = _text(source.get("scenario_status"), "MISSING")
    evaluation = _text(source.get("evaluation"))
    drawdown_mismatch = _stress_drawdown_mismatch(source)
    root_cause = _stress_root_cause_category(source)
    return {
        "scenario_id": scenario_id,
        "scenario_status": status,
        "candidate_behavior": (
            f"return_proxy={source.get('return_proxy')}; "
            f"drawdown_proxy={source.get('drawdown_proxy')}; "
            f"evaluation={evaluation}"
        ),
        "expected_behavior": (
            "PASS or non-blocking mixed stress evidence without partial static proxy reliance"
        ),
        "benchmark_behavior": "not_isolated_by_stress_source",
        "drawdown_mismatch": drawdown_mismatch,
        "rotation_flip_issue": (
            f"rotation_count={source.get('rotation_count')}; "
            f"false_risk_off_count={source.get('false_risk_off_count')}"
        ),
        "turnover_impact": (
            f"turnover_proxy={source.get('turnover_proxy')}; "
            "cost impact evaluated separately in TRADING-476"
        ),
        "root_cause_category": root_cause,
        "candidate_design_implication": _stress_design_implication(status, root_cause),
        "redesign_required": status == "FAIL",
        "reject_candidate": False,
        "recommended_action": _text(source.get("recommended_action")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _stress_drawdown_mismatch(source: Mapping[str, Any]) -> str:
    status = _text(source.get("scenario_status"))
    evaluation = _text(source.get("evaluation")).lower()
    if status == "FAIL" or "drawdown proxy breaches" in evaluation:
        return "blocking_drawdown_mismatch"
    if "drawdown proxy remains weak" in evaluation:
        return "warning_drawdown_mismatch"
    return "none_reported"


def _stress_root_cause_category(source: Mapping[str, Any]) -> str:
    status = _text(source.get("scenario_status"))
    evaluation = _text(source.get("evaluation")).lower()
    if status == "FAIL" or "drawdown proxy breaches" in evaluation:
        return "structural_drawdown_failure"
    if "partial" in evaluation or "binding lacks historical signals" in evaluation:
        return "partial_static_proxy_evidence_limit"
    if "return/drawdown proxy remains weak" in evaluation:
        return "weak_return_drawdown_warning"
    return "stress_warning"


def _stress_design_implication(status: str, root_cause: str) -> str:
    if status == "FAIL" or root_cause == "structural_drawdown_failure":
        return "redesign_drawdown_or_stress_handling_before_research_gate"
    if root_cause == "partial_static_proxy_evidence_limit":
        return "complete_dynamic_binding_before_final_stress_judgment"
    if root_cause == "required_stress_scenario_missing":
        return "complete_required_stress_scenario_coverage"
    return "retain_as_warning_for_redesign_hypothesis"


def _stress_root_causes(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_cause: dict[str, list[str]] = {}
    for row in rows:
        cause = _text(row.get("root_cause_category"))
        if not cause:
            continue
        by_cause.setdefault(cause, []).append(_text(row.get("scenario_id")))
    if _text(summary.get("source_backfill_status")) == next_cycle.CANDIDATE_BACKFILL_PARTIAL:
        by_cause.setdefault("partial_static_proxy_evidence_limit", [])
    result = []
    for cause, scenarios in sorted(by_cause.items()):
        result.append(
            {
                "root_cause_id": cause,
                "affected_scenarios": sorted(set(scenarios)),
                "affected_scenario_count": len(set(scenarios)),
                "design_implication": _stress_design_implication("", cause),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return result


def _stress_design_judgment(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    if any(row.get("reject_candidate") is True for row in rows):
        return "REJECT_CURRENT_CANDIDATE"
    if any(row.get("redesign_required") is True for row in rows):
        return "REDESIGN_REQUIRED"
    if _text(summary.get("source_backfill_status")) == next_cycle.CANDIDATE_BACKFILL_PARTIAL:
        return "REPAIR_EVIDENCE_BEFORE_DECISION"
    return "STRESS_WEAKNESS_ACCEPTABLE"


def _stress_design_implications(
    design_judgment: str,
    root_causes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "design_judgment": design_judgment,
            "root_cause_id": _text(row.get("root_cause_id")),
            "implication": _text(row.get("design_implication")),
            "production_effect": PRODUCTION_EFFECT,
        }
        for row in root_causes
    ]


def _stress_scenario_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "scenario_id",
        "scenario_status",
        "candidate_behavior",
        "expected_behavior",
        "benchmark_behavior",
        "drawdown_mismatch",
        "rotation_flip_issue",
        "turnover_impact",
        "root_cause_category",
        "candidate_design_implication",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and isinstance(row.get("redesign_required"), bool)
        and isinstance(row.get("reject_candidate"), bool)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _stress_counts_consistent(payload: Mapping[str, Any]) -> bool:
    rows = _records(payload.get("stress_scenario_attributions"))
    summary = _mapping(payload.get("summary"))
    failed = len(
        [row for row in rows if _text(row.get("scenario_status")) in {"FAIL", "MISSING"}]
    )
    warnings = len([row for row in rows if _text(row.get("scenario_status")) == "WARNING"])
    return (
        _int(summary.get("failed_scenario_count")) == failed
        and _int(summary.get("warning_scenario_count")) == warnings
        and _int(summary.get("required_scenario_count")) == len(REQUIRED_STRESS_SCENARIOS)
    )


def _cost_benchmark_attribution_source_paths(
    *,
    reports_dir: Path,
    as_of: date,
) -> dict[str, Path]:
    cost_review_path = next_cycle.default_next_research_cycle_json_path(
        next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    cost_payload = _read_json_mapping(cost_review_path)
    input_artifacts = _mapping(cost_payload.get("input_artifacts"))
    return {
        next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE: cost_review_path,
        "cost_sensitivity_framework": _resolve_source_artifact_path(
            input_artifacts.get("cost_sensitivity_framework")
        ),
        "benchmark_baseline_control": _resolve_source_artifact_path(
            input_artifacts.get("benchmark_baseline_control")
        ),
        STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE: default_evidence_repair_json_path(
            STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE: default_evidence_repair_json_path(
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: default_evidence_repair_json_path(
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        EVIDENCE_GAP_LEDGER_REPORT_TYPE: default_evidence_repair_json_path(
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
    }


def _resolve_source_artifact_path(value: Any) -> Path:
    raw = _text(value)
    if not raw:
        raise ValueError("Required source artifact path is missing.")
    path = Path(raw)
    candidates = [path] if path.is_absolute() else [PROJECT_ROOT / path, path]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(raw)


def _cost_benchmark_source_artifact(
    source_key: str,
    source_path: Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"), source_key)
    row = _source_artifact(report_type, source_path, payload)
    row["source_key"] = source_key
    return row


def _cost_weakness_rows(
    cost_payload: Mapping[str, Any],
    cost_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    review_by_id = {
        _text(row.get("scenario_id")): row
        for row in _records(cost_payload.get("cost_scenario_reviews"))
        if _text(row.get("scenario_id"))
    }
    source_by_id = {
        _text(row.get("scenario_id")): row
        for row in _records(cost_source.get("scenario_results"))
        if _text(row.get("scenario_id"))
    }
    return [
        _cost_weakness_row(
            scenario_id,
            review_by_id.get(scenario_id),
            source_by_id.get(scenario_id),
        )
        for scenario_id in REQUIRED_COST_SCENARIOS
    ]


def _cost_weakness_row(
    scenario_id: str,
    review: Mapping[str, Any] | None,
    source: Mapping[str, Any] | None,
) -> dict[str, Any]:
    review_row = _mapping(review)
    source_row = _mapping(source)
    reason = _cost_weakness_reason(review_row, source_row)
    return {
        "scenario_id": scenario_id,
        "scenario_status": _text(review_row.get("cost_survival_status"), "MISSING"),
        "framework_classification": _text(source_row.get("classification"), "MISSING"),
        "turnover_proxy": review_row.get("turnover_proxy"),
        "source_turnover": source_row.get("turnover"),
        "gross_return_proxy": review_row.get("gross_return_proxy"),
        "gross_improvement_proxy": source_row.get("gross_improvement_proxy"),
        "cost_drag": review_row.get("cost_drag", source_row.get("cost_drag")),
        "net_proxy_result": review_row.get("net_proxy_result"),
        "net_improvement_proxy": source_row.get("net_improvement_proxy"),
        "meaningful_threshold": review_row.get(
            "meaningful_threshold",
            source_row.get("meaningful_improvement_threshold"),
        ),
        "cost_weakness_reason": reason,
        "high_turnover_assessment": _cost_turnover_assessment(review_row, source_row),
        "cost_drag_assessment": _cost_drag_assessment(review_row, source_row),
        "gross_return_assessment": _cost_gross_assessment(source_row),
        "net_return_assessment": _cost_net_assessment(source_row),
        "defensive_benefit_assessment": "not_isolated_by_cost_scenario_source",
        "recovery_behavior_assessment": "not_isolated_by_cost_scenario_source",
        "fixable_by_candidate_redesign": reason
        in {
            "weak_gross_return_proxy",
            "weak_net_return_proxy",
            "cost_drag_secondary_to_weak_edge",
        },
        "recommended_action": _cost_row_recommended_action(reason),
        "production_effect": PRODUCTION_EFFECT,
    }


def _cost_weakness_reason(
    review: Mapping[str, Any],
    source: Mapping[str, Any],
) -> str:
    status = _text(review.get("cost_survival_status"))
    classification = _text(source.get("classification"))
    threshold = _float(
        source.get("meaningful_improvement_threshold"),
        _float(review.get("meaningful_threshold")),
    )
    gross = _float(source.get("gross_improvement_proxy"), threshold)
    net = _float(source.get("net_improvement_proxy"), threshold)
    if not status and not classification:
        return "missing_cost_scenario"
    if net < threshold or classification == "NOT_MEANINGFUL":
        return "weak_net_return_proxy"
    if gross < threshold:
        return "weak_gross_return_proxy"
    if status in {"COST_SURVIVAL_FAIL", "COST_SURVIVAL_WARNING"}:
        return "cost_drag_secondary_to_weak_edge"
    return "none"


def _cost_turnover_assessment(
    review: Mapping[str, Any],
    source: Mapping[str, Any],
) -> str:
    if review.get("turnover_proxy") is not None:
        return f"turnover_proxy={review.get('turnover_proxy')}"
    if source.get("turnover") is not None:
        return f"source_turnover={source.get('turnover')}"
    return "not_reported"


def _cost_drag_assessment(
    review: Mapping[str, Any],
    source: Mapping[str, Any],
) -> str:
    if review.get("cost_drag") is not None:
        return f"cost_drag={review.get('cost_drag')}"
    if source.get("cost_drag") is not None:
        return f"source_cost_drag={source.get('cost_drag')}"
    return "not_reported"


def _cost_gross_assessment(source: Mapping[str, Any]) -> str:
    threshold = source.get("meaningful_improvement_threshold")
    gross = source.get("gross_improvement_proxy")
    if gross is None:
        return "not_reported"
    return f"gross_improvement_proxy={gross}; meaningful_threshold={threshold}"


def _cost_net_assessment(source: Mapping[str, Any]) -> str:
    threshold = source.get("meaningful_improvement_threshold")
    net = source.get("net_improvement_proxy")
    if net is None:
        return "not_reported"
    return f"net_improvement_proxy={net}; meaningful_threshold={threshold}"


def _cost_row_recommended_action(reason: str) -> str:
    if reason in {"weak_gross_return_proxy", "weak_net_return_proxy"}:
        return "redesign_candidate_for_larger_net_edge_before_benchmark_review"
    if reason == "cost_drag_secondary_to_weak_edge":
        return "redesign_candidate_to_reduce_turnover_or_increase_gross_edge"
    if reason == "missing_cost_scenario":
        return "restore_required_cost_scenario_source_row"
    return "retain_cost_scenario_evidence"


def _benchmark_weakness_rows(
    cost_payload: Mapping[str, Any],
    benchmark_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    review_by_id = {
        _text(row.get("baseline_id")): row
        for row in _records(cost_payload.get("benchmark_reviews"))
        if _text(row.get("baseline_id"))
    }
    source_by_id = {
        _text(row.get("baseline_id")): row
        for row in _records(benchmark_source.get("baselines"))
        if _text(row.get("baseline_id"))
    }
    return [
        _benchmark_weakness_row(
            baseline_id,
            review_by_id.get(baseline_id),
            source_by_id.get(baseline_id),
        )
        for baseline_id in REQUIRED_BENCHMARK_BASELINES
    ]


def _benchmark_weakness_row(
    baseline_id: str,
    review: Mapping[str, Any] | None,
    source: Mapping[str, Any] | None,
) -> dict[str, Any]:
    review_row = _mapping(review)
    source_row = _mapping(source)
    reason = _benchmark_weakness_reason(review_row)
    return {
        "baseline_id": baseline_id,
        "baseline_status": _text(review_row.get("benchmark_relative_status"), "MISSING"),
        "source_comparison_classification": _text(
            source_row.get("comparison_classification"),
            "MISSING",
        ),
        "candidate_return_proxy": review_row.get("candidate_return_proxy"),
        "baseline_return_proxy": review_row.get("baseline_return_proxy"),
        "candidate_delta_vs_baseline": review_row.get("candidate_delta_vs_baseline"),
        "minimum_outperformance_threshold": review_row.get(
            "minimum_outperformance_threshold",
            source_row.get("minimum_outperformance_threshold"),
        ),
        "benchmark_weakness_reason": reason,
        "defensive_benefit_assessment": _benchmark_defensive_assessment(
            baseline_id,
            reason,
        ),
        "recovery_behavior_assessment": "not_isolated_by_benchmark_source",
        "fixable_by_candidate_redesign": reason
        in {"benchmark_underperformance", "insufficient_outperformance_margin"},
        "recommended_action": _benchmark_row_recommended_action(reason),
        "production_effect": PRODUCTION_EFFECT,
    }


def _benchmark_weakness_reason(review: Mapping[str, Any]) -> str:
    status = _text(review.get("benchmark_relative_status"))
    if not status:
        return "missing_benchmark_coverage"
    if status == "BENCHMARK_UNDERPERFORMS":
        return "benchmark_underperformance"
    if status == "BENCHMARK_MIXED":
        return "insufficient_outperformance_margin"
    if status == "UNTESTED":
        return "missing_benchmark_coverage"
    return "none"


def _benchmark_defensive_assessment(baseline_id: str, reason: str) -> str:
    if reason == "none":
        return "baseline_margin_cleared"
    if baseline_id in {"spy_only", "equal_weight_etf", "static_allocation"}:
        return "insufficient_defensive_or_diversified_benchmark_margin"
    if baseline_id == "no_trade":
        return "insufficient_incremental_benefit_vs_no_trade"
    return "insufficient_growth_benchmark_margin"


def _benchmark_row_recommended_action(reason: str) -> str:
    if reason == "benchmark_underperformance":
        return "redesign_candidate_for_benchmark_relative_strength"
    if reason == "insufficient_outperformance_margin":
        return "increase_edge_before_research_gate_or_reclassify_as_warning"
    if reason == "missing_benchmark_coverage":
        return "restore_required_benchmark_baseline_row"
    return "retain_benchmark_evidence"


def _cost_benchmark_root_causes(
    cost_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    cost_source: Mapping[str, Any],
    benchmark_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_cause: dict[str, set[str]] = {}
    if any(_text(row.get("gross_return_assessment")) != "not_reported" for row in cost_rows):
        if _text(cost_source.get("cost_sensitivity_status")) == "NOT_MEANINGFUL_UNDER_COSTS":
            by_cause.setdefault("weak_gross_return_proxy", set()).update(
                _text(row.get("scenario_id")) for row in cost_rows
            )
    if any(_text(row.get("cost_weakness_reason")) == "weak_net_return_proxy" for row in cost_rows):
        by_cause.setdefault("weak_net_return_proxy", set()).update(
            _text(row.get("scenario_id"))
            for row in cost_rows
            if _text(row.get("cost_weakness_reason")) == "weak_net_return_proxy"
        )
    if any(_text(row.get("cost_drag_assessment")) != "cost_drag=0.0" for row in cost_rows):
        by_cause.setdefault("turnover_cost_exposure", set()).update(
            _text(row.get("scenario_id"))
            for row in cost_rows
            if _text(row.get("cost_drag_assessment")) not in {"cost_drag=0.0", "not_reported"}
        )
    by_cause.setdefault("partial_static_proxy_distortion", set())
    if _text(summary.get("source_backfill_status")) != next_cycle.CANDIDATE_BACKFILL_PARTIAL:
        by_cause.pop("partial_static_proxy_distortion", None)
    if any(
        _text(row.get("benchmark_weakness_reason")) == "benchmark_underperformance"
        for row in benchmark_rows
    ):
        by_cause.setdefault("benchmark_underperformance", set()).update(
            _text(row.get("baseline_id"))
            for row in benchmark_rows
            if _text(row.get("benchmark_weakness_reason")) == "benchmark_underperformance"
        )
    if any(
        _text(row.get("benchmark_weakness_reason"))
        == "insufficient_outperformance_margin"
        for row in benchmark_rows
    ):
        by_cause.setdefault("insufficient_benchmark_outperformance", set()).update(
            _text(row.get("baseline_id"))
            for row in benchmark_rows
            if _text(row.get("benchmark_weakness_reason"))
            == "insufficient_outperformance_margin"
        )
    if not bool(benchmark_source.get("required_baselines_present", True)):
        by_cause.setdefault("missing_benchmark_coverage", set()).update(
            _list_values(benchmark_source.get("missing_required_baselines"))
        )
    return [
        {
            "root_cause_id": cause,
            "affected_items": sorted(item for item in items if item),
            "affected_item_count": len({item for item in items if item}),
            "design_implication": _cost_benchmark_design_implication(cause),
            "production_effect": PRODUCTION_EFFECT,
        }
        for cause, items in sorted(by_cause.items())
    ]


def _cost_benchmark_design_judgment(
    root_causes: Sequence[Mapping[str, Any]],
) -> str:
    cause_ids = {_text(row.get("root_cause_id")) for row in root_causes}
    if "missing_benchmark_coverage" in cause_ids and len(cause_ids) == 1:
        return "REPAIR_EVIDENCE_BEFORE_DECISION"
    if cause_ids & {
        "weak_gross_return_proxy",
        "weak_net_return_proxy",
        "benchmark_underperformance",
        "insufficient_benchmark_outperformance",
        "turnover_cost_exposure",
    }:
        return "REDESIGN_REQUIRED"
    if "partial_static_proxy_distortion" in cause_ids:
        return "REPAIR_EVIDENCE_BEFORE_DECISION"
    return "COST_BENCHMARK_WEAKNESS_ACCEPTABLE"


def _cost_benchmark_design_implication(cause: str) -> str:
    if cause in {"weak_gross_return_proxy", "weak_net_return_proxy"}:
        return "increase_candidate_edge_before_cost_benchmark_retest"
    if cause == "turnover_cost_exposure":
        return "reduce_turnover_or_raise_gross_edge_before_retest"
    if cause in {
        "benchmark_underperformance",
        "insufficient_benchmark_outperformance",
    }:
        return "redesign_for_benchmark_relative_strength"
    if cause == "missing_benchmark_coverage":
        return "restore_required_benchmark_coverage_before_decision"
    if cause == "partial_static_proxy_distortion":
        return "complete_dynamic_binding_before_final_benchmark_claim"
    return "retain_as_cost_benchmark_warning"


def _cost_benchmark_design_implications(
    design_judgment: str,
    root_causes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "design_judgment": design_judgment,
            "root_cause_id": _text(row.get("root_cause_id")),
            "implication": _text(row.get("design_implication")),
            "production_effect": PRODUCTION_EFFECT,
        }
        for row in root_causes
    ]


def _cost_scenario_attribution_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "scenario_id",
        "scenario_status",
        "framework_classification",
        "cost_weakness_reason",
        "high_turnover_assessment",
        "cost_drag_assessment",
        "gross_return_assessment",
        "net_return_assessment",
        "defensive_benefit_assessment",
        "recovery_behavior_assessment",
        "recommended_action",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and isinstance(row.get("fixable_by_candidate_redesign"), bool)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _benchmark_attribution_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "baseline_id",
        "baseline_status",
        "source_comparison_classification",
        "benchmark_weakness_reason",
        "defensive_benefit_assessment",
        "recovery_behavior_assessment",
        "recommended_action",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and isinstance(row.get("fixable_by_candidate_redesign"), bool)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _cost_benchmark_counts_consistent(payload: Mapping[str, Any]) -> bool:
    cost_rows = _records(payload.get("cost_scenario_attributions"))
    benchmark_rows = _records(payload.get("benchmark_baseline_attributions"))
    summary = _mapping(payload.get("summary"))
    return (
        _int(summary.get("cost_scenario_count")) == len(cost_rows)
        and _int(summary.get("benchmark_baseline_count")) == len(benchmark_rows)
        and _int(summary.get("cost_weakness_count"))
        == len([row for row in cost_rows if _text(row.get("cost_weakness_reason")) != "none"])
        and _int(summary.get("benchmark_weakness_count"))
        == len(
            [
                row
                for row in benchmark_rows
                if _text(row.get("benchmark_weakness_reason")) != "none"
            ]
        )
        and _int(summary.get("root_cause_count"))
        == len(_records(payload.get("cost_benchmark_root_causes")))
    )


def _candidate_redesign_source_paths(
    *,
    reports_dir: Path,
    as_of: date,
) -> dict[str, Path]:
    return {
        EVIDENCE_GAP_LEDGER_REPORT_TYPE: default_evidence_repair_json_path(
            EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE: default_evidence_repair_json_path(
            BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE: default_evidence_repair_json_path(
            SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE: default_evidence_repair_json_path(
            WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE: default_evidence_repair_json_path(
            STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE: (
            default_evidence_repair_json_path(
                COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
    }


def _candidate_redesign_hypotheses(
    *,
    signal_drilldown: Mapping[str, Any],
    window_attribution: Mapping[str, Any],
    stress_attribution: Mapping[str, Any],
    cost_attribution: Mapping[str, Any],
    repair_plan: Mapping[str, Any],
    ledger: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_issue_refs = _candidate_source_issue_refs(
        signal_drilldown=signal_drilldown,
        window_attribution=window_attribution,
        stress_attribution=stress_attribution,
        cost_attribution=cost_attribution,
        repair_plan=repair_plan,
        ledger=ledger,
    )
    templates = [
        {
            "hypothesis_id": "v2_dynamic_signal_repair",
            "priority": "P0",
            "target_areas": ["signal_robustness_repair", "window_stability"],
            "expected_improvement": (
                "Replace static single-point signal evidence with historical dynamic "
                "signal series so signal completeness no longer blocks robustness review."
            ),
            "changed_signal_logic": (
                "Compute candidate signal state per historical date with explicit stale "
                "signal and market-coverage fail-closed checks."
            ),
            "changed_regime_logic": (
                "Keep existing regime interpretation but require date-aligned regime "
                "inputs before emitting a signal."
            ),
            "changed_rotation_rule": (
                "No new rotation rule; only permit rotations from date-valid signal state."
            ),
            "expected_failure_mode": "historical_signal_series_still_incomplete",
            "validation_method": (
                "Rerun signal binding, mini backfill, and signal robustness review on "
                "representative windows without relaxing completeness rules."
            ),
            "stop_condition": (
                "Stop if partial_signal_series, stale_signal_series, or market_coverage_gap "
                "remains blocking."
            ),
            "source_issue_refs": source_issue_refs["signal"],
        },
        {
            "hypothesis_id": "v2_drawdown_stress_guard",
            "priority": "P0",
            "target_areas": ["stress_handling", "window_stability"],
            "expected_improvement": (
                "Reduce slow-drawdown and stress-heavy fragility by making risk-off "
                "state respond earlier to persistent drawdown pressure."
            ),
            "changed_signal_logic": (
                "Add drawdown persistence confirmation before maintaining full risk-on "
                "candidate exposure."
            ),
            "changed_regime_logic": (
                "Treat slow drawdown and AI/semiconductor correction as explicit stress "
                "sub-regimes in research-only validation."
            ),
            "changed_rotation_rule": (
                "Rotate out of vulnerable sleeve only after persistence confirmation to "
                "avoid one-day flip churn."
            ),
            "expected_failure_mode": "drawdown_guard_lags_or_overreacts_in_v_shaped_recovery",
            "validation_method": (
                "Rerun stress review on rapid/slow drawdown, V-shaped recovery, and "
                "AI/semiconductor correction windows."
            ),
            "stop_condition": (
                "Stop if slow_drawdown remains FAIL or V-shaped recovery coverage remains missing."
            ),
            "source_issue_refs": source_issue_refs["stress_window"],
        },
        {
            "hypothesis_id": "v2_turnover_cost_benchmark_guard",
            "priority": "P0",
            "target_areas": [
                "lower_turnover",
                "benchmark_relative_behavior",
                "cost_survival",
            ],
            "expected_improvement": (
                "Improve net edge and benchmark margin by reducing unnecessary rotation "
                "and requiring expected edge to clear source cost thresholds."
            ),
            "changed_signal_logic": (
                "Require stronger signal confirmation before changing sleeves when "
                "benchmark margin is already thin."
            ),
            "changed_regime_logic": (
                "Keep regime mismatch filter but add benchmark-relative confirmation "
                "before defensive switches."
            ),
            "changed_rotation_rule": (
                "Add turnover-aware hold band and minimum benefit check before any "
                "research weight rotation."
            ),
            "expected_failure_mode": "lower_turnover_reduces_reactivity_in_fast_drawdowns",
            "validation_method": (
                "Rerun cost/benchmark attribution after mini backfill; require improved "
                "net proxy and no equal-weight ETF underperformance."
            ),
            "stop_condition": (
                "Stop if weak_net_return_proxy or benchmark_underperformance persists."
            ),
            "source_issue_refs": source_issue_refs["cost_benchmark"],
        },
        {
            "hypothesis_id": "v2_false_risk_off_rotation_control",
            "priority": "P1",
            "target_areas": ["lower_turnover", "stress_handling"],
            "expected_improvement": (
                "Reduce false risk-off cluster damage by requiring confirmation before "
                "risk-off rotations in choppy markets."
            ),
            "changed_signal_logic": (
                "Add disagreement check between risk-off trigger and trend confirmation."
            ),
            "changed_regime_logic": (
                "Separate high-volatility sideways regime from true drawdown regime."
            ),
            "changed_rotation_rule": (
                "Delay risk-off rotation until confirmation unless drawdown guard is breached."
            ),
            "expected_failure_mode": "confirmation_delay_misses_rapid_drawdown",
            "validation_method": (
                "Compare false_risk_off_cluster and rapid_drawdown windows after mini backfill."
            ),
            "stop_condition": (
                "Stop if false risk-off improves only by worsening rapid_drawdown behavior."
            ),
            "source_issue_refs": source_issue_refs["stress_window"],
        },
        {
            "hypothesis_id": "v2_benchmark_relative_edge_filter",
            "priority": "P1",
            "target_areas": ["benchmark_relative_behavior", "cost_survival"],
            "expected_improvement": (
                "Avoid candidate states that do not clear static/no-trade/QQQ/SPY/equal-weight "
                "baseline margins."
            ),
            "changed_signal_logic": (
                "Gate new exposure changes when recent candidate edge is below baseline margin."
            ),
            "changed_regime_logic": (
                "Require benchmark-relative context before declaring a regime mismatch opportunity."
            ),
            "changed_rotation_rule": (
                "Hold existing research weights when candidate delta does not clear "
                "source threshold."
            ),
            "expected_failure_mode": "benchmark_gate_blocks_valid_early_recovery",
            "validation_method": (
                "Rerun benchmark baseline comparison and vs-returned comparison "
                "after mini backfill."
            ),
            "stop_condition": (
                "Stop if equal_weight_etf underperformance or all-baseline mixed margin persists."
            ),
            "source_issue_refs": source_issue_refs["cost_benchmark"],
        },
        {
            "hypothesis_id": "v2_observation_quality_gate",
            "priority": "P2",
            "target_areas": ["signal_robustness_repair", "window_stability"],
            "expected_improvement": (
                "Improve auditability by preventing under-observed static proxy windows from "
                "being treated as strategy conclusions."
            ),
            "changed_signal_logic": (
                "Emit explicit insufficient-evidence state when historical signal "
                "coverage is incomplete."
            ),
            "changed_regime_logic": (
                "Require each validation window to disclose regime coverage and observation count."
            ),
            "changed_rotation_rule": (
                "No rotation change; fail closed before rotation analysis when "
                "evidence is incomplete."
            ),
            "expected_failure_mode": "evidence_gate_blocks_too_many_windows_for_mini_backfill",
            "validation_method": (
                "Validate mini-backfill evidence coverage and Reader Brief disclosure "
                "before full backfill."
            ),
            "stop_condition": (
                "Stop if required validation windows remain under-observed after binding repair."
            ),
            "source_issue_refs": source_issue_refs["repair_evidence"],
        },
    ]
    return [
        {
            **template,
            "paper_shadow_activation_allowed": False,
            "production_effect": PRODUCTION_EFFECT,
        }
        for template in templates
    ]


def _candidate_source_issue_refs(
    *,
    signal_drilldown: Mapping[str, Any],
    window_attribution: Mapping[str, Any],
    stress_attribution: Mapping[str, Any],
    cost_attribution: Mapping[str, Any],
    repair_plan: Mapping[str, Any],
    ledger: Mapping[str, Any],
) -> dict[str, list[str]]:
    signal_refs = [
        _text(row.get("check_id"), _text(row.get("blocker_id")))
        for row in _records(signal_drilldown.get("signal_blockers"))
    ]
    window_refs = [
        _text(row.get("failure_mode_id"))
        for row in _records(window_attribution.get("failure_modes"))
    ]
    stress_refs = [
        _text(row.get("root_cause_id"))
        for row in _records(stress_attribution.get("stress_weakness_root_causes"))
    ]
    cost_refs = [
        _text(row.get("root_cause_id"))
        for row in _records(cost_attribution.get("cost_benchmark_root_causes"))
    ]
    repair_refs = [
        _text(row.get("window_id"))
        for row in _records(repair_plan.get("window_repair_diagnostics"))
    ]
    ledger_refs = [
        _text(row.get("gap_id"))
        for row in _records(ledger.get("evidence_gaps"))
    ][:8]
    return {
        "signal": sorted({item for item in signal_refs if item}),
        "stress_window": sorted({item for item in [*window_refs, *stress_refs] if item}),
        "cost_benchmark": sorted({item for item in cost_refs if item}),
        "repair_evidence": sorted({item for item in [*repair_refs, *ledger_refs] if item}),
    }


def _redesign_target_coverage(
    hypotheses: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for target in REQUIRED_REDESIGN_TARGETS:
        covering = [
            _text(row.get("hypothesis_id"))
            for row in hypotheses
            if target in _list_values(row.get("target_areas"))
        ]
        rows.append(
            {
                "target_area": target,
                "covering_hypotheses": covering,
                "covered": bool(covering),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _candidate_hypothesis_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "hypothesis_id",
        "priority",
        "target_areas",
        "expected_improvement",
        "changed_signal_logic",
        "changed_regime_logic",
        "changed_rotation_rule",
        "expected_failure_mode",
        "validation_method",
        "stop_condition",
        "source_issue_refs",
    )
    return (
        all(bool(row.get(key)) for key in required)
        and _text(row.get("priority")) in CANDIDATE_REDESIGN_PRIORITIES
        and isinstance(row.get("paper_shadow_activation_allowed"), bool)
        and row.get("paper_shadow_activation_allowed") is False
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _candidate_hypothesis_counts_consistent(payload: Mapping[str, Any]) -> bool:
    hypotheses = _records(payload.get("candidate_redesign_hypotheses"))
    summary = _mapping(payload.get("summary"))
    target_coverage = _records(payload.get("target_coverage"))
    return (
        _int(summary.get("hypothesis_count")) == len(hypotheses)
        and _int(summary.get("p0_hypothesis_count"))
        == len([row for row in hypotheses if _text(row.get("priority")) == "P0"])
        and _int(summary.get("p1_hypothesis_count"))
        == len([row for row in hypotheses if _text(row.get("priority")) == "P1"])
        and _int(summary.get("p2_hypothesis_count"))
        == len([row for row in hypotheses if _text(row.get("priority")) == "P2"])
        and _int(summary.get("target_coverage_count"))
        == len([row for row in target_coverage if row.get("covered") is True])
        and _int(summary.get("required_target_count")) == len(REQUIRED_REDESIGN_TARGETS)
    )


def _candidate_selection_boundary_valid(value: Any) -> bool:
    boundary = _mapping(value)
    return (
        boundary.get("selects_final_spec") is False
        and boundary.get("implements_binding") is False
        and boundary.get("runs_backfill") is False
        and boundary.get("paper_shadow_activation_allowed") is False
        and _text(boundary.get("production_effect")) == PRODUCTION_EFFECT
    )


def _select_candidate_v2_hypothesis(
    hypotheses: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    p0 = [dict(row) for row in hypotheses if _text(row.get("priority")) == "P0"]
    if not p0:
        raise ValueError("No P0 hypothesis available for v2 spec freeze.")
    return max(
        p0,
        key=lambda row: (
            len(_list_values(row.get("target_areas"))),
            -_int(row.get("source_order"), 0),
            _text(row.get("hypothesis_id")),
        ),
    )


def _candidate_v2_frozen_spec(selected: Mapping[str, Any]) -> dict[str, Any]:
    hypothesis_id = _text(selected.get("hypothesis_id"), "selected_p0")
    return {
        "candidate_id": f"median_plus_regime_mismatch_filter_{hypothesis_id}",
        "spec_version": "v2_research_freeze_trading_478",
        "source_hypothesis_id": hypothesis_id,
        "source_hypothesis_priority": _text(selected.get("priority")),
        "signal_inputs": [
            "historical_candidate_signal_series_by_date",
            "date_aligned_feature_matrix",
            "market_coverage_status_by_window",
            "benchmark_margin_context",
        ],
        "feature_inputs": [
            "ETF price returns",
            "AI/semiconductor regime indicators",
            "drawdown persistence metrics",
            "high-volatility sideways detector inputs",
            "cost scenario and benchmark baseline context",
        ],
        "regime_assumptions": [
            "market_regime=ai_after_chatgpt",
            (
                "rapid_drawdown, slow_drawdown, V-shaped recovery, high-volatility "
                "sideways, false risk-off cluster, and AI/semiconductor correction "
                "remain required validation contexts"
            ),
            "regime mismatch opportunity must be confirmed against benchmark-relative edge",
        ],
        "drawdown_handling": (
            "Apply persistent drawdown confirmation before maintaining full risk-on "
            "exposure; fail closed if slow_drawdown remains a stress blocker."
        ),
        "rotation_handling": _text(selected.get("changed_rotation_rule")),
        "turnover_constraints": [
            "Use turnover-aware hold band before research weight rotation.",
            "Mini backfill must show turnover proxy improvement versus TRADING-470 candidate.",
            "Do not add a numeric turnover cap until governed policy is reviewed.",
        ],
        "cost_expectations": [
            "Weak net return proxy must clear source meaningful thresholds before full backfill.",
            "High-cost scenario must not remain a root cause in TRADING-476 attribution rerun.",
        ],
        "benchmark_expectations": [
            "Equal-weight ETF baseline must no longer be BENCHMARK_UNDERPERFORMS.",
            (
                "Static allocation, no-trade, QQQ-only, and SPY-only margins must "
                "clear source threshold or be explicitly documented as residual warnings."
            ),
        ],
        "validation_windows": [
            "normal_market_regime",
            "rapid_drawdown",
            "slow_drawdown",
            "v_shaped_recovery",
            "high_volatility_sideways_market",
            "false_risk_off_cluster",
            "ai_semiconductor_correction",
        ],
        "stop_conditions": [
            _text(selected.get("stop_condition")),
            "Stop if signal robustness remains BLOCKED after v2 binding.",
            "Stop if mini-backfill is weak or remains partial for required windows.",
            "Stop if cost/benchmark attribution remains REDESIGN_REQUIRED.",
            (
                "Stop if paper-shadow, official weights, broker/order, or production "
                "mutation is requested before owner review."
            ),
        ],
        "paper_shadow_eligible": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_differences_from_trading_470(
    selected: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "difference_id": "dynamic_signal_series_required",
            "trading_470_behavior": "single-point signal binding used as static proxy",
            "v2_behavior": "historical candidate signal series required by date",
            "source_hypothesis_id": _text(selected.get("hypothesis_id")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "difference_id": "turnover_aware_rotation_guard",
            "trading_470_behavior": "rotation allowed without benchmark margin confirmation",
            "v2_behavior": _text(selected.get("changed_rotation_rule")),
            "source_hypothesis_id": _text(selected.get("hypothesis_id")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "difference_id": "cost_benchmark_precheck",
            "trading_470_behavior": "cost/benchmark weakness diagnosed after executable review",
            "v2_behavior": "cost and benchmark expectations included before binding update",
            "source_hypothesis_id": _text(selected.get("hypothesis_id")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "difference_id": "explicit_stress_validation_contexts",
            "trading_470_behavior": "V-shaped recovery scenario was missing from stress source",
            "v2_behavior": "V-shaped recovery is a required validation window before full backfill",
            "source_hypothesis_id": _text(selected.get("hypothesis_id")),
            "production_effect": PRODUCTION_EFFECT,
        },
    ]


def _candidate_v2_spec_complete(spec: Mapping[str, Any]) -> bool:
    required = (
        "candidate_id",
        "spec_version",
        "source_hypothesis_id",
        "signal_inputs",
        "feature_inputs",
        "regime_assumptions",
        "drawdown_handling",
        "rotation_handling",
        "turnover_constraints",
        "cost_expectations",
        "benchmark_expectations",
        "validation_windows",
        "stop_conditions",
    )
    return (
        all(bool(spec.get(key)) for key in required)
        and spec.get("paper_shadow_eligible") is False
        and _text(spec.get("production_effect")) == PRODUCTION_EFFECT
    )


def _candidate_v2_difference_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "difference_id",
        "trading_470_behavior",
        "v2_behavior",
        "source_hypothesis_id",
    )
    return (
        all(bool(_text(row.get(key))) for key in required)
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
    )


def _candidate_v2_freeze_boundary_valid(value: Any) -> bool:
    boundary = _mapping(value)
    return (
        boundary.get("research_only_spec") is True
        and boundary.get("paper_shadow_eligible") is False
        and boundary.get("implements_binding") is False
        and boundary.get("runs_backfill") is False
        and boundary.get("generates_official_target_weights") is False
        and _text(boundary.get("production_effect")) == PRODUCTION_EFFECT
    )


def _v2_data_quality_gate(value: Mapping[str, Any] | None) -> dict[str, Any]:
    gate = _mapping(value)
    return {
        "status": _text(gate.get("status"), "MISSING"),
        "passed": gate.get("passed") is True,
        "error_count": _int(gate.get("error_count")),
        "warning_count": _int(gate.get("warning_count")),
        "report_path": _text(gate.get("report_path")),
    }


def _candidate_v2_binding_windows(
    spec: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_windows = {
        _text(row.get("window_id")): _mapping(row)
        for row in _records(backfill_payload.get("backfill_windows"))
    }
    rows: list[dict[str, Any]] = []
    for window_id in _list_values(spec.get("validation_windows")):
        key = _text(window_id)
        source = source_windows.get(key, {})
        rows.append(
            {
                "window_id": key,
                "start": _text(source.get("start")),
                "end": _text(source.get("end")),
                "source_available": bool(source),
                "source_status": _text(source.get("backfill_window_status"), "MISSING"),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _candidate_v2_binding_blocking_reasons(
    *,
    spec_payload: Mapping[str, Any],
    spec: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    feature_path: Path,
    binding_windows: Sequence[Mapping[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if _text(spec_payload.get("status")) != CANDIDATE_V2_SPEC_FREEZE_READY:
        reasons.append("candidate_v2_spec_freeze_not_ready")
    if not _candidate_v2_spec_complete(spec):
        reasons.append("candidate_v2_spec_incomplete")
    if data_quality.get("passed") is not True:
        reasons.append("data_quality_gate_not_passed")
    if not feature_path.exists():
        reasons.append("feature_cache_missing")
    if not any(_text(row.get("start")) and _text(row.get("end")) for row in binding_windows):
        reasons.append("validation_window_dates_missing")
    return sorted(set(reasons))


def _candidate_v2_signal_rows(
    *,
    candidate_id: str,
    feature_path: Path,
    binding_windows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    import pandas as pd

    from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
    from ai_trading_system.etf_portfolio.signals import generate_signals_for_date

    features = pd.read_csv(feature_path)
    if features.empty:
        return [], [], ["feature_cache_empty"]
    features["_date"] = pd.to_datetime(features["date"], errors="coerce")
    strategy = load_etf_config_bundle().strategy
    signal_rows: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    warnings: list[str] = []
    features_without_helper_column = features.drop(columns=["_date"])
    for window in binding_windows:
        window_id = _text(window.get("window_id"))
        start = _parse_iso_date(_text(window.get("start")))
        end = _parse_iso_date(_text(window.get("end")))
        if start is None or end is None:
            coverage.append(
                {
                    "window_id": window_id,
                    "covered": False,
                    "signal_row_count": 0,
                    "reason": "source_window_dates_missing",
                    "production_effect": PRODUCTION_EFFECT,
                }
            )
            warnings.append(f"validation_context_dates_missing:{window_id}")
            continue
        date_mask = (features["_date"] >= pd.Timestamp(start)) & (
            features["_date"] <= pd.Timestamp(end)
        )
        window_dates = [
            pd.Timestamp(item).date()
            for item in sorted(features.loc[date_mask, "_date"].dropna().unique())
        ]
        row_count_before = len(signal_rows)
        skipped_missing_symbols = 0
        for signal_date in window_dates:
            selected_symbols = set(
                features.loc[
                    features["_date"] == pd.Timestamp(signal_date),
                    "symbol",
                ].astype(str)
            )
            if set(V2_REQUIRED_SIGNAL_SYMBOLS) - selected_symbols:
                skipped_missing_symbols += 1
                continue
            records = generate_signals_for_date(
                features_without_helper_column,
                strategy=strategy,
                run_date=signal_date,
            )
            by_symbol = {
                str(record.symbol): record.to_record()
                for record in records
                if str(record.symbol) in V2_REQUIRED_SIGNAL_SYMBOLS
            }
            if set(V2_REQUIRED_SIGNAL_SYMBOLS) - set(by_symbol):
                skipped_missing_symbols += 1
                continue
            signal_rows.append(
                _candidate_v2_signal_row(
                    candidate_id=candidate_id,
                    signal_date=signal_date.isoformat(),
                    window_id=window_id,
                    records_by_symbol=by_symbol,
                )
            )
        produced = len(signal_rows) - row_count_before
        if skipped_missing_symbols:
            warnings.append(f"market_coverage_gap:{window_id}")
        coverage.append(
            {
                "window_id": window_id,
                "covered": produced > 0,
                "signal_row_count": produced,
                "reason": "covered" if produced else "no_feature_dates_in_window",
                "skipped_missing_symbol_date_count": skipped_missing_symbols,
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    signal_rows.sort(key=lambda row: _text(row.get("signal_date")))
    return signal_rows, coverage, sorted(set(warnings))


def _candidate_v2_signal_row(
    *,
    candidate_id: str,
    signal_date: str,
    window_id: str,
    records_by_symbol: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    symbol_scores = {
        symbol: _float(records_by_symbol[symbol].get("composite_score"))
        for symbol in V2_REQUIRED_SIGNAL_SYMBOLS
    }
    normalized_scores = {
        symbol: round((score - 50.0) / 50.0, 6)
        for symbol, score in symbol_scores.items()
    }
    ai_scores = [normalized_scores[symbol] for symbol in V2_AI_SIGNAL_SYMBOLS]
    ai_median = sorted(ai_scores)[len(ai_scores) // 2]
    spy_signal = normalized_scores["SPY"]
    ai_vs_spy_delta = round(ai_median - spy_signal, 6)
    directions = {
        _text(records_by_symbol[symbol].get("direction")).lower()
        for symbol in V2_REQUIRED_SIGNAL_SYMBOLS
    }
    if directions and directions.issubset({"bullish"}):
        risk_state = "risk_on"
    elif "bearish" in directions:
        risk_state = "risk_off"
    else:
        risk_state = "neutral"
    if risk_state == "risk_off":
        base_rotation = "reduce_ai_risk"
    elif ai_vs_spy_delta > 0:
        base_rotation = "increase_ai_risk"
    elif ai_vs_spy_delta < 0:
        base_rotation = "reduce_ai_risk"
    else:
        base_rotation = "hold_current_research_weight"
    rotation_state, guard_reason = _candidate_v2_guarded_rotation(
        base_rotation=base_rotation,
        window_id=window_id,
    )
    confidence_values = sorted(
        {
            _text(records_by_symbol[symbol].get("confidence")).lower()
            for symbol in V2_REQUIRED_SIGNAL_SYMBOLS
            if _text(records_by_symbol[symbol].get("confidence"))
        }
    )
    return {
        "candidate_id": candidate_id,
        "binding_version": V2_SIGNAL_BINDING_VERSION,
        "signal_date": signal_date,
        "validation_window_id": window_id,
        "signal_score": round(sum(ai_scores) / len(ai_scores), 6),
        "risk_state": risk_state,
        "base_rotation_state": base_rotation,
        "rotation_state": rotation_state,
        "turnover_guard_applied": bool(guard_reason),
        "guard_reason": guard_reason,
        "component_scores": {
            "symbol_composite_scores": {
                symbol: round(score, 6) for symbol, score in symbol_scores.items()
            },
            "normalized_symbol_scores": normalized_scores,
            "ai_median_signal_score": round(ai_median, 6),
            "spy_signal_score": round(spy_signal, 6),
            "ai_vs_spy_delta": ai_vs_spy_delta,
            "directions": sorted(directions),
            "confidence_values": confidence_values,
        },
        "source_signal_records": {
            symbol: {
                "trend_score": records_by_symbol[symbol].get("trend_score"),
                "momentum_score": records_by_symbol[symbol].get("momentum_score"),
                "relative_strength_score": records_by_symbol[symbol].get(
                    "relative_strength_score"
                ),
                "risk_score": records_by_symbol[symbol].get("risk_score"),
                "composite_score": records_by_symbol[symbol].get("composite_score"),
            }
            for symbol in V2_REQUIRED_SIGNAL_SYMBOLS
        },
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": "none",
        "order_effect": "none",
    }


def _candidate_v2_guarded_rotation(
    *,
    base_rotation: str,
    window_id: str,
) -> tuple[str, str]:
    if base_rotation == "increase_ai_risk" and window_id in V2_TURNOVER_GUARD_WINDOWS:
        return "hold_current_research_weight", f"{window_id}:turnover_aware_hold_band"
    if base_rotation == "reduce_ai_risk" and window_id == "false_risk_off_cluster":
        return "hold_current_research_weight", "false_risk_off_cluster:rotation_guard"
    return base_rotation, ""


def _candidate_v2_weight_policy(path: Path) -> dict[str, Any]:
    from ai_trading_system.yaml_loader import safe_load_yaml_path

    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"Weight binding policy must be a mapping: {path}")
    return dict(raw)


def _candidate_v2_weight_rows(
    *,
    candidate_id: str,
    signal_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    universe = _list_values(policy.get("research_weight_universe"))
    previous_weights = _candidate_v2_weights_from_mapping(
        _mapping(policy.get("initial_previous_hypothetical_weight")),
        universe=universe,
    )
    previous_source = "policy_initial_previous_weight"
    rows: list[dict[str, Any]] = []
    blocking: list[str] = []
    for signal in signal_rows:
        target_weights = _candidate_v2_target_weights(
            signal=signal,
            previous_weights=previous_weights,
            policy=policy,
            universe=universe,
        )
        constraint_hit = _candidate_v2_weight_constraint_hits(target_weights, policy)
        if constraint_hit:
            blocking.extend(constraint_hit)
        rotation_delta = {
            symbol: round(
                target_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0),
                6,
            )
            for symbol in universe
        }
        turnover_proxy = round(sum(abs(value) for value in rotation_delta.values()) / 2, 6)
        signal_date = _text(signal.get("signal_date"))
        rows.append(
            {
                "candidate_id": candidate_id,
                "binding_version": V2_WEIGHT_BINDING_VERSION,
                "signal_date": signal_date,
                "validation_window_id": _text(signal.get("validation_window_id")),
                "risk_state": _text(signal.get("risk_state"), "blocked"),
                "rotation_state": _text(signal.get("rotation_state"), "blocked"),
                "guard_reason": _text(signal.get("guard_reason")),
                "hypothetical_research_weight": _candidate_v2_weight_object(
                    weights=target_weights,
                    signal_date=signal_date,
                    source="candidate_v2_turnover_guarded_rotation_profile",
                ),
                "previous_hypothetical_weight": _candidate_v2_weight_object(
                    weights=previous_weights,
                    signal_date=signal_date,
                    source=previous_source,
                ),
                "previous_weight_source": previous_source,
                "rotation_delta": rotation_delta,
                "turnover_proxy": turnover_proxy,
                "constraint_hit": constraint_hit,
                "blocking_reason": _join_reasons(constraint_hit) or None,
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": "none",
                "order_effect": "none",
            }
        )
        previous_weights = target_weights
        previous_source = "prior_v2_hypothetical_research_weight_row"
    if not rows:
        blocking.append("empty_v2_weight_binding_window")
    return rows, sorted(set(blocking))


def _candidate_v2_target_weights(
    *,
    signal: Mapping[str, Any],
    previous_weights: Mapping[str, float],
    policy: Mapping[str, Any],
    universe: Sequence[str],
) -> dict[str, float]:
    profiles = _mapping(policy.get("rotation_profiles"))
    rotation_state = _text(signal.get("rotation_state"), "blocked")
    profile = profiles.get(rotation_state)
    if _text(profile) == "previous":
        return {symbol: round(previous_weights.get(symbol, 0.0), 6) for symbol in universe}
    if not isinstance(profile, Mapping):
        profile = _mapping(profiles.get("blocked"))
    return _candidate_v2_weights_from_mapping(_mapping(profile), universe=universe)


def _candidate_v2_weights_from_mapping(
    value: Mapping[str, Any],
    *,
    universe: Sequence[str],
) -> dict[str, float]:
    return {symbol: round(_float(value.get(symbol)), 6) for symbol in universe}


def _candidate_v2_weight_constraint_hits(
    weights: Mapping[str, float],
    policy: Mapping[str, Any],
) -> list[str]:
    constraints = _mapping(policy.get("constraints"))
    min_weight = _float(constraints.get("min_weight"), 0.0)
    max_weight = _float(constraints.get("max_single_weight"), 1.0)
    total_weight = _float(constraints.get("total_weight"), 1.0)
    tolerance = _float(constraints.get("total_weight_tolerance"), 0.000001)
    hits: list[str] = []
    for symbol, weight in weights.items():
        if weight < min_weight:
            hits.append(f"{symbol}:below_min_weight")
        if weight > max_weight:
            hits.append(f"{symbol}:above_max_single_weight")
    if abs(sum(weights.values()) - total_weight) > tolerance:
        hits.append("total_weight_mismatch")
    return hits


def _candidate_v2_weight_object(
    *,
    weights: Mapping[str, float],
    signal_date: str,
    source: str,
) -> dict[str, Any]:
    return {
        "weight_type": "hypothetical_research_weight",
        "signal_date": signal_date,
        "source": source,
        "weights": {symbol: round(weight, 6) for symbol, weight in weights.items()},
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": "none",
        "order_effect": "none",
    }


def _candidate_v2_binding_warnings(
    data_quality: Mapping[str, Any],
    signal_coverage: Sequence[Mapping[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    if _int(data_quality.get("warning_count")):
        warnings.append("data_quality_gate_passed_with_warnings")
    for row in signal_coverage:
        if row.get("covered") is not True:
            warnings.append(f"validation_context_not_covered:{_text(row.get('window_id'))}")
    return sorted(set(warnings))


def _candidate_v2_binding_safety_audit(
    *,
    signal_rows: Sequence[Mapping[str, Any]],
    weight_rows: Sequence[Mapping[str, Any]],
    spec: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    blocking_reasons: Sequence[str],
    warning_reasons: Sequence[str],
) -> dict[str, Any]:
    artifact_checks = [
        {
            "check_id": "data_quality_gate_passed",
            "status": PASS_STATUS if data_quality.get("passed") is True else FAIL_STATUS,
            "evidence": _text(data_quality.get("status"), "MISSING"),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "spec_not_paper_shadow_eligible",
            "status": PASS_STATUS if spec.get("paper_shadow_eligible") is False else FAIL_STATUS,
            "evidence": f"paper_shadow_eligible={spec.get('paper_shadow_eligible')}",
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "signal_binding_research_only",
            "status": PASS_STATUS
            if signal_rows and all(_candidate_v2_signal_row_safe(row) for row in signal_rows)
            else FAIL_STATUS,
            "evidence": f"signal_rows={len(signal_rows)}",
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "weight_binding_hypothetical_only",
            "status": PASS_STATUS
            if weight_rows and all(_candidate_v2_weight_row_safe(row) for row in weight_rows)
            else FAIL_STATUS,
            "evidence": f"weight_rows={len(weight_rows)}",
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "no_execution_surface",
            "status": PASS_STATUS,
            "evidence": "paper_shadow=false; official_target_weights=false; broker_order=false",
            "production_effect": PRODUCTION_EFFECT,
        },
    ]
    failed_checks = [
        row for row in artifact_checks if _text(row.get("status")) == FAIL_STATUS
    ]
    if blocking_reasons or failed_checks:
        status = binding_reports.SAFETY_BLOCKED
    elif warning_reasons:
        status = binding_reports.SAFETY_WARNING
    else:
        status = binding_reports.SAFETY_PASS
    return {
        "safety_audit_status": status,
        "acceptable_warning": status == binding_reports.SAFETY_WARNING and not failed_checks,
        "artifact_check_count": len(artifact_checks),
        "failed_artifact_check_count": len(failed_checks),
        "warning_reason_count": len(set(warning_reasons)),
        "artifact_checks": artifact_checks,
        "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
        "warning_reasons": list(dict.fromkeys(warning_reasons)),
        "signal_binding_research_only": bool(signal_rows)
        and all(_candidate_v2_signal_row_safe(row) for row in signal_rows),
        "weight_binding_hypothetical_only": bool(weight_rows)
        and all(_candidate_v2_weight_row_safe(row) for row in weight_rows),
        "official_target_weights": False,
        "paper_shadow_activation": False,
        "owner_decision_append": False,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": "none",
        "order_effect": "none",
    }


def _candidate_v2_signal_row_safe(row: Mapping[str, Any]) -> bool:
    return (
        row.get("research_only") is True
        and row.get("manual_review_only") is True
        and row.get("official_target_weights") is False
        and row.get("not_official_target_weights") is True
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
        and _text(row.get("broker_effect")) == "none"
        and _text(row.get("order_effect")) == "none"
    )


def _candidate_v2_weight_row_safe(row: Mapping[str, Any]) -> bool:
    return (
        row.get("research_only") is True
        and row.get("manual_review_only") is True
        and row.get("official_target_weights") is False
        and row.get("not_official_target_weights") is True
        and _text(row.get("production_effect")) == PRODUCTION_EFFECT
        and _text(row.get("broker_effect")) == "none"
        and _text(row.get("order_effect")) == "none"
        and _candidate_v2_weight_object_safe(_mapping(row.get("hypothetical_research_weight")))
        and _candidate_v2_weight_object_safe(_mapping(row.get("previous_hypothetical_weight")))
    )


def _candidate_v2_weight_object_safe(value: Mapping[str, Any]) -> bool:
    return (
        value.get("research_only") is True
        and value.get("manual_review_only") is True
        and value.get("official_target_weights") is False
        and value.get("not_official_target_weights") is True
        and _text(value.get("production_effect")) == PRODUCTION_EFFECT
        and _text(value.get("broker_effect")) == "none"
        and _text(value.get("order_effect")) == "none"
    )


def _candidate_v2_context_coverage(
    signal_coverage: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [dict(row) for row in signal_coverage]


def _candidate_v2_mini_backfill_blocking_reasons(
    *,
    binding_payload: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    prices_path: Path,
) -> list[str]:
    summary = _mapping(binding_payload.get("summary"))
    reasons: list[str] = []
    if _text(binding_payload.get("report_type")) != CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE:
        reasons.append("candidate_v2_binding_report_type_mismatch")
    if _text(binding_payload.get("status")) == CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED:
        reasons.append("candidate_v2_binding_blocked")
    if summary.get("safety_audit_allows_mini_backfill") is not True:
        reasons.append("source_safety_audit_does_not_allow_mini_backfill")
    if _text(summary.get("safety_audit_status")) not in {
        binding_reports.SAFETY_PASS,
        binding_reports.SAFETY_WARNING,
    }:
        reasons.append("source_safety_audit_not_pass_or_warning")
    if data_quality.get("passed") is not True:
        reasons.append("data_quality_gate_not_passed")
    if not prices_path.exists():
        reasons.append("price_history_missing")
    if not _records(binding_payload.get("v2_candidate_signal_series")):
        reasons.append("v2_signal_series_missing")
    if not _records(binding_payload.get("v2_hypothetical_research_weight_series")):
        reasons.append("v2_weight_series_missing")
    return sorted(set(reasons))


def _candidate_v2_price_history(
    *,
    prices_path: Path,
    symbols: Sequence[str],
) -> tuple[dict[str, dict[date, float]], list[str]]:
    history: dict[str, dict[date, float]] = {symbol: {} for symbol in symbols}
    warnings: list[str] = []
    with prices_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return history, ["price_history_header_missing"]
        for row in reader:
            symbol = _price_row_symbol(row)
            if symbol not in history:
                continue
            price_date = _parse_iso_date(_text(row.get("date")))
            if price_date is None:
                continue
            price_value = _price_row_close(row)
            if price_value <= 0:
                continue
            history[symbol][price_date] = price_value
    for symbol in symbols:
        if len(history.get(symbol, {})) < 2:
            warnings.append(f"price_history_insufficient:{symbol}")
    return history, sorted(set(warnings))


def _price_row_symbol(row: Mapping[str, Any]) -> str:
    for key in ("canonical_symbol", "symbol", "ticker"):
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _price_row_close(row: Mapping[str, Any]) -> float:
    for key in ("adj_close", "close"):
        value = _float(row.get(key), -1.0)
        if value > 0:
            return value
    return -1.0


def _candidate_v2_mini_backfill_windows(
    *,
    binding_payload: Mapping[str, Any],
    price_history: Mapping[str, Mapping[date, float]],
) -> list[dict[str, Any]]:
    binding_windows = {
        _text(row.get("window_id")): row
        for row in _records(binding_payload.get("binding_windows"))
    }
    return [
        _candidate_v2_mini_window_metrics(
            window_id=window_id,
            source_window=binding_windows.get(window_id, {"window_id": window_id}),
            binding_payload=binding_payload,
            price_history=price_history,
        )
        for window_id in V2_MINI_BACKFILL_WINDOWS
    ]


def _candidate_v2_mini_window_metrics(
    *,
    window_id: str,
    source_window: Mapping[str, Any],
    binding_payload: Mapping[str, Any],
    price_history: Mapping[str, Mapping[date, float]],
) -> dict[str, Any]:
    start = _parse_iso_date(_text(source_window.get("start")))
    end = _parse_iso_date(_text(source_window.get("end")))
    signal_rows = [
        row
        for row in _records(binding_payload.get("v2_candidate_signal_series"))
        if _text(row.get("validation_window_id")) == window_id
    ]
    weight_rows = [
        row
        for row in _records(binding_payload.get("v2_hypothetical_research_weight_series"))
        if _text(row.get("validation_window_id")) == window_id
    ]
    if start is None or end is None:
        return _candidate_v2_partial_mini_window(
            window_id=window_id,
            source_window=source_window,
            signal_rows=signal_rows,
            weight_rows=weight_rows,
            missing_reason="source_window_dates_missing",
        )
    common_dates = _candidate_v2_common_price_dates(
        price_history=price_history,
        start=start,
        end=end,
    )
    daily_returns, skipped_return_count = _candidate_v2_weighted_daily_returns(
        price_history=price_history,
        dates=common_dates,
        weight_rows=weight_rows,
    )
    return_proxy, drawdown_proxy = _candidate_v2_return_and_drawdown(daily_returns)
    signal_dates = {
        parsed
        for row in signal_rows
        if (parsed := _parse_iso_date(_text(row.get("signal_date")))) is not None
    }
    signal_completeness_ratio = (
        round(len(signal_dates & set(common_dates)) / len(common_dates), 6)
        if common_dates
        else 0.0
    )
    status = "COMPLETE" if daily_returns else "PARTIAL"
    missing_reason = (
        "none"
        if status == "COMPLETE"
        else "insufficient_price_or_weight_aligned_return_observations"
    )
    turnover = round(sum(_float(row.get("turnover_proxy")) for row in weight_rows), 6)
    rotation_count = len(
        [row for row in weight_rows if _float(row.get("turnover_proxy")) > 0]
    )
    false_risk_off_count = len(
        [
            row
            for row in signal_rows
            if "false_risk_off" in _text(row.get("guard_reason"))
            or (
                window_id == "false_risk_off_cluster"
                and _text(row.get("base_rotation_state")) == "reduce_ai_risk"
            )
        ]
    )
    constraint_hit_count = sum(
        len(_list_values(row.get("constraint_hit"))) for row in weight_rows
    )
    return {
        "window_id": window_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "mini_backfill_window_status": status,
        "missing_reason": missing_reason,
        "return_proxy": return_proxy,
        "drawdown_proxy": drawdown_proxy,
        "turnover": turnover,
        "average_turnover_proxy": (
            round(turnover / len(weight_rows), 6) if weight_rows else None
        ),
        "rotation_count": rotation_count,
        "false_risk_off_count": false_risk_off_count,
        "constraint_hit_count": constraint_hit_count,
        "signal_completeness": "COMPLETE" if signal_completeness_ratio >= 1.0 else "PARTIAL",
        "signal_completeness_ratio": signal_completeness_ratio,
        "price_observation_count": len(common_dates) * len(V2_PRICE_SYMBOLS),
        "return_observation_count": len(daily_returns),
        "skipped_return_observation_count": skipped_return_count,
        "weight_row_count": len(weight_rows),
        "signal_row_count": len(signal_rows),
        "cost_proxy_inputs": _candidate_v2_window_cost_proxy_inputs(
            window_id=window_id,
            return_proxy=return_proxy,
            turnover=turnover,
            return_observation_count=len(daily_returns),
            missing_reason=missing_reason,
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_partial_mini_window(
    *,
    window_id: str,
    source_window: Mapping[str, Any],
    signal_rows: Sequence[Mapping[str, Any]],
    weight_rows: Sequence[Mapping[str, Any]],
    missing_reason: str,
) -> dict[str, Any]:
    return {
        "window_id": window_id,
        "start": _text(source_window.get("start")),
        "end": _text(source_window.get("end")),
        "mini_backfill_window_status": "PARTIAL",
        "missing_reason": missing_reason,
        "return_proxy": None,
        "drawdown_proxy": None,
        "turnover": None,
        "average_turnover_proxy": None,
        "rotation_count": 0,
        "false_risk_off_count": 0,
        "constraint_hit_count": 0,
        "signal_completeness": "PARTIAL",
        "signal_completeness_ratio": 0.0,
        "price_observation_count": 0,
        "return_observation_count": 0,
        "skipped_return_observation_count": 0,
        "weight_row_count": len(weight_rows),
        "signal_row_count": len(signal_rows),
        "cost_proxy_inputs": _candidate_v2_window_cost_proxy_inputs(
            window_id=window_id,
            return_proxy=None,
            turnover=None,
            return_observation_count=0,
            missing_reason=missing_reason,
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_common_price_dates(
    *,
    price_history: Mapping[str, Mapping[date, float]],
    start: date,
    end: date,
) -> list[date]:
    symbol_dates = []
    for symbol in V2_PRICE_SYMBOLS:
        dates = {
            item
            for item in price_history.get(symbol, {})
            if start <= item <= end
        }
        symbol_dates.append(dates)
    if not symbol_dates:
        return []
    return sorted(set.intersection(*symbol_dates))


def _candidate_v2_weighted_daily_returns(
    *,
    price_history: Mapping[str, Mapping[date, float]],
    dates: Sequence[date],
    weight_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[float], int]:
    returns: list[float] = []
    skipped = 0
    for previous_date, current_date in zip(dates, dates[1:], strict=False):
        weight_row = _candidate_v2_weight_for_date(weight_rows, previous_date)
        if not weight_row:
            skipped += 1
            continue
        weights = _candidate_v2_weight_values(weight_row)
        if not weights:
            skipped += 1
            continue
        daily_return = 0.0
        missing_symbol = False
        for symbol in V2_PRICE_SYMBOLS:
            previous_price = price_history.get(symbol, {}).get(previous_date)
            current_price = price_history.get(symbol, {}).get(current_date)
            if not previous_price or not current_price:
                missing_symbol = True
                break
            daily_return += weights.get(symbol, 0.0) * (
                (current_price / previous_price) - 1.0
            )
        if missing_symbol:
            skipped += 1
            continue
        returns.append(round(daily_return, 10))
    return returns, skipped


def _candidate_v2_weight_for_date(
    weight_rows: Sequence[Mapping[str, Any]],
    target_date: date,
) -> dict[str, Any]:
    dated_rows: list[tuple[date, Mapping[str, Any]]] = []
    for row in weight_rows:
        signal_date = _parse_iso_date(_text(row.get("signal_date")))
        if signal_date is None or signal_date > target_date:
            continue
        dated_rows.append((signal_date, row))
    if not dated_rows:
        return {}
    return dict(max(dated_rows, key=lambda item: item[0])[1])


def _candidate_v2_weight_values(row: Mapping[str, Any]) -> dict[str, float]:
    weight_object = _mapping(row.get("hypothetical_research_weight"))
    weights = _mapping(weight_object.get("weights"))
    return {symbol: _float(weights.get(symbol)) for symbol in (*V2_PRICE_SYMBOLS, "CASH")}


def _candidate_v2_return_and_drawdown(
    daily_returns: Sequence[float],
) -> tuple[float | None, float | None]:
    if not daily_returns:
        return None, None
    value = 1.0
    peak = 1.0
    worst_drawdown = 0.0
    for daily_return in daily_returns:
        value *= 1.0 + daily_return
        peak = max(peak, value)
        if peak > 0:
            worst_drawdown = min(worst_drawdown, (value / peak) - 1.0)
    return round(value - 1.0, 6), round(worst_drawdown, 6)


def _candidate_v2_mini_aggregate(
    mini_windows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return_values = [
        _float(row.get("return_proxy"))
        for row in mini_windows
        if row.get("return_proxy") is not None
    ]
    drawdown_values = [
        _float(row.get("drawdown_proxy"))
        for row in mini_windows
        if row.get("drawdown_proxy") is not None
    ]
    turnover_values = [
        _float(row.get("turnover"))
        for row in mini_windows
        if row.get("turnover") is not None
    ]
    price_observation_count = sum(_int(row.get("price_observation_count")) for row in mini_windows)
    return_observation_count = sum(
        _int(row.get("return_observation_count")) for row in mini_windows
    )
    signal_completeness_weight = sum(
        _float(row.get("signal_completeness_ratio"))
        * max(_int(row.get("price_observation_count")), 1)
        for row in mini_windows
    )
    signal_completeness_ratio = (
        round(signal_completeness_weight / price_observation_count, 6)
        if price_observation_count
        else 0.0
    )
    return {
        "aggregate_return_proxy": (
            round(sum(return_values) / len(return_values), 6) if return_values else None
        ),
        "aggregate_drawdown_proxy": min(drawdown_values) if drawdown_values else None,
        "turnover_proxy": (
            round(sum(turnover_values) / len(turnover_values), 6)
            if turnover_values
            else None
        ),
        "total_turnover_proxy": round(sum(turnover_values), 6) if turnover_values else None,
        "rotation_count": sum(_int(row.get("rotation_count")) for row in mini_windows),
        "false_risk_off_count": sum(
            _int(row.get("false_risk_off_count")) for row in mini_windows
        ),
        "constraint_hit_count": sum(
            _int(row.get("constraint_hit_count")) for row in mini_windows
        ),
        "completed_window_count": len(
            [
                row
                for row in mini_windows
                if _text(row.get("mini_backfill_window_status")) == "COMPLETE"
            ]
        ),
        "price_observation_count": price_observation_count,
        "return_observation_count": return_observation_count,
        "signal_completeness": "COMPLETE" if signal_completeness_ratio >= 1.0 else "PARTIAL",
        "signal_completeness_ratio": signal_completeness_ratio,
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_mini_cost_proxy_inputs(
    mini_windows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        _candidate_v2_window_cost_proxy_inputs(
            window_id=_text(row.get("window_id")),
            return_proxy=row.get("return_proxy"),
            turnover=row.get("turnover"),
            return_observation_count=_int(row.get("return_observation_count")),
            missing_reason=_text(row.get("missing_reason"), "unknown"),
        )
        for row in mini_windows
    ]


def _candidate_v2_window_cost_proxy_inputs(
    *,
    window_id: str,
    return_proxy: Any,
    turnover: Any,
    return_observation_count: int,
    missing_reason: str,
) -> dict[str, Any]:
    available = (
        return_proxy is not None
        and turnover is not None
        and return_observation_count > 0
    )
    return {
        "window_id": window_id,
        "gross_return_proxy": return_proxy,
        "turnover_proxy": turnover,
        "return_observation_count": return_observation_count,
        "required_cost_scenarios": list(REQUIRED_COST_SCENARIOS),
        "turnover_available": turnover is not None,
        "gross_return_proxy_available": return_proxy is not None,
        "cost_scenario_inputs_available": available,
        "missing_reason": "none" if available else missing_reason,
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_mini_status(
    mini_windows: Sequence[Mapping[str, Any]],
    aggregate: Mapping[str, Any],
) -> str:
    if (
        len(mini_windows) != len(V2_MINI_BACKFILL_WINDOWS)
        or _int(aggregate.get("completed_window_count")) != len(V2_MINI_BACKFILL_WINDOWS)
        or aggregate.get("aggregate_return_proxy") is None
    ):
        return V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE
    window_returns = [
        _float(row.get("return_proxy"))
        for row in mini_windows
        if row.get("return_proxy") is not None
    ]
    if _float(aggregate.get("aggregate_return_proxy")) < V2_MINI_RETURN_WEAKNESS_CUTOFF:
        return V2_MINI_BACKFILL_WEAK
    if any(value < V2_MINI_RETURN_WEAKNESS_CUTOFF for value in window_returns):
        return V2_MINI_BACKFILL_WEAK
    return V2_MINI_BACKFILL_PROMISING


def _candidate_v2_mini_gate_signal_quick_check(
    *,
    binding_payload: Mapping[str, Any],
    signal_payload: Mapping[str, Any],
    mini_payload: Mapping[str, Any],
) -> dict[str, Any]:
    binding_summary = _mapping(binding_payload.get("summary"))
    mini_summary = _mapping(mini_payload.get("summary"))
    uncovered_contexts = [
        _text(row.get("window_id"))
        for row in _records(binding_payload.get("v2_signal_context_coverage"))
        if row.get("covered") is not True
    ]
    blocker_rows = _records(signal_payload.get("signal_blockers"))
    status = (
        "SIGNAL_QUICK_CHECK_PASS"
        if not uncovered_contexts
        and _text(mini_summary.get("signal_completeness")) == "COMPLETE"
        else "SIGNAL_QUICK_CHECK_WARNING"
    )
    return {
        "check_id": "signal_robustness_quick_check",
        "status": status,
        "source_binding_signal_row_count": _int(binding_summary.get("signal_row_count")),
        "covered_validation_context_count": _int(
            binding_summary.get("covered_validation_context_count")
        ),
        "required_validation_context_count": _int(
            binding_summary.get("required_validation_context_count")
        ),
        "uncovered_validation_contexts": uncovered_contexts,
        "mini_signal_completeness": _text(mini_summary.get("signal_completeness")),
        "mini_signal_completeness_ratio": mini_summary.get("signal_completeness_ratio"),
        "source_signal_blocker_count": len(blocker_rows),
        "source_signal_drilldown_status": _text(signal_payload.get("status")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_mini_gate_turnover_cost_quick_check(
    *,
    mini_payload: Mapping[str, Any],
    cost_payload: Mapping[str, Any],
) -> dict[str, Any]:
    mini_summary = _mapping(mini_payload.get("summary"))
    cost_inputs = _records(mini_payload.get("cost_proxy_inputs"))
    unavailable_inputs = [
        _text(row.get("window_id"))
        for row in cost_inputs
        if row.get("cost_scenario_inputs_available") is not True
    ]
    negative_windows = [
        _text(row.get("window_id"))
        for row in _records(mini_payload.get("mini_backfill_windows"))
        if row.get("return_proxy") is not None
        and _float(row.get("return_proxy")) < V2_MINI_RETURN_WEAKNESS_CUTOFF
    ]
    status = (
        "TURNOVER_COST_QUICK_CHECK_WEAK"
        if _text(mini_payload.get("status")) == V2_MINI_BACKFILL_WEAK
        else "TURNOVER_COST_QUICK_CHECK_READY"
    )
    return {
        "check_id": "turnover_cost_quick_check",
        "status": status,
        "mini_turnover_proxy": mini_summary.get("turnover_proxy"),
        "mini_aggregate_return_proxy": mini_summary.get("aggregate_return_proxy"),
        "mini_aggregate_drawdown_proxy": mini_summary.get("aggregate_drawdown_proxy"),
        "negative_return_windows": negative_windows,
        "cost_proxy_input_count": len(cost_inputs),
        "unavailable_cost_proxy_input_windows": unavailable_inputs,
        "source_cost_benchmark_status": _text(cost_payload.get("status")),
        "source_design_judgment": _text(
            _mapping(cost_payload.get("summary")).get("design_judgment")
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _candidate_v2_mini_gate_positive_evidence(
    *,
    mini_payload: Mapping[str, Any],
    signal_quick_check: Mapping[str, Any],
    turnover_cost_quick_check: Mapping[str, Any],
) -> list[dict[str, Any]]:
    summary = _mapping(mini_payload.get("summary"))
    rows: list[dict[str, Any]] = []
    if _int(summary.get("completed_window_count")) == len(V2_MINI_BACKFILL_WINDOWS):
        rows.append(
            _mini_gate_evidence(
                evidence_id="mini_windows_complete",
                category="coverage",
                strength="strong",
                evidence=(
                    f"completed_window_count={summary.get('completed_window_count')}/"
                    f"{len(V2_MINI_BACKFILL_WINDOWS)}"
                ),
            )
        )
    if _text(summary.get("signal_completeness")) == "COMPLETE":
        rows.append(
            _mini_gate_evidence(
                evidence_id="mini_signal_completeness_complete",
                category="signal_robustness",
                strength="medium",
                evidence=f"signal_completeness_ratio={summary.get('signal_completeness_ratio')}",
            )
        )
    normal_window = _window_by_id(mini_payload, "normal_market_regime")
    if normal_window and _float(normal_window.get("return_proxy")) >= 0:
        rows.append(
            _mini_gate_evidence(
                evidence_id="normal_market_positive_return_proxy",
                category="return_proxy",
                strength="medium",
                evidence=f"return_proxy={normal_window.get('return_proxy')}",
            )
        )
    if not _list_values(turnover_cost_quick_check.get("unavailable_cost_proxy_input_windows")):
        rows.append(
            _mini_gate_evidence(
                evidence_id="cost_proxy_inputs_available",
                category="cost_inputs",
                strength="medium",
                evidence=(
                    "cost_proxy_input_count="
                    f"{turnover_cost_quick_check.get('cost_proxy_input_count')}"
                ),
            )
        )
    if _int(signal_quick_check.get("source_binding_signal_row_count")) > 1:
        rows.append(
            _mini_gate_evidence(
                evidence_id="dynamic_signal_rows_available",
                category="signal_binding",
                strength="medium",
                evidence=(
                    "source_binding_signal_row_count="
                    f"{signal_quick_check.get('source_binding_signal_row_count')}"
                ),
            )
        )
    return rows


def _candidate_v2_mini_gate_negative_evidence(
    *,
    binding_payload: Mapping[str, Any],
    mini_payload: Mapping[str, Any],
    signal_quick_check: Mapping[str, Any],
    turnover_cost_quick_check: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if _text(mini_payload.get("status")) == V2_MINI_BACKFILL_WEAK:
        rows.append(
            _mini_gate_evidence(
                evidence_id="mini_backfill_weak",
                category="hard_stop",
                strength="strong",
                evidence="source_mini_backfill_status=V2_MINI_BACKFILL_WEAK",
            )
        )
    for window_id in _list_values(turnover_cost_quick_check.get("negative_return_windows")):
        window = _window_by_id(mini_payload, window_id)
        rows.append(
            _mini_gate_evidence(
                evidence_id=f"negative_return_proxy:{window_id}",
                category="return_proxy",
                strength="strong",
                evidence=(
                    f"window={window_id}; return_proxy={window.get('return_proxy')}; "
                    f"drawdown_proxy={window.get('drawdown_proxy')}"
                ),
            )
        )
    for context in _list_values(signal_quick_check.get("uncovered_validation_contexts")):
        rows.append(
            _mini_gate_evidence(
                evidence_id=f"uncovered_validation_context:{context}",
                category="signal_robustness",
                strength="medium",
                evidence=f"validation_context_not_covered={context}",
            )
        )
    warning_reason = _text(_mapping(binding_payload.get("summary")).get("warning_reason"))
    if warning_reason:
        rows.append(
            _mini_gate_evidence(
                evidence_id="source_binding_warning",
                category="binding_safety",
                strength="medium",
                evidence=warning_reason,
            )
        )
    return rows


def _candidate_v2_mini_gate_input_checks(
    *,
    spec_payload: Mapping[str, Any],
    binding_payload: Mapping[str, Any],
    mini_payload: Mapping[str, Any],
    mini_validation_payload: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    signal_quick_check: Mapping[str, Any],
    turnover_cost_quick_check: Mapping[str, Any],
) -> list[dict[str, Any]]:
    binding_summary = _mapping(binding_payload.get("summary"))
    checks = [
        {
            "check_id": "source_spec_ready",
            "status": PASS_STATUS
            if _text(spec_payload.get("status")) == CANDIDATE_V2_SPEC_FREEZE_READY
            else FAIL_STATUS,
            "evidence": _text(spec_payload.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "source_binding_safety_allows_gate",
            "status": PASS_STATUS
            if _text(binding_summary.get("safety_audit_status"))
            in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING}
            else FAIL_STATUS,
            "evidence": _text(binding_summary.get("safety_audit_status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "source_mini_backfill_validated",
            "status": PASS_STATUS
            if _text(mini_validation_payload.get("status")) == PASS_STATUS
            else FAIL_STATUS,
            "evidence": _text(mini_validation_payload.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "current_data_quality_gate_passed",
            "status": PASS_STATUS if data_quality.get("passed") is True else FAIL_STATUS,
            "evidence": _text(data_quality.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "mini_backfill_not_blocked",
            "status": PASS_STATUS
            if _text(mini_payload.get("status")) != V2_MINI_BACKFILL_BLOCKED
            else FAIL_STATUS,
            "evidence": _text(mini_payload.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "signal_quick_check_available",
            "status": PASS_STATUS
            if _text(signal_quick_check.get("status"))
            in {"SIGNAL_QUICK_CHECK_PASS", "SIGNAL_QUICK_CHECK_WARNING"}
            else FAIL_STATUS,
            "evidence": _text(signal_quick_check.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "check_id": "turnover_cost_quick_check_available",
            "status": PASS_STATUS
            if _text(turnover_cost_quick_check.get("status"))
            in {"TURNOVER_COST_QUICK_CHECK_READY", "TURNOVER_COST_QUICK_CHECK_WEAK"}
            else FAIL_STATUS,
            "evidence": _text(turnover_cost_quick_check.get("status")),
            "production_effect": PRODUCTION_EFFECT,
        },
    ]
    return checks


def _candidate_v2_mini_gate_decision(
    *,
    gate_checks: Sequence[Mapping[str, Any]],
    mini_payload: Mapping[str, Any],
) -> tuple[str, bool, str]:
    failed_checks = [
        _text(row.get("check_id"))
        for row in gate_checks
        if _text(row.get("status")) == FAIL_STATUS
    ]
    mini_status = _text(mini_payload.get("status"))
    if failed_checks:
        return V2_BLOCKED, False, _join_reasons(failed_checks)
    if mini_status == V2_MINI_BACKFILL_WEAK:
        return V2_NEEDS_REDESIGN, False, "mini_backfill_weak"
    if mini_status in {V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE, V2_MINI_BACKFILL_BLOCKED}:
        return V2_BLOCKED, False, f"mini_backfill_status:{mini_status}"
    if mini_status == V2_MINI_BACKFILL_PROMISING:
        return V2_PROCEED_TO_FULL_BACKFILL, True, ""
    return V2_BLOCKED, False, f"unknown_mini_backfill_status:{mini_status}"


def _mini_gate_evidence(
    *,
    evidence_id: str,
    category: str,
    strength: str,
    evidence: str,
) -> dict[str, Any]:
    return {
        "evidence_id": evidence_id,
        "category": category,
        "strength": strength,
        "evidence": evidence,
        "production_effect": PRODUCTION_EFFECT,
    }


def _window_by_id(payload: Mapping[str, Any], window_id: str) -> dict[str, Any]:
    for row in _records(payload.get("mini_backfill_windows")):
        if _text(row.get("window_id")) == window_id:
            return row
    return {}


def _latest_by_date(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any]:
    if not rows:
        return {}
    return dict(max(rows, key=lambda row: _text(row.get(key))))


def _join_reasons(values: Sequence[Any]) -> str:
    reasons = [_text(value) for value in values if _text(value)]
    return "; ".join(dict.fromkeys(reasons))


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


def _float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _reader_brief_complete(payload: Mapping[str, Any]) -> bool:
    brief = _mapping(payload.get("reader_brief"))
    required = ("summary", "key_result", "blocking_issues", "warnings", "next_action")
    return all(bool(_text(brief.get(key))) for key in required)


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
    if report_type == SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Signal Blockers", "signal_blockers"),
            ("Non Blocking Signal Checks", "non_blocking_signal_checks"),
        ]
    if report_type == SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Window Attributions", "window_attributions"),
            ("Failure Modes", "failure_modes"),
        ]
    if report_type == WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Stress Scenario Attributions", "stress_scenario_attributions"),
            ("Stress Weakness Root Causes", "stress_weakness_root_causes"),
        ]
    if report_type == STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Cost Scenario Attributions", "cost_scenario_attributions"),
            ("Benchmark Baseline Attributions", "benchmark_baseline_attributions"),
            ("Cost Benchmark Root Causes", "cost_benchmark_root_causes"),
        ]
    if report_type == COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Candidate Redesign Hypotheses", "candidate_redesign_hypotheses"),
            ("Target Coverage", "target_coverage"),
        ]
    if report_type == CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Differences From Trading 470 Candidate", "differences_from_trading_470_candidate"),
        ]
    if report_type == CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("V2 Signal Context Coverage", "v2_signal_context_coverage"),
            ("V2 Candidate Signal Series", "v2_candidate_signal_series"),
            (
                "V2 Hypothetical Research Weight Series",
                "v2_hypothetical_research_weight_series",
            ),
        ]
    if report_type == CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Mini Backfill Windows", "mini_backfill_windows"),
            ("Cost Proxy Inputs", "cost_proxy_inputs"),
        ]
    if report_type == CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE:
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    if report_type == CANDIDATE_V2_MINI_GATE_REPORT_TYPE:
        return [
            ("Source Artifacts", "source_artifacts"),
            ("Gate Input Checks", "gate_input_checks"),
            ("Strongest Positive Evidence", "strongest_positive_evidence"),
            ("Strongest Negative Evidence", "strongest_negative_evidence"),
        ]
    if report_type == CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE:
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
    "CANDIDATE_REDESIGN_HYPOTHESIS_READY",
    "CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE",
    "CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE",
    "CANDIDATE_REDESIGN_PRIORITIES",
    "CANDIDATE_V2_EXECUTABLE_BINDING_BLOCKED",
    "CANDIDATE_V2_EXECUTABLE_BINDING_READY",
    "CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS",
    "CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE",
    "CANDIDATE_V2_EXECUTABLE_BINDING_STATUSES",
    "CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE",
    "CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE",
    "CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE",
    "CANDIDATE_V2_MINI_GATE_REPORT_TYPE",
    "CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE",
    "V2_MINI_BACKFILL_BLOCKED",
    "V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE",
    "V2_MINI_BACKFILL_PROMISING",
    "V2_MINI_BACKFILL_STATUSES",
    "V2_MINI_BACKFILL_WEAK",
    "V2_MINI_GATE_STATUSES",
    "V2_BLOCKED",
    "V2_NEEDS_REDESIGN",
    "V2_PROCEED_TO_FULL_BACKFILL",
    "V2_REJECT_RESEARCH_CANDIDATE",
    "CANDIDATE_V2_SPEC_FREEZE_READY",
    "CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE",
    "CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE",
    "COST_BENCHMARK_DESIGN_JUDGMENTS",
    "COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY",
    "COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE",
    "COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE",
    "EVIDENCE_GAP_LEDGER_REPORT_TYPE",
    "EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE",
    "REPORT_PREFIXES",
    "REPAIRABILITY_TYPES",
    "REQUIRED_BACKFILL_WINDOWS",
    "REQUIRED_BENCHMARK_BASELINES",
    "REQUIRED_COST_SCENARIOS",
    "REQUIRED_REDESIGN_TARGETS",
    "REQUIRED_STRESS_SCENARIOS",
    "REQUIRED_WINDOW_SPLITS",
    "SIGNAL_BLOCKER_CAUSES",
    "SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE",
    "SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE",
    "SIGNAL_ROBUSTNESS_DRILLDOWN_STATUSES",
    "SIGNAL_ROBUSTNESS_NEEDS_CANDIDATE_REDESIGN",
    "SIGNAL_ROBUSTNESS_NOT_REPAIRABLE",
    "SIGNAL_ROBUSTNESS_REPAIRABLE",
    "STRESS_DESIGN_JUDGMENTS",
    "STRESS_WEAKNESS_ATTRIBUTION_READY",
    "STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE",
    "STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE",
    "VALIDATION_SUFFIX",
    "WINDOW_FRAGILITY_ATTRIBUTION_CATEGORIES",
    "WINDOW_FRAGILITY_ATTRIBUTION_READY",
    "WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE",
    "WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE",
    "WINDOW_FRAGILITY_JUDGMENTS",
    "build_backfill_partial_root_cause_repair_plan_payload",
    "build_candidate_redesign_hypothesis_payload",
    "build_candidate_v2_executable_binding_update_payload",
    "build_candidate_v2_mini_backfill_payload",
    "build_candidate_v2_mini_gate_payload",
    "build_candidate_v2_spec_freeze_payload",
    "build_cost_benchmark_weakness_attribution_payload",
    "build_executable_research_evidence_gap_ledger_payload",
    "build_signal_robustness_blocker_drilldown_payload",
    "build_stress_weakness_attribution_payload",
    "build_window_fragility_attribution_payload",
    "default_evidence_repair_json_path",
    "default_evidence_repair_markdown_path",
    "latest_evidence_repair_json_path",
    "render_evidence_repair_markdown",
    "validate_backfill_partial_root_cause_repair_plan_payload",
    "validate_candidate_redesign_hypothesis_payload",
    "validate_candidate_v2_executable_binding_update_payload",
    "validate_candidate_v2_mini_backfill_payload",
    "validate_candidate_v2_mini_gate_payload",
    "validate_candidate_v2_spec_freeze_payload",
    "validate_cost_benchmark_weakness_attribution_payload",
    "validate_executable_research_evidence_gap_ledger_payload",
    "validate_signal_robustness_blocker_drilldown_payload",
    "validate_stress_weakness_attribution_payload",
    "validate_window_fragility_attribution_payload",
    "write_evidence_repair_json",
    "write_evidence_repair_markdown",
]
