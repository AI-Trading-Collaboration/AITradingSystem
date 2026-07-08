from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_regime_slice_attribution_review.v1"
REGIME_SLICE_ATTRIBUTION_MATRIX_SCHEMA_VERSION = (
    "growth_tilt_regime_slice_attribution_matrix.v1"
)
CANDIDATE_STATUS_BY_REGIME_SCHEMA_VERSION = (
    "growth_tilt_candidate_status_by_regime.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_regime_slice_attribution_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2436_STATUS = "GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_READY"
EXPECTED_2436_NEXT_ROUTE = "TRADING-2437_Growth_Tilt_Regime_Slice_Attribution_Review"
EXPECTED_2432_STATUS = "GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY"
EXPECTED_CANDIDATE_SET_ID = "growth_tilt_batch_2432"
RECOMMENDED_REGIME_SLICES: tuple[str, ...] = (
    "growth_bull",
    "growth_drawdown",
    "rate_shock",
    "volatility_spike",
    "liquidity_stress",
    "post_drawdown_recovery",
    "sideways_chop",
    "semiconductor_leadership",
    "mega_cap_concentration",
)
REQUIRED_CANDIDATE_SET_REGIME_SLICES: tuple[str, ...] = (
    "ai_after_chatgpt_full_window",
    "risk_off_drawdown_windows",
    "growth_recovery_windows",
    "sideways_whipsaw_windows",
)
REQUIRED_METRIC_IDS: tuple[str, ...] = (
    "regime_robustness_score",
    "return_delta_vs_baseline",
    "max_drawdown_delta_vs_baseline",
)
NEXT_ROUTE = "TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay"
BLOCKED_ROUTE = "TRADING-2437_Growth_Tilt_Regime_Slice_Attribution_Evidence_Gap_Remediation"
REPORT_TYPE = "growth_tilt_regime_slice_attribution_review"
REGIME_STATUS_VALUES: tuple[str, ...] = ("pass", "fail", "inconclusive")
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_turnover_cooldown_parameter_plateau_study",
    "growth_tilt_candidate_gauntlet_harness",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-regime-slice-attribution-review",
    "outputs/research_strategies/growth_tilt_regime_slice_attribution_review/"
    "regime_slice_attribution_review_result.json",
    "outputs/research_strategies/growth_tilt_regime_slice_attribution_review/"
    "regime_slice_attribution_matrix.json",
    "outputs/research_strategies/growth_tilt_regime_slice_attribution_review/"
    "candidate_status_by_regime.json",
    "outputs/research_strategies/growth_tilt_regime_slice_attribution_review/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_regime_slice_attribution_review.md",
    "docs/research/growth_tilt_regime_slice_attribution_matrix.md",
    "docs/research/growth_tilt_candidate_status_by_regime.md",
    "docs/research/growth_tilt_regime_slice_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2438_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-regime-slice-attribution-review",
    READY_STATUS,
    NEXT_ROUTE,
)
NOT_COMPUTED_SCORE = 0.0
NOT_COMPUTED_COUNT = 0


def build_growth_tilt_regime_slice_attribution_review(
    source_2436_parameter_plateau_study: Mapping[str, Any],
    source_2432_candidate_gauntlet: Mapping[str, Any],
    candidate_set_2432: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2436_parameter_plateau_study,
        source_2432_candidate_gauntlet,
        candidate_set_2432,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_text=research_text,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    status = READY_STATUS if not gaps else BLOCKED_STATUS
    matrix = _regime_slice_attribution_matrix(status, gaps)
    status_by_regime = _candidate_status_by_regime(status, gaps)
    boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2437",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2436", "TRADING-2432"],
        "source_2436_ready": _source_2436_ready(source_2436_parameter_plateau_study),
        "source_2432_gauntlet_ready": _source_2432_ready(
            source_2432_candidate_gauntlet
        ),
        "candidate_set_regime_slice_contract_ready": (
            _candidate_set_regime_slice_contract_ready(candidate_set_2432)
        ),
        "candidate_set_required_metrics_ready": (
            _candidate_set_required_metrics_ready(candidate_set_2432)
        ),
        "regime_slice_attribution_review_ready": status == READY_STATUS,
        "regime_slice_attribution_matrix_ready": matrix[
            "regime_slice_attribution_matrix_ready"
        ],
        "candidate_status_by_regime_ready": status_by_regime[
            "candidate_status_by_regime_ready"
        ],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "recommended_regime_slices": list(RECOMMENDED_REGIME_SLICES),
        "recommended_regime_slice_count": len(RECOMMENDED_REGIME_SLICES),
        "candidate_set_regime_slice_count": _candidate_set_regime_slice_count(
            candidate_set_2432
        ),
        "regime_robustness_score": NOT_COMPUTED_SCORE,
        "single_regime_dependency_detected": False,
        "single_regime_dependency_assessed": False,
        "candidate_status_by_regime": _default_candidate_status_by_regime(),
        "regime_pass_count": NOT_COMPUTED_COUNT,
        "regime_fail_count": NOT_COMPUTED_COUNT,
        "regime_inconclusive_count": len(RECOMMENDED_REGIME_SLICES),
        "all_recommended_regime_status_inconclusive": True,
        "component_value_found": False,
        "candidate_status": "needs_pit",
        "regime_status_values": list(REGIME_STATUS_VALUES),
        "computed_new_metrics": False,
        "regime_attribution_run": False,
        "market_data_regime_attribution_run": False,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "regime_slice_attribution_matrix": matrix,
        "candidate_status_by_regime_artifact": status_by_regime,
        "no_effect_boundary": boundary,
        "market_data_experiment_run": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "recommended_next_research_task": NEXT_ROUTE if status == READY_STATUS else BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Regime slice attribution contract is ready but no real regime "
            "attribution was run; continue to top-3 PIT replay."
            if status == READY_STATUS
            else "Required regime slice attribution review evidence is missing."
        ),
    }


def _requirements(
    source_2436: Mapping[str, Any],
    source_2432: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2436_parameter_plateau_study_ready",
            _source_2436_ready(source_2436),
            "prior_parameter_plateau_study_gap",
            {
                "status": source_2436.get("status"),
                "next_route": source_2436.get("recommended_next_research_task"),
            },
        ),
        _requirement(
            "source_2432_candidate_gauntlet_ready",
            _source_2432_ready(source_2432),
            "candidate_gauntlet_gap",
            {"status": source_2432.get("status")},
        ),
        _requirement(
            "candidate_set_regime_slice_contract_ready",
            _candidate_set_regime_slice_contract_ready(candidate_set),
            "candidate_set_regime_slice_contract_gap",
            {"required_slices": list(REQUIRED_CANDIDATE_SET_REGIME_SLICES)},
        ),
        _requirement(
            "candidate_set_required_metrics_ready",
            _candidate_set_required_metrics_ready(candidate_set),
            "candidate_set_metric_gap",
            {"required_metric_ids": list(REQUIRED_METRIC_IDS)},
        ),
        _requirement(
            "prior_research_doc_coverage",
            "regime" in research_text
            and _candidate_set_regime_slice_contract_ready(candidate_set),
            "research_doc_gap",
            {
                "required_references": [
                    "regime",
                    "candidate_set.regime_slice_check",
                ]
            },
        ),
        _requirement(
            "report_registry_coverage",
            _report_registry_has(report_registry, REQUIRED_REPORT_IDS),
            "registry_catalog_doc_gap",
            {"required_report_ids": list(REQUIRED_REPORT_IDS)},
        ),
        _requirement(
            "artifact_catalog_coverage",
            _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_CATALOG_REFERENCES)},
        ),
        _requirement(
            "system_flow_coverage",
            _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_SYSTEM_FLOW_REFERENCES)},
        ),
    ]


def _source_2436_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2436_STATUS
        and payload.get("parameter_plateau_study_ready") is True
        and payload.get("recommended_next_research_task") == EXPECTED_2436_NEXT_ROUTE
        and payload.get("parameter_sweep_run") is False
    )


def _source_2432_ready(payload: Mapping[str, Any]) -> bool:
    section_status = _mapping(payload.get("candidate_set_section_status"))
    return (
        payload.get("status") == EXPECTED_2432_STATUS
        and payload.get("candidate_set_ready") is True
        and payload.get("candidate_set_id") == EXPECTED_CANDIDATE_SET_ID
        and payload.get("candidates_tested") == 0
        and payload.get("candidate_gauntlet_run") is False
        and section_status.get("regime_slice_check") is True
    )


def _candidate_set_regime_slice_contract_ready(
    candidate_set: Mapping[str, Any],
) -> bool:
    regime_slice_check = _mapping(candidate_set.get("regime_slice_check"))
    slices = set(str(item) for item in _sequence(regime_slice_check.get("slices")))
    return (
        candidate_set.get("candidate_set_id") == EXPECTED_CANDIDATE_SET_ID
        and regime_slice_check.get("ready") is True
        and set(REQUIRED_CANDIDATE_SET_REGIME_SLICES).issubset(slices)
    )


def _candidate_set_regime_slice_count(candidate_set: Mapping[str, Any]) -> int:
    regime_slice_check = _mapping(candidate_set.get("regime_slice_check"))
    return len(_sequence(regime_slice_check.get("slices")))


def _candidate_set_required_metrics_ready(candidate_set: Mapping[str, Any]) -> bool:
    metrics = _sequence(_mapping(candidate_set.get("unified_metrics")).get("metrics"))
    metric_ids = {
        str(metric.get("metric_id")) for metric in metrics if isinstance(metric, Mapping)
    }
    return set(REQUIRED_METRIC_IDS).issubset(metric_ids)


def _regime_slice_attribution_matrix(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = [
        {
            "regime_slice": regime_slice,
            "candidate_status": "inconclusive",
            "attribution_available": False,
            "regime_robustness_score": NOT_COMPUTED_SCORE,
            "single_regime_dependency_detected": False,
            "measurement_basis": (
                "not_computed_prior_artifact_contract_only_no_regime_attribution_run"
            ),
        }
        for regime_slice in RECOMMENDED_REGIME_SLICES
    ]
    return {
        "schema_version": REGIME_SLICE_ATTRIBUTION_MATRIX_SCHEMA_VERSION,
        "status": status,
        "regime_slice_attribution_matrix_ready": not gaps,
        "recommended_regime_slice_count": len(RECOMMENDED_REGIME_SLICES),
        "regime_robustness_score": NOT_COMPUTED_SCORE,
        "single_regime_dependency_detected": False,
        "single_regime_dependency_assessed": False,
        "rows": rows,
        "computed_new_metrics": False,
        "regime_attribution_run": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_status_by_regime(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    status_by_regime = _default_candidate_status_by_regime()
    return {
        "schema_version": CANDIDATE_STATUS_BY_REGIME_SCHEMA_VERSION,
        "status": status,
        "candidate_status_by_regime_ready": not gaps,
        "candidate_status_by_regime": status_by_regime,
        "regime_pass_count": NOT_COMPUTED_COUNT,
        "regime_fail_count": NOT_COMPUTED_COUNT,
        "regime_inconclusive_count": len(status_by_regime),
        "all_recommended_regime_status_inconclusive": True,
        "measurement_basis": "not_computed_prior_artifact_contract_only_no_regime_attribution_run",
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": not gaps,
        "computed_new_metrics": False,
        "regime_attribution_run": False,
        "market_data_regime_attribution_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_candidate_status_by_regime() -> dict[str, str]:
    return {regime_slice: "inconclusive" for regime_slice in RECOMMENDED_REGIME_SLICES}


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    evidence: Any,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": evidence,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement["requirement_id"],
        "classification": requirement["classification"],
        "gap": f"{requirement['requirement_id']} did not pass.",
        "evidence": requirement.get("evidence"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence):
        return False
    present = {
        str(report.get("report_id"))
        for report in reports
        if isinstance(report, Mapping) and report.get("report_id")
    }
    return set(report_ids).issubset(present)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return value
    return ()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
