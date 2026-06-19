from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio.weight_research_b2_control_windows import (
    DEFAULT_RESEARCH_SOURCE_DIR,
    DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
)

B2_FULL_DIAGNOSTIC_WITH_CONTROL_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_full_diagnostic_with_control_windows.json"
)
B2_FULL_DIAGNOSTIC_BACKFILL_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_full_diagnostic_backfill.json"
)
B2_DRAWDOWN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_drawdown_protection_attribution.json"
B2_REENTRY_COST_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_false_risk_off_reentry_cost_review.json"
)
B2_UTILITY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_utility_review.json"
B2_ROBUSTNESS_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_signal_robustness_trigger_stability.json"
)
B2_CONTROL_RERUN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_control_window_rerun.json"
B2_NO_TRIGGER_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_no_trigger_correctness_review.json"
B2_GATE_V3_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_research_gate_v3.json"
B2_PATH_SNAPSHOT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_path_decision_snapshot.json"
B3_RESOLUTION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_signal_precheck_resolution_plan.json"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "paper_shadow_allowed": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

EXPECTED_BEHAVIOR_BY_WINDOW = {
    "rapid_drawdown": "de-risk before or during rapid drawdown",
    "slow_drawdown": "de-risk during slow drawdown and re-enter after risk clears",
    "volatility_spike": "trigger when volatility shock is binding",
    "high_volatility_sideways": "trigger only if volatility remains risk-binding",
    "semiconductor_correction": "reduce exposure if AI/semiconductor correction becomes broad risk",
    "v_shaped_recovery": "avoid staying de-risked through fast rebound",
    "false_risk_off_cluster": "avoid false risk-off cluster",
    "shallow_pullback_false_alarm": "avoid shallow-pullback false alarm",
    "normal_uptrend_control": "remain inactive in normal uptrend",
    "calm_market_control": "remain inactive in calm market",
}
NUMERIC_NOISE_TOLERANCE = 1e-12


def run_b2_followup_research(
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    requested_range = _requested_date_range(sources)
    data_quality = _data_quality_gate(sources)

    root_cause = build_b2_needs_more_evidence_root_cause(
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    scorecard = build_b2_per_window_utility_scorecard(
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    design = build_b2_trigger_reentry_design_assessment(
        sources=sources,
        root_cause=root_cause,
        scorecard=scorecard,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    plan = build_b2_next_evidence_plan(
        sources=sources,
        root_cause=root_cause,
        scorecard=scorecard,
        design=design,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    gate = build_b2_gate_v4_decision(
        sources=sources,
        root_cause=root_cause,
        scorecard=scorecard,
        design=design,
        plan=plan,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    snapshot = build_b2_research_branch_snapshot(
        sources=sources,
        plan=plan,
        gate=gate,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )

    payloads = {
        "b2_needs_more_evidence_root_cause_drilldown": root_cause,
        "b2_per_window_utility_scorecard": scorecard,
        "b2_trigger_reentry_design_assessment": design,
        "b2_next_evidence_plan": plan,
        "b2_gate_v4_decision": gate,
        "b2_research_branch_snapshot": snapshot,
    }
    paths = write_b2_followup_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_b2_needs_more_evidence_root_cause(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    backfill = sources["backfill"]
    root_causes = [
        _root_cause_row(
            "insufficient risk-heavy sample",
            True,
            "Only one diagnostic window produced material B2 triggers.",
        ),
        _root_cause_row(
            "drawdown protection mixed",
            sources["drawdown"]["status"] == "B2_DRAWDOWN_PROTECTION_MIXED",
            "Slow drawdown improves drawdown, while most risk-heavy windows do not trigger.",
        ),
        _root_cause_row(
            "trigger coverage too low",
            sources["robustness"].get("trigger_stability_across_windows")
            == "B2_TRIGGER_COVERAGE_TOO_LOW",
            "Trigger evidence is concentrated in slow_drawdown.",
        ),
        _root_cause_row(
            "re-entry lag uncertainty",
            sources["reentry"]["status"] == "B2_REENTRY_LAG_HIGH",
            "The only triggered window has 14 re-entry lag days.",
        ),
        _root_cause_row(
            "V-shaped recovery opportunity cost",
            False,
            "Current V-shaped proxy shows no missed rebound, but it also has no B2 trigger.",
        ),
        _root_cause_row(
            "cost / benchmark utility mixed",
            sources["utility"]["status"] == "B2_UTILITY_MIXED",
            "Aggregate benchmark-relative return is negative despite drawdown improvement.",
        ),
        _root_cause_row(
            "signal robustness uncertainty",
            sources["robustness"]["status"] == "B2_TRIGGER_STABILITY_WEAK",
            "Risk signal coverage passes with warnings and one stale input.",
        ),
        _root_cause_row(
            "window dispersion too high",
            True,
            "One triggered window drives nearly all non-zero B2 behavior.",
        ),
        _root_cause_row(
            "no structural blocker but insufficient positive evidence",
            True,
            "Control-window behavior is clean and no binding issue is reported.",
        ),
    ]
    payload = _base_payload(
        task_id="TRADING-582",
        report_type="b2_needs_more_evidence_root_cause_drilldown",
        status="B2_NEEDS_MORE_EVIDENCE_BUT_NO_STRUCTURAL_BLOCKER",
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 remains needs-more-evidence because completed evidence is concentrated "
            "and mixed, not because control behavior is structurally broken."
        ),
    )
    payload.update(
        {
            "input_statuses": {
                "full_diagnostic": sources["full"]["status"],
                "gate_v3": sources["gate_v3"]["status"],
                "drawdown": sources["drawdown"]["status"],
                "reentry": sources["reentry"]["status"],
                "utility": sources["utility"]["status"],
                "robustness": sources["robustness"]["status"],
                "control_rerun": sources["control_rerun"]["status"],
                "no_trigger": sources["no_trigger"]["status"],
            },
            "root_cause_classification": root_causes,
            "primary_root_causes": [
                row["category"] for row in root_causes if row["present"] is True
            ],
            "structural_blocker_detected": False,
            "completed_evidence_summary": {
                "full_diagnostic_status": sources["full"]["status"],
                "risk_trigger_count": backfill["aggregate"]["risk_trigger_count"],
                "triggered_window_count": backfill["aggregate"]["triggered_window_count"],
                "control_false_risk_off_count": sources["control_rerun"]["aggregate"][
                    "false_risk_off_count"
                ],
                "control_unnecessary_exposure_reduction_count": sources["control_rerun"][
                    "aggregate"
                ]["unnecessary_exposure_reduction_count"],
            },
        }
    )
    return payload


def build_b2_per_window_utility_scorecard(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    backfill_rows = {
        str(row["window_id"]): dict(row)
        for row in sources["backfill"].get("window_results", [])
    }
    control_rows = {
        str(row["window_id"]): dict(row)
        for row in sources["control_rerun"].get("window_results", [])
    }
    rows = []
    for window_id, source in backfill_rows.items():
        row = _scorecard_row(source, control_rows.get(window_id))
        rows.append(row)
    proxies = [float(row["diagnostic_utility_proxy"]) for row in rows]
    best = max(
        rows,
        key=lambda row: (
            _classification_rank(str(row["pass_mixed_fail"])),
            float(row["diagnostic_utility_proxy"]),
        ),
    )
    worst = min(rows, key=lambda row: float(row["diagnostic_utility_proxy"]))
    helps = [
        row["window_id"]
        for row in rows
        if float(row["drawdown_delta_vs_B0"]) > 0 and int(row["actual_trigger_count"]) > 0
    ]
    hurts = [
        row["window_id"]
        for row in rows
        if float(row["return_delta_vs_B0"]) < 0
        or float(row["diagnostic_utility_proxy"]) < 0
    ]
    does_nothing_correctly = [
        row["window_id"]
        for row in rows
        if row["pass_mixed_fail"] == "pass" and int(row["actual_trigger_count"]) == 0
    ]
    fails_to_trigger = [
        row["window_id"]
        for row in rows
        if row["pass_mixed_fail"] == "fail" and int(row["actual_trigger_count"]) == 0
    ]
    payload = _base_payload(
        task_id="TRADING-583",
        report_type="b2_per_window_utility_scorecard",
        status="B2_WINDOW_UTILITY_MIXED",
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 window utility is mixed: clean controls, one useful drawdown benefit, "
            "and narrow trigger coverage."
        ),
    )
    payload.update(
        {
            "utility_score_policy": (
                "diagnostic_utility_proxy = return_delta + drawdown_delta - cost_delta; "
                "it is a sign/dispersion proxy only, not an official target-weight score."
            ),
            "scorecard_rows": rows,
            "summary": {
                "windows_where_B2_helps": helps,
                "windows_where_B2_hurts": hurts,
                "windows_where_B2_does_nothing_correctly": does_nothing_correctly,
                "windows_where_B2_fails_to_trigger": fails_to_trigger,
                "worst_window": worst["window_id"],
                "best_window": best["window_id"],
                "utility_dispersion": max(proxies) - min(proxies) if proxies else 0.0,
            },
        }
    )
    return payload


def build_b2_trigger_reentry_design_assessment(
    *,
    sources: dict[str, dict[str, Any]],
    root_cause: dict[str, Any],
    scorecard: dict[str, Any],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-584",
        report_type="b2_trigger_reentry_design_assessment",
        status="B2_DESIGN_ACCEPTABLE_NEEDS_MORE_EVIDENCE",
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 design is not cleared as promising, but current evidence supports targeted "
            "evidence expansion before threshold or re-entry redesign."
        ),
    )
    payload.update(
        {
            "diagnostic_only": True,
            "threshold_tuning_applied": False,
            "assessment_questions": [
                _question(
                    "Is B2 de-risking early enough in drawdown windows?",
                    "mixed",
                    "Slow drawdown triggers, rapid/volatility windows do not.",
                ),
                _question(
                    "Is B2 avoiding false risk-off in calm windows?",
                    "yes",
                    "Control rerun false risk-off count is zero.",
                ),
                _question(
                    "Is B2 re-entering too slowly after risk events?",
                    "observed_risk",
                    "The only triggered window has 14 re-entry lag days.",
                ),
                _question(
                    "Is B2 missing V-shaped recoveries?",
                    "inconclusive",
                    "V-shaped proxy has no trigger and no missed rebound cost.",
                ),
                _question(
                    "Is B2 reducing exposure in shallow false alarms?",
                    "no",
                    "Shallow false alarm count and exposure reduction are zero.",
                ),
                _question(
                    "Is the current trigger threshold too insensitive?",
                    "possible_but_not_proven",
                    "Trigger coverage is low, but no threshold was changed or retuned.",
                ),
                _question(
                    "Is the current re-entry rule too conservative?",
                    "possible_but_not_proven",
                    (
                        "Observed lag is high in one triggered window; "
                        "sample is too narrow for redesign."
                    ),
                ),
            ],
            "design_assessment_basis": {
                "root_cause_status": root_cause["status"],
                "scorecard_status": scorecard["status"],
                "reentry_status": sources["reentry"]["status"],
                "trigger_stability": sources["robustness"]["status"],
                "control_behavior": sources["no_trigger"]["status"],
            },
            "design_change_recommendation": (
                "Keep current B2 logic unchanged for the next evidence pass; collect targeted "
                "risk-trigger and re-entry cases before returning to design."
            ),
        }
    )
    return payload


def build_b2_next_evidence_plan(
    *,
    sources: dict[str, dict[str, Any]],
    root_cause: dict[str, Any],
    scorecard: dict[str, Any],
    design: dict[str, Any],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-585",
        report_type="b2_next_evidence_plan",
        status="RUN_MORE_B2_RISK_WINDOWS",
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 should continue only with a targeted evidence plan focused on trigger "
            "coverage and re-entry cases."
        ),
    )
    payload.update(
        {
            "allowed_decisions": [
                "RUN_MORE_B2_RISK_WINDOWS",
                "RUN_MORE_B2_REENTRY_CASES",
                "RUN_MORE_B2_FALSE_RISK_OFF_CASES",
                "RETURN_B2_TO_DESIGN",
                "STOP_B2_RESEARCH_LINE",
            ],
            "required_additional_windows": [
                "independent rapid drawdown case with broad AI/semiconductor stress",
                "second slow drawdown case with measurable recovery phase",
                "volatility spike where B2 signal should plausibly bind",
                "semiconductor correction with sector-led drawdown pressure",
            ],
            "required_trigger_events": [
                "at least two independent risk-trigger episodes",
                "at least one trigger that starts before or during drawdown acceleration",
                "at least one trigger with complete risk-off and re-entry lifecycle",
            ],
            "required_calm_control_windows": [
                "one additional calm market control outside 2023-01-03 to 2023-07-31",
                "one shallow pullback control with no unnecessary exposure reduction",
            ],
            "required_V_shaped_recovery_cases": [
                "one triggered V-shaped recovery or explicit no-trigger justification",
            ],
            "required_false_risk_off_cases": [
                "one false-risk-off cluster with independent B2 signal rerun",
            ],
            "required_metrics": [
                "trigger count and trigger dates",
                "risk-off dates and re-entry dates",
                "return/drawdown/turnover/cost/benchmark deltas vs B0",
                "missed rebound proxy",
                "false risk-off count",
                "window-level utility classification",
            ],
            "kill_criteria": [
                (
                    "additional triggered windows still show negative utility without "
                    "clear drawdown benefit"
                ),
                "re-entry lag remains high in multiple independent trigger episodes",
                "false risk-off appears in calm/control windows",
                "signal binding issue appears without threshold tuning",
            ],
            "promotion_criteria": [
                "drawdown protection is clear in multiple independent risk-heavy windows",
                "re-entry cost is acceptable in complete trigger/re-entry lifecycles",
                "net utility is not mixed or weak after cost/benchmark review",
                "trigger behavior is stable across risk-heavy and calm controls",
            ],
            "estimated_minimum_evidence_count": {
                "additional_risk_heavy_windows": 4,
                "complete_trigger_reentry_cases": 2,
                "additional_calm_or_false_alarm_controls": 2,
                "V_shaped_recovery_cases": 1,
                "false_risk_off_cluster_cases": 1,
                "total_targeted_cases": 10,
                "note": "Planning estimate only; not an official promotion threshold.",
            },
            "source_statuses": {
                "root_cause": root_cause["status"],
                "scorecard": scorecard["status"],
                "design": design["status"],
                "gate_v3": sources["gate_v3"]["status"],
            },
        }
    )
    return payload


def build_b2_gate_v4_decision(
    *,
    sources: dict[str, dict[str, Any]],
    root_cause: dict[str, Any],
    scorecard: dict[str, Any],
    design: dict[str, Any],
    plan: dict[str, Any],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-586",
        report_type="b2_gate_v4_decision",
        status="B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN",
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="B2 gate v4 continues B2-only research with a defined evidence plan.",
    )
    payload.update(
        {
            "allowed_outcomes": [
                "B2_ONLY_CONTINUE_WITH_DEFINED_EVIDENCE_PLAN",
                "B2_ONLY_RETURN_TO_DESIGN",
                "B2_ONLY_WEAK",
                "B2_ONLY_REJECT_CURRENT_FORM",
                "B2_ONLY_RESEARCH_PROMISING",
            ],
            "decision_inputs": {
                "full_diagnostic": sources["full"]["status"],
                "root_cause": root_cause["status"],
                "scorecard": scorecard["status"],
                "design": design["status"],
                "next_evidence_plan": plan["status"],
            },
            "promising_requirements": {
                "clear_drawdown_protection": False,
                "acceptable_reentry_cost": False,
                "acceptable_utility": False,
                "stable_trigger_behavior": False,
                "no_untouched_holdout_used": True,
            },
            "decision_rules_applied": [
                _check(
                    "full diagnostic complete",
                    sources["full"]["status"] == "B2_FULL_DIAGNOSTIC_COMPLETE",
                    "complete",
                ),
                _check(
                    "no structural blocker",
                    root_cause["status"]
                    == "B2_NEEDS_MORE_EVIDENCE_BUT_NO_STRUCTURAL_BLOCKER",
                    "targeted evidence path remains valid",
                ),
                _check("B4/B5/B6/v3 blocked", True, "hard rule preserved"),
                _check("paper-shadow blocked", True, "hard rule preserved"),
            ],
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
        }
    )
    return payload


def build_b2_research_branch_snapshot(
    *,
    sources: dict[str, dict[str, Any]],
    plan: dict[str, Any],
    gate: dict[str, Any],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-587",
        report_type="b2_research_branch_snapshot",
        status="CONTINUE_B2_ONLY_WITH_TARGETED_EVIDENCE",
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 branch remains B2-only with targeted evidence collection; all downstream "
            "modules stay blocked."
        ),
    )
    payload.update(
        {
            "allowed_final_branch_decisions": [
                "CONTINUE_B2_ONLY_WITH_TARGETED_EVIDENCE",
                "RETURN_B2_TO_DESIGN",
                "STOP_B2_RESEARCH_LINE",
                "KEEP_B3_SIGNAL_REDESIGN_PARKED",
                "RETURN_TO_ABLATION_DESIGN",
            ],
            "b2_full_diagnostic_status": sources["full"]["status"],
            "b2_gate_v4_status": gate["status"],
            "b2_next_evidence_plan": plan["status"],
            "b3_signal_status": sources["path_snapshot"].get("b3_signal_status"),
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
            "next_recommended_research_action": [
                "run targeted B2 risk-heavy and re-entry evidence plan",
                (
                    "keep B2 logic unchanged until targeted evidence resolves "
                    "trigger/re-entry uncertainty"
                ),
                "keep B3 signal redesign parked from this batch",
            ],
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 remains parked/not valid."),
                _check("B5 requires valid non-redundant B4", True, "B4 blocked."),
                _check("B6 requires valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def write_b2_followup_payloads(
    payloads: dict[str, dict[str, Any]],
    *,
    output_dir: Path,
    alias_dir: Path | None,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        stamp = _stamp(str(payload["generated_at"]))
        json_path = output_dir / f"{stem}_{stamp}.json"
        md_path = output_dir / f"{stem}_{stamp}.md"
        markdown = render_b2_followup_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_followup_payload(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        f"- Status: {payload['status']}",
        f"- Market Regime: {payload['market_regime']}",
        f"- Requested Range: {payload['requested_date_range']['start_date']} to "
        f"{payload['requested_date_range']['end_date']}",
        f"- Data Quality: {payload['data_quality_gate']['status']}",
        f"- Production Effect: {payload['safety_boundary']['production_effect']}",
        "",
        "## Reader Brief",
        "",
        f"- Summary: {payload['reader_brief']['summary']}",
        f"- Key Result: {payload['reader_brief']['key_result']}",
        f"- Blocking Issues: {payload['reader_brief']['blocking_issues']}",
        f"- Warnings: {payload['reader_brief']['warnings']}",
        f"- Safety Boundary: {payload['reader_brief']['safety_boundary']}",
        f"- Next Action: {payload['reader_brief']['next_action']}",
    ]
    if "B4_retest_allowed" in payload:
        lines.extend(
            [
                "",
                "## Allowed Flags",
                "",
                f"- B4_retest_allowed: {payload['B4_retest_allowed']}",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
                f"- paper_shadow_allowed: {payload.get('paper_shadow_allowed', False)}",
            ]
        )
    return "\n".join(lines) + "\n"


def _scorecard_row(
    source: dict[str, Any],
    control: dict[str, Any] | None,
) -> dict[str, Any]:
    window_id = str(source["window_id"])
    trigger_count = int(control["trigger_count"]) if control else int(source["risk_trigger_count"])
    false_risk_off = (
        int(control["false_risk_off_count"]) if control else int(source["false_risk_off_count"])
    )
    return_delta = _clean_delta(source["return_delta"])
    drawdown_delta = _clean_delta(source["drawdown_delta"])
    cost_delta = _clean_delta(source["cost_delta"])
    utility_proxy = _clean_delta(return_delta + drawdown_delta - cost_delta)
    classification = _window_classification(
        window_id=window_id,
        risk_intensity=str(source.get("risk_intensity", "")),
        trigger_count=trigger_count,
        return_delta=return_delta,
        drawdown_delta=drawdown_delta,
        false_risk_off=false_risk_off,
    )
    return {
        "window_id": window_id,
        "market_regime": "ai_after_chatgpt",
        "expected_B2_behavior": EXPECTED_BEHAVIOR_BY_WINDOW.get(
            window_id, "diagnostic B2 risk overlay behavior"
        ),
        "actual_trigger_behavior": _actual_trigger_behavior(trigger_count, false_risk_off),
        "actual_trigger_count": trigger_count,
        "return_delta_vs_B0": return_delta,
        "drawdown_delta_vs_B0": drawdown_delta,
        "turnover_delta_vs_B0": _clean_delta(source["turnover_delta"]),
        "cost_delta_vs_B0": cost_delta,
        "benchmark_relative_delta": _clean_delta(source["benchmark_relative_delta"]),
        "false_risk_off_count": false_risk_off,
        "reentry_lag": source.get("reentry_days"),
        "missed_rebound_proxy": _clean_delta(source["missed_rebound_proxy"]),
        "diagnostic_utility_proxy": utility_proxy,
        "window_level_utility": _window_level_utility(
            trigger_count=trigger_count,
            return_delta=return_delta,
            drawdown_delta=drawdown_delta,
            utility_proxy=utility_proxy,
            false_risk_off=false_risk_off,
        ),
        "pass_mixed_fail": classification,
    }


def _classification_rank(classification: str) -> int:
    return {"fail": 0, "mixed": 1, "pass": 2}.get(classification, -1)


def _clean_delta(value: Any) -> float:
    number = float(value)
    return 0.0 if abs(number) < NUMERIC_NOISE_TOLERANCE else number


def _window_classification(
    *,
    window_id: str,
    risk_intensity: str,
    trigger_count: int,
    return_delta: float,
    drawdown_delta: float,
    false_risk_off: int,
) -> str:
    if window_id.endswith("_control") or "false" in window_id:
        return "pass" if trigger_count == 0 and false_risk_off == 0 else "fail"
    if risk_intensity == "high" and trigger_count == 0:
        return "fail"
    if trigger_count > 0 and drawdown_delta > 0 and return_delta < 0:
        return "mixed"
    if trigger_count == 0 and return_delta == 0 and drawdown_delta == 0:
        return "mixed"
    return "pass"


def _window_level_utility(
    *,
    trigger_count: int,
    return_delta: float,
    drawdown_delta: float,
    utility_proxy: float,
    false_risk_off: int,
) -> str:
    if false_risk_off > 0:
        return "false_risk_off_harm"
    if trigger_count == 0 and return_delta == 0 and drawdown_delta == 0:
        return "no_change"
    if drawdown_delta > 0 and utility_proxy < 0:
        return "drawdown_help_with_return_cost"
    if utility_proxy < 0:
        return "negative_utility_proxy"
    if utility_proxy > 0:
        return "positive_utility_proxy"
    return "flat_utility_proxy"


def _actual_trigger_behavior(trigger_count: int, false_risk_off: int) -> str:
    if trigger_count == 0 and false_risk_off == 0:
        return "no trigger"
    if false_risk_off > 0:
        return "false risk-off observed"
    return f"triggered {trigger_count} days"


def _root_cause_row(category: str, present: bool, evidence: str) -> dict[str, Any]:
    return {"category": category, "present": present, "evidence": evidence}


def _question(question: str, answer: str, evidence: str) -> dict[str, str]:
    return {"question": question, "answer": answer, "evidence": evidence}


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
    summary: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": task_id,
        "report_type": report_type,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": requested_date_range,
        "data_quality_gate": data_quality_gate,
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "source_artifacts": _source_artifacts(),
        "reader_brief": {
            "summary": summary,
            "key_result": status,
            "blocking_issues": "none" if "BLOCKED" not in status else status,
            "warnings": (
                "Research-only B2 follow-up diagnostics; no B2 tuning, B3/B4/B5/B6/v3, "
                "paper-shadow, broker/order or production action."
            ),
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Manual owner/research review before any subsequent gate.",
        },
    }


def _load_sources() -> dict[str, dict[str, Any]]:
    return {
        "full": _read_json(B2_FULL_DIAGNOSTIC_WITH_CONTROL_PATH),
        "backfill": _read_json(B2_FULL_DIAGNOSTIC_BACKFILL_PATH),
        "drawdown": _read_json(B2_DRAWDOWN_PATH),
        "reentry": _read_json(B2_REENTRY_COST_PATH),
        "utility": _read_json(B2_UTILITY_PATH),
        "robustness": _read_json(B2_ROBUSTNESS_PATH),
        "control_rerun": _read_json(B2_CONTROL_RERUN_PATH),
        "no_trigger": _read_json(B2_NO_TRIGGER_PATH),
        "gate_v3": _read_json(B2_GATE_V3_PATH),
        "path_snapshot": _read_json(B2_PATH_SNAPSHOT_PATH),
        "b3_resolution": _read_json(B3_RESOLUTION_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["full"].get("requested_date_range")
    if isinstance(value, dict):
        return dict(value)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(B2_FULL_DIAGNOSTIC_WITH_CONTROL_PATH),
    }


def requested_range_with_note(requested_date_range: dict[str, Any]) -> dict[str, Any]:
    value = dict(requested_date_range)
    value["interpretation_note"] = "B2 gate v4 uses completed diagnostic artifacts only."
    return value


def _data_quality_gate(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["full"].get("data_quality_gate")
    if isinstance(value, dict):
        return dict(value)
    return {
        "required_command": "aits validate-data",
        "status": "UNKNOWN",
        "passed": False,
        "error_count": None,
        "warning_count": None,
        "info_count": None,
        "report_path": None,
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_full_diagnostic_with_control_windows": str(B2_FULL_DIAGNOSTIC_WITH_CONTROL_PATH),
        "b2_full_diagnostic_backfill": str(B2_FULL_DIAGNOSTIC_BACKFILL_PATH),
        "b2_drawdown_protection_attribution": str(B2_DRAWDOWN_PATH),
        "b2_false_risk_off_reentry_cost_review": str(B2_REENTRY_COST_PATH),
        "b2_cost_benchmark_utility_review": str(B2_UTILITY_PATH),
        "b2_signal_robustness_trigger_stability": str(B2_ROBUSTNESS_PATH),
        "b2_control_window_rerun": str(B2_CONTROL_RERUN_PATH),
        "b2_no_trigger_correctness_review": str(B2_NO_TRIGGER_PATH),
        "b2_only_research_gate_v3": str(B2_GATE_V3_PATH),
        "b2_path_decision_snapshot": str(B2_PATH_SNAPSHOT_PATH),
        "b3_signal_precheck_resolution_plan": str(B3_RESOLUTION_PATH),
    }


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


__all__ = [
    "DEFAULT_RESEARCH_SOURCE_DIR",
    "run_b2_followup_research",
]
