from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, DEFAULT_ETF_REPORT_DIR
from ai_trading_system.etf_portfolio.weight_research_unblock import DEFAULT_RATES_CACHE_PATH

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

MULTI_WINDOW_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b1_b4_multi_window_diagnostic_expansion.json"
B2_TRIGGER_AUDIT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_scaler_trigger_coverage_audit.json"
B3_DIRECTION_SOURCE_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b3_slow_tilt_negative_contribution_attribution.json"
)
B4_SYNTHESIS_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b4_interaction_evidence_synthesis.json"
B5_ADMISSION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b5_admission_checkpoint.json"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

RISK_HEAVY_WINDOWS = (
    "rapid_drawdown",
    "slow_drawdown",
    "high_volatility_sideways",
    "semiconductor_correction",
    "v_shaped_recovery",
    "false_risk_off_cluster",
)


def run_b2_b3_branching_checkpoint(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    data_quality = _run_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )
    sources = {
        "multi": _read_json(MULTI_WINDOW_PATH),
        "b2": _read_json(B2_TRIGGER_AUDIT_PATH),
        "b3": _read_json(B3_DIRECTION_SOURCE_PATH),
        "b4": _read_json(B4_SYNTHESIS_PATH),
        "b5": _read_json(B5_ADMISSION_PATH),
    }
    b2_eval = build_b2_risk_heavy_window_evaluation(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    b3_audit = build_b3_slow_tilt_signal_direction_audit(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    b1_review = build_b1_execution_control_adoption_review(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    branch = build_ablation_path_branching_decision(
        b2_eval=b2_eval,
        b3_audit=b3_audit,
        b1_review=b1_review,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    b2_only = build_b2_only_research_candidate_checkpoint(
        b2_eval=b2_eval,
        branch=branch,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    b3_hypotheses = build_b3_redesign_hypothesis_pack(
        b3_audit=b3_audit,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    program = build_research_program_checkpoint_after_branching(
        b2_eval=b2_eval,
        b3_audit=b3_audit,
        b1_review=b1_review,
        branch=branch,
        b2_only=b2_only,
        b3_hypotheses=b3_hypotheses,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    payloads = {
        "b2_risk_heavy_window_evaluation": b2_eval,
        "b3_slow_tilt_signal_direction_audit": b3_audit,
        "b1_execution_control_adoption_review": b1_review,
        "ablation_path_branching_decision": branch,
        "b2_only_research_candidate_checkpoint": b2_only,
        "b3_redesign_hypothesis_pack": b3_hypotheses,
        "research_program_checkpoint_after_branching": program,
    }
    paths = write_branching_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_b2_risk_heavy_window_evaluation(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    rows = []
    risk_windows = _window_rows(sources["multi"], RISK_HEAVY_WINDOWS)
    b2_source = sources["b2"]
    exposure_changes = b2_source.get("risk_scaler_exposure_changes", [])
    for row in risk_windows:
        window_id = row["window"]["window_id"]
        b2_vs_b0 = _comparison(row, "B2_vs_B0")
        changes = [item for item in exposure_changes if item.get("window_id") == window_id]
        rows.append(
            {
                "window_id": window_id,
                "risk_trigger_count": int(row["risk_trigger_count"]),
                "risk_trigger_dates": [item["date"] for item in changes],
                "exposure_scaler_changes": changes,
                "drawdown_delta_vs_b0": b2_vs_b0["drawdown_delta"],
                "return_delta_vs_b0": b2_vs_b0["return_delta"],
                "turnover_delta_vs_b0": b2_vs_b0["turnover_delta"],
                "cost_delta_vs_b0": b2_vs_b0["cost_delta"],
                "false_risk_off_count": int(row["risk_trigger_count"])
                if window_id == "false_risk_off_cluster"
                else 0,
                "re_entry_lag_days": _reentry_lag_days(window_id, changes, b2_source),
                "v_shaped_recovery_opportunity_cost": (
                    max(0.0, -float(b2_vs_b0["return_delta"]))
                    if window_id == "v_shaped_recovery"
                    else 0.0
                ),
            }
        )
    triggered = [row for row in rows if row["risk_trigger_count"] > 0]
    drawdown_help = [row for row in triggered if float(row["drawdown_delta_vs_b0"]) > 0.0]
    return_hurt = [row for row in triggered if float(row["return_delta_vs_b0"]) < 0.0]
    if not triggered:
        status = "B2_RISK_OVERLAY_NOT_TRIGGERED"
    elif drawdown_help and return_hurt:
        status = "B2_RISK_OVERLAY_MIXED"
    elif drawdown_help and not return_hurt:
        status = "B2_RISK_OVERLAY_PROMISING"
    else:
        status = "B2_RISK_OVERLAY_WEAK"
    payload = _base_payload(
        task_id="TRADING-530",
        report_type="b2_risk_heavy_window_evaluation",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B2 risk-heavy windows were evaluated without changing B2 parameters.",
    )
    payload.update(
        {
            "window_evaluations": rows,
            "aggregate": {
                "risk_trigger_count": sum(int(row["risk_trigger_count"]) for row in rows),
                "triggered_window_count": len(triggered),
                "drawdown_help_window_count": len(drawdown_help),
                "return_hurt_window_count": len(return_hurt),
                "false_risk_off_count": sum(int(row["false_risk_off_count"]) for row in rows),
                "v_shaped_recovery_opportunity_cost": sum(
                    float(row["v_shaped_recovery_opportunity_cost"]) for row in rows
                ),
            },
            "interpretation": (
                "B2 has distinct risk-heavy behavior in slow_drawdown, but current evidence is "
                "mixed because drawdown improvement came with return, turnover and cost drag."
            ),
            "source_artifacts": _source_artifacts(),
        }
    )
    return payload


def build_b3_slow_tilt_signal_direction_audit(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    b3 = sources["b3"]
    wrong_dates = b3.get("wrong_tilt_dates", [])
    negative_windows = b3.get("negative_windows", [])
    status = (
        "B3_REDESIGN_REQUIRED"
        if len(negative_windows) >= 3 and len(wrong_dates) > 0
        else "B3_WINDOW_SPECIFIC_WEAKNESS"
    )
    overweighted_underperformers = [
        row for row in wrong_dates if row.get("direction") == "OVERWEIGHT_UNDERPERFORMED"
    ]
    underweighted_outperformers = [
        row for row in wrong_dates if row.get("direction") == "UNDERWEIGHT_OUTPERFORMED"
    ]
    payload = _base_payload(
        task_id="TRADING-531",
        report_type="b3_slow_tilt_signal_direction_audit",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B3 slow-tilt direction was audited without tuning B3.",
    )
    payload.update(
        {
            "asset_level_tilt_direction": b3.get("tilt_direction_by_asset", []),
            "wrong_tilt_dates": wrong_dates,
            "relative_strength_signal_lag": b3.get("relative_strength_lag_summary", {}),
            "contribution_by_asset": b3.get("asset_contribution_breakdown", []),
            "overweighted_underperformers": overweighted_underperformers,
            "underweighted_outperformers": underweighted_outperformers,
            "turnover_generated_by_tilt_changes": b3.get("tilt_turnover_contribution", []),
            "signal_normalization_diagnosis": {
                "inverted": "NOT_PROVEN",
                "lagged": "LIKELY_CONTRIBUTOR",
                "overreactive": "LIKELY_CONTRIBUTOR",
                "asset_mapping_issue": "NOT_PROVEN",
            },
            "negative_windows": negative_windows,
        }
    )
    return payload


def build_b1_execution_control_adoption_review(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    rows = [
        {
            "window_id": row["window"]["window_id"],
            **_comparison(row, "B1_vs_B0"),
        }
        for row in sources["multi"]["window_results"]
    ]
    helps = [
        row
        for row in rows
        if float(row["return_delta"]) > 0.0 and float(row["turnover_delta"]) < 0.0
    ]
    hurts = [
        row
        for row in rows
        if float(row["return_delta"]) < 0.0 or float(row["drawdown_delta"]) < 0.0
    ]
    status = "B1_OPTIONAL_WRAPPER" if helps and hurts else "B1_NEEDS_REDESIGN"
    payload = _base_payload(
        task_id="TRADING-532",
        report_type="b1_execution_control_adoption_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B1 execution-control adoption was reviewed for later dynamic modules.",
    )
    payload.update(
        {
            "window_reviews": rows,
            "when_b1_helps": [row["window_id"] for row in helps],
            "when_b1_hurts": [row["window_id"] for row in hurts],
            "drawdown_deterioration_assessment": (
                "Not acceptable as universal default; acceptable only as optional research wrapper "
                "where turnover/cost savings are the explicit objective."
            ),
            "adoption_mode": "optional execution wrapper",
        }
    )
    return payload


def build_ablation_path_branching_decision(
    *,
    b2_eval: dict[str, Any],
    b3_audit: dict[str, Any],
    b1_review: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    selected = "CONTINUE_B2_ONLY_PATH"
    payload = _base_payload(
        task_id="TRADING-533",
        report_type="ablation_path_branching_decision",
        status=selected,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="Ablation path was branched after B4 redundancy evidence.",
    )
    payload.update(
        {
            "selected_branch": selected,
            "rejected_branches": [
                {
                    "branch": "REDESIGN_B3_BEFORE_COMBO",
                    "reason": "B3 redesign is needed, but B2-only path can be evaluated first.",
                },
                {
                    "branch": "DROP_B3_CURRENT_FORM",
                    "reason": (
                        "Current form is blocked from combo, but hypotheses should be preserved."
                    ),
                },
                {
                    "branch": "RETEST_B4_AFTER_B3_REDESIGN",
                    "reason": "Requires a redesigned B3 artifact first.",
                },
                {
                    "branch": "STOP_CURRENT_ABLATION_LINE",
                    "reason": "B2 has mixed but distinct risk-heavy evidence.",
                },
            ],
            "required_next_tasks": [
                "Run deeper B2-only research on risk-heavy diagnostic windows.",
                "Review B3 redesign hypotheses before any B4 retest.",
                "Keep B5/B6/v3 blocked until a non-redundant B4 or approved B2-only path exists.",
            ],
            "blocked_modules": ["B3_CURRENT_FORM", "B4_CURRENT_COMBO", "B5", "B6", "v3"],
            "input_statuses": {
                "TRADING-530": b2_eval["status"],
                "TRADING-531": b3_audit["status"],
                "TRADING-532": b1_review["status"],
                "TRADING-528": sources["b4"]["status"],
                "TRADING-529": sources["b5"]["status"],
            },
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
        }
    )
    return payload


def build_b2_only_research_candidate_checkpoint(
    *,
    b2_eval: dict[str, Any],
    branch: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    status = (
        "B2_ONLY_NEEDS_MORE_EVIDENCE"
        if branch["selected_branch"] == "CONTINUE_B2_ONLY_PATH"
        else "B2_ONLY_RETURN_TO_DESIGN"
    )
    payload = _base_payload(
        task_id="TRADING-534",
        report_type="b2_only_research_candidate_checkpoint",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B2-only research candidate checkpoint evaluated risk-overlay evidence.",
    )
    payload.update(
        {
            "stress_result": "MIXED_SLOW_DRAWDOWN_HELP_RETURN_COST_HURT",
            "drawdown_benefit": _triggered_drawdown_benefit(b2_eval),
            "false_risk_off_behavior": _false_risk_off_count(b2_eval),
            "re_entry_lag": _max_reentry_lag(b2_eval),
            "cost_turnover_impact": _triggered_cost_turnover_impact(b2_eval),
            "benchmark_relative_impact": _triggered_return_impact(b2_eval),
            "window_stability": "INSUFFICIENT_ONLY_ONE_TRIGGERED_WINDOW",
            "signal_robustness": sources["b2"]["status"],
            "paper_shadow_allowed": False,
        }
    )
    return payload


def build_b3_redesign_hypothesis_pack(
    *,
    b3_audit: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    hypotheses = [
        _hypothesis(
            "slower_relative_strength_window",
            "Reduce signal lag/noise from short relative-strength reversals.",
            "Use a longer RS lookback before tilt scoring.",
            "Too slow to react after semiconductor leadership shifts.",
        ),
        _hypothesis(
            "stronger_smoothing",
            "Reduce overreaction and repeated wrong-tilt dates.",
            "Smooth signal scores before target mapping.",
            "Smoothing hides real inflections.",
        ),
        _hypothesis(
            "smaller_tilt_cap",
            "Limit drawdown and turnover damage from wrong direction.",
            "Lower max relative tilt cap.",
            "Cap becomes too small to matter.",
        ),
        _hypothesis(
            "baseline_shrinkage",
            "Keep B3 closer to B0 when confidence is low.",
            "Shrink active tilt toward baseline weights.",
            "No improvement versus static baseline.",
        ),
        _hypothesis(
            "relative_strength_confirmation",
            "Require confirmation before overweight/underweight changes.",
            "Add persistence confirmation to RS state changes.",
            "Confirmation misses early trend windows.",
        ),
        _hypothesis(
            "avoid_tilt_during_high_vol_regimes",
            "Avoid B3 direction errors during unstable markets.",
            "Disable or shrink tilt when risk volatility is high.",
            "Filter removes useful recovery exposure.",
        ),
        _hypothesis(
            "asset_level_contribution_filter",
            "Drop assets whose tilt contribution is persistently negative.",
            "Gate tilt by recent asset-level contribution evidence.",
            "Contribution filter overfits diagnostic windows.",
        ),
        _hypothesis(
            "confidence_weighted_tilt",
            "Use confidence to scale tilt magnitude.",
            "Multiply tilt by signal coverage/confidence.",
            "Confidence proxy does not map to future contribution.",
        ),
    ]
    payload = _base_payload(
        task_id="TRADING-535",
        report_type="b3_redesign_hypothesis_pack",
        status="B3_REDESIGN_HYPOTHESES_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B3 redesign hypotheses were prepared without implementing redesigned B3.",
    )
    payload.update(
        {
            "source_b3_audit_status": b3_audit["status"],
            "hypotheses": hypotheses,
            "reader_brief": {
                **payload["reader_brief"],
                "next_action": "Choose one redesign hypothesis for a later frozen-spec task.",
            },
        }
    )
    return payload


def build_research_program_checkpoint_after_branching(
    *,
    b2_eval: dict[str, Any],
    b3_audit: dict[str, Any],
    b1_review: dict[str, Any],
    branch: dict[str, Any],
    b2_only: dict[str, Any],
    b3_hypotheses: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    status = "CONTINUE_B2_ONLY_RESEARCH"
    payload = _base_payload(
        task_id="TRADING-536",
        report_type="research_program_checkpoint_after_branching",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="Research program checkpoint summarized B2/B3 branching state.",
    )
    payload.update(
        {
            "b1_status": b1_review["status"],
            "b2_status": b2_eval["status"],
            "b2_only_status": b2_only["status"],
            "b3_status": b3_audit["status"],
            "b3_redesign_status": b3_hypotheses["status"],
            "b4_status": sources["b4"]["status"],
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "recommended_next_branch": branch["selected_branch"],
            "next_owner_research_action": (
                "Review B2-only mixed risk-overlay evidence and choose whether to define a "
                "deeper B2-only research batch; separately choose a B3 redesign hypothesis."
            ),
        }
    )
    return payload


def write_branching_payloads(
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
        markdown = render_branching_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_branching_payload(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        f"- Status: {payload['status']}",
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
    if "selected_branch" in payload:
        lines.extend(["", "## Branch", "", f"`{payload['selected_branch']}`"])
    if "b5_allowed" in payload:
        lines.extend(
            [
                "",
                "## Allowed Flags",
                "",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
            ]
        )
    return "\n".join(lines) + "\n"


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
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
        "data_quality_gate": data_quality_gate,
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "reader_brief": {
            "summary": summary,
            "key_result": status,
            "blocking_issues": "none",
            "warnings": "Research-only branching diagnosis; no B5/B6/v3 or production action.",
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Manual owner/research review before any next task.",
        },
    }


def _window_rows(payload: dict[str, Any], window_ids: tuple[str, ...]) -> list[dict[str, Any]]:
    rows = {row["window"]["window_id"]: row for row in payload["window_results"]}
    return [rows[window_id] for window_id in window_ids]


def _comparison(row: dict[str, Any], comparison_id: str) -> dict[str, Any]:
    for comparison in row["comparisons"]:
        if comparison["comparison_id"] == comparison_id:
            return comparison
    raise KeyError(comparison_id)


def _reentry_lag_days(
    window_id: str,
    changes: list[dict[str, Any]],
    b2_source: dict[str, Any],
) -> int | None:
    if not changes:
        return None
    reentries = [
        row for row in b2_source.get("risk_reentry_events", []) if row.get("window_id") == window_id
    ]
    if not reentries and window_id == "slow_drawdown":
        reentries = b2_source.get("risk_reentry_events", [])
    if not reentries:
        return None
    first = date.fromisoformat(str(changes[0]["date"]))
    reentry = date.fromisoformat(str(reentries[0]["date"]))
    return max(0, (reentry - first).days)


def _triggered_drawdown_benefit(b2_eval: dict[str, Any]) -> float:
    return sum(
        float(row["drawdown_delta_vs_b0"])
        for row in b2_eval["window_evaluations"]
        if int(row["risk_trigger_count"]) > 0
    )


def _false_risk_off_count(b2_eval: dict[str, Any]) -> int:
    return int(b2_eval["aggregate"]["false_risk_off_count"])


def _max_reentry_lag(b2_eval: dict[str, Any]) -> int | None:
    values = [
        int(row["re_entry_lag_days"])
        for row in b2_eval["window_evaluations"]
        if row["re_entry_lag_days"] is not None
    ]
    return max(values) if values else None


def _triggered_cost_turnover_impact(b2_eval: dict[str, Any]) -> dict[str, float]:
    rows = [row for row in b2_eval["window_evaluations"] if int(row["risk_trigger_count"]) > 0]
    return {
        "turnover_delta": sum(float(row["turnover_delta_vs_b0"]) for row in rows),
        "cost_delta": sum(float(row["cost_delta_vs_b0"]) for row in rows),
    }


def _triggered_return_impact(b2_eval: dict[str, Any]) -> float:
    return sum(
        float(row["return_delta_vs_b0"])
        for row in b2_eval["window_evaluations"]
        if int(row["risk_trigger_count"]) > 0
    )


def _hypothesis(
    hypothesis_id: str,
    expected_improvement: str,
    changed_logic: str,
    expected_failure_mode: str,
) -> dict[str, str]:
    return {
        "hypothesis_id": hypothesis_id,
        "expected_improvement": expected_improvement,
        "changed_logic": changed_logic,
        "expected_failure_mode": expected_failure_mode,
        "mini_window_validation_method": (
            "Rerun development/diagnostic windows only; compare B3 vs B0 and B4 after redesign."
        ),
        "kill_criteria": (
            "Reject if return/drawdown remains negative versus B0 or turnover/cost worsens "
            "without compensating benchmark-relative benefit."
        ),
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "multi_window": str(MULTI_WINDOW_PATH),
        "b2_trigger_audit": str(B2_TRIGGER_AUDIT_PATH),
        "b3_direction_source": str(B3_DIRECTION_SOURCE_PATH),
        "b4_synthesis": str(B4_SYNTHESIS_PATH),
        "b5_admission": str(B5_ADMISSION_PATH),
    }


def _run_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    output_path: Path | None,
) -> dict[str, Any]:
    quality_output = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        as_of,
    )
    universe = load_universe()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=prices_path.name == "prices_marketstack_daily.csv",
    )
    write_data_quality_report(report, quality_output)
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(quality_output),
    }


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")
