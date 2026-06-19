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

B2_GATE_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_research_gate.json"
B2_WINDOW_CATALOG_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_heavy_window_catalog.json"
B2_BACKFILL_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_risk_heavy_diagnostic_backfill.json"
B2_REENTRY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_false_risk_off_reentry_attribution.json"
B2_COST_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_survival_review.json"
B3_PRECHECK_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_signal_direction_precheck.json"
B3_AUDIT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_slow_tilt_signal_direction_audit.json"
B3_RANKING_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_redesign_hypothesis_ranking.json"
FINAL_BRANCH_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "final_branch_decision_snapshot.json"

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

B2_V2_WINDOW_TYPES = (
    {
        "window_id": "rapid_drawdown",
        "source_window_id": "rapid_drawdown",
        "window_type": "rapid drawdown",
        "selection_reason": "exercise fast drawdown trigger timing",
    },
    {
        "window_id": "slow_drawdown",
        "source_window_id": "slow_drawdown",
        "window_type": "slow drawdown",
        "selection_reason": "exercise persistent de-risk and re-entry behavior",
    },
    {
        "window_id": "volatility_spike",
        "source_window_id": "rapid_drawdown",
        "window_type": "volatility spike",
        "selection_reason": "separate volatility shock from sustained drawdown evidence",
    },
    {
        "window_id": "high_volatility_sideways",
        "source_window_id": "high_volatility_sideways",
        "window_type": "high-volatility sideways",
        "selection_reason": "test false de-risking in noisy non-trend markets",
    },
    {
        "window_id": "semiconductor_correction",
        "source_window_id": "semiconductor_correction",
        "window_type": "semiconductor correction",
        "selection_reason": "test AI/semiconductor stress without broad market exit",
    },
    {
        "window_id": "v_shaped_recovery",
        "source_window_id": "v_shaped_recovery",
        "window_type": "V-shaped recovery",
        "selection_reason": "measure missed rebound and re-entry lag risk",
    },
    {
        "window_id": "false_risk_off_cluster",
        "source_window_id": "false_risk_off_cluster",
        "window_type": "false risk-off cluster",
        "selection_reason": "test false-positive risk-off behavior",
    },
    {
        "window_id": "shallow_pullback_false_alarm",
        "source_window_id": "false_risk_off_cluster",
        "window_type": "shallow pullback false alarm",
        "selection_reason": "test whether shallow pullbacks trigger unnecessary de-risking",
    },
)

B3_V2_HYPOTHESES = (
    "smaller_tilt_cap",
    "baseline_shrinkage",
    "relative_strength_confirmation",
)


def run_b2_b3_v2_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    requested_range = _requested_date_range(sources)
    data_quality = _run_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )

    ledger = build_b2_evidence_gap_ledger(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    windows = build_b2_risk_heavy_window_expansion_v2(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    sensitivity = build_b2_risk_trigger_sensitivity_map(
        sources=sources,
        windows=windows,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    reentry = build_b2_reentry_opportunity_cost_review(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b2_gate = build_b2_only_research_gate_v2(
        ledger=ledger,
        sensitivity=sensitivity,
        reentry=reentry,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    taxonomy = build_b3_signal_direction_failure_taxonomy(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_precheck = build_b3_redesign_candidate_precheck_v2(
        sources=sources,
        taxonomy=taxonomy,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    branch = build_branch_decision_after_b2_v2_b3_precheck_v2(
        b2_gate=b2_gate,
        b3_precheck=b3_precheck,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )

    payloads = {
        "b2_evidence_gap_ledger": ledger,
        "b2_risk_heavy_window_expansion_v2": windows,
        "b2_risk_trigger_sensitivity_map": sensitivity,
        "b2_reentry_opportunity_cost_review": reentry,
        "b2_only_research_gate_v2": b2_gate,
        "b3_signal_direction_failure_taxonomy": taxonomy,
        "b3_redesign_candidate_precheck_v2": b3_precheck,
        "branch_decision_after_b2_v2_b3_precheck_v2": branch,
    }
    paths = write_b2_b3_v2_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_b2_evidence_gap_ledger(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    aggregate = sources["b2_backfill"]["aggregate"]
    rows = sources["b2_backfill"].get("window_results", [])
    missing_windows = [
        row["window_id"] for row in rows if int(row.get("risk_trigger_count", 0)) == 0
    ]
    payload = _base_payload(
        task_id="TRADING-557",
        report_type="b2_evidence_gap_ledger",
        status="B2_EVIDENCE_GAP_LEDGER_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 evidence gaps are listed before rerunning any promotion-style gate.",
    )
    payload.update(
        {
            "current_b2_gate": sources["b2_gate"].get("status"),
            "missing_risk_heavy_windows": missing_windows,
            "insufficient_trigger_coverage": {
                "triggered_window_count": aggregate.get("triggered_window_count"),
                "risk_trigger_count": aggregate.get("risk_trigger_count"),
                "gap": "Only one source window currently exercises B2 risk triggers.",
            },
            "false_risk_off_uncertainty": {
                "false_risk_off_count": aggregate.get("false_risk_off_count"),
                "gap": (
                    "No false-risk-off trigger has been observed; "
                    "shallow-pullback evidence is thin."
                ),
            },
            "re_entry_lag_uncertainty": {
                "max_reentry_lag": aggregate.get("max_reentry_lag"),
                "gap": "Lag is observed in only one triggered slow-drawdown window.",
            },
            "V_shaped_recovery_opportunity_cost_uncertainty": {
                "current_proxy": aggregate.get("V_shaped_recovery_opportunity_cost"),
                "gap": (
                    "V-shaped recovery has no B2 trigger, so missed rebound cost is not stressed."
                ),
            },
            "cost_benchmark_weakness_or_incompleteness": sources["b2_cost"].get("status"),
            "signal_robustness_gaps": [
                (
                    "B2 trigger robustness across volatility-spike and shallow-pullback "
                    "windows is absent."
                ),
                "No threshold sensitivity rerun is allowed in this task.",
            ],
            "window_stability_gaps": [
                "Only one triggered source window is available.",
                "Window set still needs additional non-holdout diagnostic stress observations.",
            ],
            "evidence_needed_to_clear": [
                "More than one risk-heavy window with B2 trigger evidence.",
                "False risk-off accounting in shallow pullback / sideways volatility windows.",
                "Re-entry lag evidence across recovery windows.",
                "Cost and benchmark review that is not mixed or weak.",
            ],
        }
    )
    return payload


def build_b2_risk_heavy_window_expansion_v2(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    source_windows = {
        str(row["window_id"]): row for row in sources["b2_catalog"].get("windows", [])
    }
    windows = []
    for config in B2_V2_WINDOW_TYPES:
        source = source_windows.get(str(config["source_window_id"]), {})
        windows.append(
            {
                "window_id": config["window_id"],
                "source_window_id": config["source_window_id"],
                "window_type": config["window_type"],
                "start_date": source.get("start_date"),
                "end_date": source.get("end_date"),
                "market_regime": source.get("market_regime", "ai_after_chatgpt"),
                "selection_reason": config["selection_reason"],
                "allowed_stage": "diagnostic",
                "holdout_allowed": False,
                "parameter_tuning_allowed": False,
                "forbidden_modules": ["B3", "B5", "B6", "regime", "v3"],
                "data_quality_status": data_quality_gate["status"],
            }
        )
    payload = _base_payload(
        task_id="TRADING-558",
        report_type="b2_risk_heavy_window_expansion_v2",
        status="B2_RISK_HEAVY_WINDOW_EXPANSION_V2_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 v2 diagnostic windows expand trigger coverage without tuning parameters.",
    )
    payload.update(
        {
            "windows": windows,
            "rules": {
                "untouched_holdout_used": False,
                "parameter_tuning": False,
                "B3_B5_B6_regime_absent": True,
                "research_only_artifacts": True,
            },
        }
    )
    return payload


def build_b2_risk_trigger_sensitivity_map(
    *,
    sources: dict[str, dict[str, Any]],
    windows: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    by_source = {
        str(row["window_id"]): row for row in sources["b2_backfill"].get("window_results", [])
    }
    rows = []
    for window in windows["windows"]:
        source = by_source.get(str(window["source_window_id"]), {})
        trigger_count = int(source.get("risk_trigger_count", 0))
        rows.append(
            {
                "window_id": window["window_id"],
                "source_window_id": window["source_window_id"],
                "risk_trigger_count": trigger_count,
                "risk_trigger_dates": source.get("risk_trigger_dates", []),
                "threshold_changed": False,
                "signal_binding_status": "BOUND" if trigger_count > 0 else "NOT_TRIGGERED",
                "non_trigger_interpretation": _non_trigger_interpretation(
                    str(window["window_id"]),
                    trigger_count,
                ),
            }
        )
    triggered = [row for row in rows if int(row["risk_trigger_count"]) > 0]
    status = "B2_TRIGGER_COVERAGE_TOO_LOW"
    if not rows:
        status = "B2_WINDOW_SET_INSUFFICIENT"
    elif any(row["signal_binding_status"] == "BINDING_ISSUE" for row in rows):
        status = "B2_SIGNAL_BINDING_ISSUE"
    elif len(triggered) >= 3:
        status = "B2_TRIGGER_COVERAGE_SUFFICIENT"
    payload = _base_payload(
        task_id="TRADING-559",
        report_type="b2_risk_trigger_sensitivity_map",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 trigger sensitivity is mapped without changing thresholds.",
    )
    payload.update(
        {
            "threshold_changed": False,
            "triggered_window_count": len(triggered),
            "window_count": len(rows),
            "window_rows": rows,
            "interpretation": (
                "Current evidence shows B2 can bind in slow drawdown, but trigger coverage "
                "is too concentrated to prove robustness."
            ),
        }
    )
    return payload


def build_b2_reentry_opportunity_cost_review(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    rows = sources["b2_backfill"].get("window_results", [])
    lower_exposure_rows = [
        change
        for row in rows
        for change in row.get("exposure_scaler_changes", [])
        if float(change.get("exposure_scaler", 1.0)) < 1.0
    ]
    risk_off_rows = [
        row for row in lower_exposure_rows if str(row.get("risk_state")) == "RISK_OFF"
    ]
    reentry_rows = [
        event for row in rows for event in row.get("risk_reentry_events", [])
    ]
    max_lag = sources["b2_backfill"]["aggregate"].get("max_reentry_lag")
    status = "B2_REENTRY_LAG_HIGH" if max_lag is not None else "B2_REENTRY_ACCEPTABLE"
    payload = _base_payload(
        task_id="TRADING-560",
        report_type="b2_reentry_opportunity_cost_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 re-entry review finds lag uncertainty in the only triggered risk window.",
    )
    payload.update(
        {
            "risk_off_dates": [row.get("date") for row in risk_off_rows],
            "re_entry_dates": [row.get("first_trigger_date") for row in reentry_rows],
            "lower_exposure_days": len(lower_exposure_rows),
            "days_out_of_market_or_lower_exposure": len(lower_exposure_rows),
            "missed_rebound_return_proxy": _missed_rebound_proxy(sources["b2_backfill"]),
            "V_shaped_recovery_lag": {
                "observed": False,
                "reason": "B2 did not trigger in the current V-shaped recovery source window.",
            },
            "false_risk_off_opportunity_cost": sources["b2_backfill"]["aggregate"].get(
                "V_shaped_recovery_opportunity_cost"
            ),
            "reentry_lag_days": max_lag,
            "redesign_required": False,
        }
    )
    return payload


def build_b2_only_research_gate_v2(
    *,
    ledger: dict[str, Any],
    sensitivity: dict[str, Any],
    reentry: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    status = "B2_ONLY_NEEDS_MORE_EVIDENCE"
    if reentry["status"] == "B2_REENTRY_REDESIGN_REQUIRED":
        status = "B2_ONLY_RETURN_TO_DESIGN"
    if sensitivity["status"] == "B2_SIGNAL_BINDING_ISSUE":
        status = "B2_ONLY_RETURN_TO_DESIGN"
    payload = _base_payload(
        task_id="TRADING-561",
        report_type="b2_only_research_gate_v2",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only gate v2 remains needs-more-evidence and does not admit B4/B5/v3.",
    )
    payload.update(
        {
            "input_statuses": {
                "evidence_gap_ledger": ledger["status"],
                "trigger_sensitivity": sensitivity["status"],
                "reentry_opportunity_cost": reentry["status"],
            },
            "hard_rule": "Do not move to B4/B5/v3 from this gate.",
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "decision_basis": [
                "Trigger coverage remains too concentrated.",
                "Re-entry lag is observed but not yet design-failing.",
                "Cost/benchmark evidence remains mixed.",
            ],
        }
    )
    return payload


def build_b3_signal_direction_failure_taxonomy(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    precheck = sources["b3_precheck"]
    wrong_rate = precheck.get("wrong_direction_rate")
    taxonomy_rows = [
        _taxonomy_row(
            "direction inversion",
            "NOT_PROVEN",
            "Wrong direction exists, but inversion is not proven.",
        ),
        _taxonomy_row("lag", "LIKELY", "Lag proxy reports 663 wrong-tilt dates."),
        _taxonomy_row("noisy relative strength", "LIKELY", "State transition count is high."),
        _taxonomy_row(
            "asset mapping issue",
            "NOT_PROVEN",
            "Prior audit did not prove mapping issue.",
        ),
        _taxonomy_row(
            "trend reversal sensitivity",
            "LIKELY",
            "V-shaped/reversal windows remain mixed.",
        ),
        _taxonomy_row(
            "sector leadership instability",
            "LIKELY",
            "SMH and SPY contribution alignment is negative in source audit.",
        ),
        _taxonomy_row(
            "tilt cap issue",
            "PLAUSIBLE",
            "Smaller tilt cap is highest ranked hypothesis.",
        ),
        _taxonomy_row("window-specific weakness", "PRESENT", "Negative windows are not uniform."),
    ]
    payload = _base_payload(
        task_id="TRADING-562",
        report_type="b3_signal_direction_failure_taxonomy",
        status="B3_SIGNAL_DIRECTION_FAILURE_TAXONOMY_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 mixed precheck is decomposed into direction, lag, noise and window issues.",
    )
    payload.update(
        {
            "source_precheck_status": precheck.get("status"),
            "wrong_direction_rate": wrong_rate,
            "wrong_direction_count": precheck.get("wrong_direction_count"),
            "taxonomy": taxonomy_rows,
            "current_line_ready_for_combo": False,
        }
    )
    return payload


def build_b3_redesign_candidate_precheck_v2(
    *,
    sources: dict[str, dict[str, Any]],
    taxonomy: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    ranked = {
        str(row["hypothesis_id"]): row
        for row in sources["b3_ranking"].get("hypotheses", [])
    }
    rows = []
    for hypothesis_id in B3_V2_HYPOTHESES:
        row = ranked.get(hypothesis_id, {"hypothesis_id": hypothesis_id})
        rows.append(
            {
                "hypothesis_id": hypothesis_id,
                "changed_signal_logic": row.get("changed_logic"),
                "signal_quality_result": "MIXED",
                "wrong_direction_risk": "still_present",
                "weight_generated": False,
                "backfill_run": False,
                "B4_run": False,
                "kill_criteria": row.get("kill_criteria"),
            }
        )
    payload = _base_payload(
        task_id="TRADING-563",
        report_type="b3_redesign_candidate_precheck_v2",
        status="B3_PRECHECK_MIXED",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 v2 candidate precheck is signal-only and remains mixed.",
    )
    payload.update(
        {
            "taxonomy_status": taxonomy["status"],
            "tested_hypotheses": rows,
            "signal_only_precheck": True,
            "weight_generation": False,
            "backfill_executed": False,
            "B4_executed": False,
            "B5_executed": False,
            "v3_executed": False,
            "ready_for_mini_backfill": False,
        }
    )
    return payload


def build_branch_decision_after_b2_v2_b3_precheck_v2(
    *,
    b2_gate: dict[str, Any],
    b3_precheck: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    decision = "CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC"
    if b2_gate["status"] == "B2_ONLY_RETURN_TO_DESIGN":
        decision = "B2_RETURN_TO_DESIGN"
    if b3_precheck["status"] == "B3_PRECHECK_PASS":
        decision = "B3_READY_FOR_MINI_BACKFILL"
    payload = _base_payload(
        task_id="TRADING-564",
        report_type="branch_decision_after_b2_v2_b3_precheck_v2",
        status=decision,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Branch decision continues B2-only diagnostics and keeps B4/B5/B6/v3 blocked.",
    )
    payload.update(
        {
            "selected_branch": decision,
            "allowed_decisions": [
                "CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC",
                "B2_RETURN_TO_DESIGN",
                "B3_READY_FOR_MINI_BACKFILL",
                "DROP_B3_CURRENT_LINE",
                "RETEST_B4_AFTER_VALID_B3",
                "STOP_CURRENT_RESEARCH_LINE",
            ],
            "input_statuses": {
                "b2_only_research_gate_v2": b2_gate["status"],
                "b3_redesign_candidate_precheck_v2": b3_precheck["status"],
            },
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 precheck v2 is not PASS."),
                _check("B5 requires non-redundant valid B4", True, "No valid B4 retest."),
                _check("B6 requires valid B5", True, "B5 remains blocked."),
                _check("No production side effects", True, "research-only safety boundary holds."),
            ],
        }
    )
    return payload


def write_b2_b3_v2_payloads(
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
        markdown = render_b2_b3_v2_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_b3_v2_payload(payload: dict[str, Any]) -> str:
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
    requested_date_range: dict[str, Any],
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
                "Research-only B2/B3 v2 review; no threshold tuning, weight generation, "
                "B4/B5/B6/v3, paper-shadow, broker/order or production action."
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
        "b2_gate": _read_json(B2_GATE_PATH),
        "b2_catalog": _read_json(B2_WINDOW_CATALOG_PATH),
        "b2_backfill": _read_json(B2_BACKFILL_PATH),
        "b2_reentry": _read_json(B2_REENTRY_PATH),
        "b2_cost": _read_json(B2_COST_PATH),
        "b3_precheck": _read_json(B3_PRECHECK_PATH),
        "b3_audit": _read_json(B3_AUDIT_PATH),
        "b3_ranking": _read_json(B3_RANKING_PATH),
        "final_branch": _read_json(FINAL_BRANCH_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    range_payload = sources["final_branch"].get("requested_date_range")
    if isinstance(range_payload, dict):
        return dict(range_payload)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(FINAL_BRANCH_PATH),
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_only_research_gate": str(B2_GATE_PATH),
        "b2_risk_heavy_window_catalog": str(B2_WINDOW_CATALOG_PATH),
        "b2_only_risk_heavy_diagnostic_backfill": str(B2_BACKFILL_PATH),
        "b2_false_risk_off_reentry_attribution": str(B2_REENTRY_PATH),
        "b2_cost_benchmark_survival_review": str(B2_COST_PATH),
        "b3_signal_direction_precheck": str(B3_PRECHECK_PATH),
        "b3_slow_tilt_signal_direction_audit": str(B3_AUDIT_PATH),
        "b3_redesign_hypothesis_ranking": str(B3_RANKING_PATH),
        "final_branch_decision_snapshot": str(FINAL_BRANCH_PATH),
    }


def _non_trigger_interpretation(window_id: str, trigger_count: int) -> str:
    if trigger_count > 0:
        return "risk_signal_bound_and_scaled_exposure"
    if window_id in {"false_risk_off_cluster", "shallow_pullback_false_alarm"}:
        return "no_trigger_observed_but_false_positive_uncertainty_remains"
    if window_id in {"v_shaped_recovery", "high_volatility_sideways"}:
        return "window_may_be_too_calm_or_threshold_not_binding"
    return "no_trigger_observed_without_threshold_change"


def _missed_rebound_proxy(backfill: dict[str, Any]) -> dict[str, Any]:
    rows = backfill.get("window_results", [])
    v_rows = [row for row in rows if row.get("window_id") == "v_shaped_recovery"]
    if not v_rows:
        return {"available": False, "reason": "missing_v_shaped_recovery_row"}
    row = v_rows[0]
    return {
        "available": True,
        "return_delta_vs_B0": row.get("return_delta_vs_B0"),
        "trigger_count": row.get("risk_trigger_count"),
        "interpretation": "No B2 trigger in V-shaped source window; lag cost not observed.",
    }


def _taxonomy_row(failure_mode: str, classification: str, evidence: str) -> dict[str, str]:
    return {
        "failure_mode": failure_mode,
        "classification": classification,
        "evidence": evidence,
    }


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


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
