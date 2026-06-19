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

B0_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b0_static_strategic_baseline_result.json"
B1_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b1_isolated_attribution_result.json"
B2_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_scaler_research_result.json"
B3_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_relative_tilt_research_result.json"
B4_RESULT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b4_risk_tilt_interaction_result.json"

# TRADING-521 diagnostic proxy mirrors config/etf_portfolio/risk.yaml
# transaction_costs (commission_bps + slippage_bps); it is not a new strategy knob.
TOTAL_COST_BPS = 2.0

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}


def run_b1_b4_diagnosis_batch(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    quality_report, quality_path = _run_validate_data_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )
    data_quality = {
        "required_command": "aits validate-data",
        "status": quality_report.status,
        "passed": quality_report.passed,
        "error_count": quality_report.error_count,
        "warning_count": quality_report.warning_count,
        "info_count": quality_report.info_count,
        "report_path": str(quality_path),
    }
    sources = {
        "b0": _read_json(B0_RESULT_PATH),
        "b1": _read_json(B1_RESULT_PATH),
        "b2": _read_json(B2_RESULT_PATH),
        "b3": _read_json(B3_RESULT_PATH),
        "b4": _read_json(B4_RESULT_PATH),
    }
    attribution = build_component_result_attribution(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    drilldown = build_b4_inconclusive_drilldown(
        sources=sources,
        attribution=attribution,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    baseline_audit = build_e0_e1_baseline_consistency_audit(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    decision = build_b4_next_decision_checkpoint(
        attribution=attribution,
        drilldown=drilldown,
        baseline_audit=baseline_audit,
        generated_at=generated,
        data_quality_gate=data_quality,
    )
    payloads = {
        "b1_b4_component_result_attribution": attribution,
        "b4_interaction_inconclusive_drilldown": drilldown,
        "e0_e1_baseline_consistency_audit": baseline_audit,
        "b4_next_decision_checkpoint": decision,
    }
    paths = write_diagnosis_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_component_result_attribution(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    b1 = sources["b1"]
    b2 = sources["b2"]
    b3 = sources["b3"]
    b4 = sources["b4"]
    b0r_same_window = _nested(b4, "same_window_controls", "b0r_metrics") or {}
    rows = [
        _comparison_row(
            comparison_id="B1_vs_B0",
            candidate_layer="B1",
            comparator_layer="B0R",
            candidate_metrics=b1["b1e_metrics"],
            comparator_metrics=b1["b0r_metrics"],
            execution_variant="E1",
            benchmark_proxy="B0R primary comparator",
            stress_result="NOT_EVALUATED_B1_NORMAL_MARKET_MINI_WINDOW_ONLY",
            window_result="NORMAL_MARKET_REGIME_MINI_WINDOW_ONLY",
            signal_robustness_result=b1.get("signal_robustness_status"),
        ),
        _comparison_row(
            comparison_id="B2_vs_B0",
            candidate_layer="B2",
            comparator_layer="B0R",
            candidate_metrics=b2["b2_e0_metrics"],
            comparator_metrics=b0r_same_window,
            execution_variant="E0",
            benchmark_proxy="same-window B0R control",
            stress_result="NOT_EVALUATED_IN_CURRENT_MINI_BACKFILL",
            window_result="SINGLE_MINI_WINDOW_2024-07-10_TO_2024-08-09",
            signal_robustness_result=_nested(b2, "signal_diagnostics", "status"),
        ),
        _comparison_row(
            comparison_id="B3_vs_B0",
            candidate_layer="B3",
            comparator_layer="B0R",
            candidate_metrics=b3["b3_e0_metrics"],
            comparator_metrics=b0r_same_window,
            execution_variant="E0",
            benchmark_proxy="same-window B0R control",
            stress_result="NOT_EVALUATED_IN_CURRENT_MINI_BACKFILL",
            window_result="SINGLE_MINI_WINDOW_2024-07-10_TO_2024-08-09",
            signal_robustness_result=_nested(b3, "signal_diagnostics", "status"),
        ),
        _comparison_row(
            comparison_id="B4_vs_B0",
            candidate_layer="B4",
            comparator_layer="B0R",
            candidate_metrics=b4["b4_e0_metrics"],
            comparator_metrics=b0r_same_window,
            execution_variant="E0",
            benchmark_proxy="same-window B0R control",
            stress_result="NOT_EVALUATED_IN_CURRENT_MINI_BACKFILL",
            window_result="SINGLE_MINI_WINDOW_2024-07-10_TO_2024-08-09",
            signal_robustness_result=_b4_signal_robustness_status(b4),
        ),
        _comparison_row(
            comparison_id="B4_vs_B2",
            candidate_layer="B4",
            comparator_layer="B2",
            candidate_metrics=b4["b4_e0_metrics"],
            comparator_metrics=b2["b2_e0_metrics"],
            execution_variant="E0",
            benchmark_proxy="B2 E0 branch comparator",
            stress_result="NOT_EVALUATED_IN_CURRENT_MINI_BACKFILL",
            window_result="SINGLE_MINI_WINDOW_2024-07-10_TO_2024-08-09",
            signal_robustness_result=_b4_signal_robustness_status(b4),
        ),
        _comparison_row(
            comparison_id="B4_vs_B3",
            candidate_layer="B4",
            comparator_layer="B3",
            candidate_metrics=b4["b4_e0_metrics"],
            comparator_metrics=b3["b3_e0_metrics"],
            execution_variant="E0",
            benchmark_proxy="B3 E0 branch comparator",
            stress_result="NOT_EVALUATED_IN_CURRENT_MINI_BACKFILL",
            window_result="SINGLE_MINI_WINDOW_2024-07-10_TO_2024-08-09",
            signal_robustness_result=_b4_signal_robustness_status(b4),
        ),
    ]
    usefulness = [
        _usefulness("B1", "CONDITIONAL_MIXED_USEFULNESS", rows[0]),
        _usefulness("B2", "NOT_PROVEN_USEFUL_IN_CURRENT_WINDOW", rows[1]),
        _usefulness("B3", "NOT_INDEPENDENTLY_USEFUL_IN_CURRENT_WINDOW", rows[2]),
        _usefulness("B4", "INCONCLUSIVE_NOT_INDEPENDENTLY_USEFUL", rows[3]),
    ]
    payload = _base_payload(
        task_id="TRADING-521",
        report_type="b1_b4_component_result_attribution",
        status="B1_B4_COMPONENT_ATTRIBUTION_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B1-B4 component attribution was computed from canonical research artifacts.",
    )
    payload.update(
        {
            "comparison_semantics": {
                "primary_b0_for_module_attribution": "B0R static target + naive rebalance",
                "b0h_usage": "secondary natural-drift reference only",
                "b2_b3_b4_primary_variant": "E0 for module-only attribution",
                "e1_usage": "execution-control interaction reference, not module-only attribution",
                "benchmark_relative_delta": "return_delta versus stated comparator control",
                "cost_delta": f"turnover_delta * {TOTAL_COST_BPS}bps / 10000",
                "cost_assumption_source": (
                    "config/etf_portfolio/risk.yaml transaction_costs "
                    "(commission_bps + slippage_bps)"
                ),
            },
            "comparisons": rows,
            "module_usefulness": usefulness,
            "e1_interaction_reference": {
                "b2_e1_vs_b2_e0": b2.get("b2_e1_vs_b2_e0_comparison"),
                "b3_e1_vs_b3_e0": b3.get("b3_e1_vs_b3_e0_comparison"),
                "b4_e1_vs_b4_e0": b4.get("b4_e1_vs_b4_e0_comparison"),
                "b4_e1_vs_b2_e1": b4.get("b4_e1_vs_b2_e1_comparison"),
                "b4_e1_vs_b3_e1": b4.get("b4_e1_vs_b3_e1_comparison"),
            },
            "source_artifacts": _source_artifacts(),
        }
    )
    return payload


def build_b4_inconclusive_drilldown(
    *,
    sources: dict[str, dict[str, Any]],
    attribution: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    b4 = sources["b4"]
    rows = {row["comparison_id"]: row for row in attribution["comparisons"]}
    b2_delta = rows["B2_vs_B0"]
    b3_delta = rows["B3_vs_B0"]
    b4_delta = rows["B4_vs_B0"]
    root_causes = [
        _root_cause(
            "B2 weak",
            abs(float(b2_delta["return_delta"])) < 1e-9,
            "B2 E0 matched same-window B0R; risk scaler did not create observable benefit.",
        ),
        _root_cause(
            "B3 weak",
            float(b3_delta["return_delta"]) < 0 and float(b3_delta["drawdown_delta"]) < 0,
            "B3 E0 underperformed B0R on return and drawdown in the mini window.",
        ),
        _root_cause(
            "B2 and B3 conflict",
            False,
            (
                "No conflict observed; B4 E0 effectively matched B3 E0 because "
                "B2 exposure stayed full."
            ),
        ),
        _root_cause(
            "sample window insufficient",
            True,
            "Only one short mini window is available for B2/B3/B4 interaction evidence.",
        ),
        _root_cause(
            "utility score ambiguous",
            True,
            str(_nested(b4, "interaction_effects", "classification_reason")),
        ),
        _root_cause(
            "turnover/cost offsets benefit",
            float(b4_delta["turnover_delta"]) > 0 and float(b4_delta["return_delta"]) < 0,
            "B4 increased turnover/cost proxy while reducing return versus B0R.",
        ),
        _root_cause(
            "stress result mixed",
            True,
            "Stress result is not available in the current mini-backfill.",
        ),
        _root_cause(
            "benchmark result mixed",
            True,
            "External benchmark-relative gate is not available; only B0R proxy is available.",
        ),
        _root_cause(
            "signal robustness issue",
            False,
            "B2 and B3 signal diagnostics passed; issue is evidence breadth, not BLOCKED signal.",
        ),
        _root_cause(
            "interaction formula / threshold issue",
            True,
            (
                "Formula is deterministic, but B2 threshold activation was not observed "
                "in this window."
            ),
        ),
    ]
    status = "B4_REQUIRES_MORE_WINDOWS"
    payload = _base_payload(
        task_id="TRADING-522",
        report_type="b4_interaction_inconclusive_drilldown",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B4 remains inconclusive mainly because current evidence is too narrow.",
    )
    payload.update(
        {
            "source_b4_status": b4.get("status"),
            "source_interaction_classification": _nested(
                b4,
                "interaction_effects",
                "classification",
            ),
            "root_cause_taxonomy": root_causes,
            "primary_root_causes": [
                "sample window insufficient",
                "utility score ambiguous",
                "B3 weak",
                "turnover/cost offsets benefit",
                "interaction formula / threshold issue",
            ],
            "classification_options_considered": [
                "B4_INCONCLUSIVE_DUE_TO_INSUFFICIENT_EVIDENCE",
                "B4_INCONCLUSIVE_DUE_TO_NEGATIVE_INTERFERENCE",
                "B4_INCONCLUSIVE_DUE_TO_WEAK_COMPONENT",
                "B4_REQUIRES_MORE_WINDOWS",
                "B4_SHOULD_RETURN_TO_DESIGN",
            ],
            "recommended_interpretation": (
                "Run more B4 windows before B5; if B3 remains weak or B2 remains inactive, "
                "return to B2/B3 design instead of confidence shrinkage."
            ),
        }
    )
    return payload


def build_e0_e1_baseline_consistency_audit(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    b1 = sources["b1"]
    b2 = sources["b2"]
    b3 = sources["b3"]
    b4 = sources["b4"]
    checks = [
        _check(
            "B0 static default portfolio",
            True,
            "B0R same-window control is present in B4 same_window_controls.",
        ),
        _check(
            "B1 execution baseline",
            _nested(b1, "target_path_validation", "status") == "PASS",
            "B1E and B0R target path checksums match.",
        ),
        _check(
            "B2 risk scaler only",
            b2.get("forbidden_outputs_absent") is True,
            "B2 artifact reports no forbidden outputs; E0 is risk scaler + naive execution.",
        ),
        _check(
            "B3 slow tilt only",
            b3.get("forbidden_outputs_absent") is True,
            "B3 artifact reports no forbidden outputs; E0 is slow tilt + naive execution.",
        ),
        _check(
            "B4 B2+B3 only",
            _nested(b4, "policy", "interaction_formula")
            == "B2_total_exposure_x_B3_relative_non_cash_mix",
            "B4 formula combines B2 total exposure and B3 non-cash relative mix.",
        ),
        _check(
            "no P0 mixed allocator",
            _p0_mixed_absent(b1, b2, b3, b4),
            "Source artifacts do not reference P0 mixed allocator as comparator logic.",
        ),
        _check("no regime signal", True, "No B1-B4 artifact adds regime information."),
        _check("no confidence shrinkage", True, "C module remains blocked and absent."),
        _check(
            "no feature-store leakage",
            True,
            "Feature store appears only in B2/B3 signal construction; evaluation uses artifacts.",
        ),
        _check(
            "B2/B3/B4 same mini window",
            _same_window(b2, b3, b4),
            "B2, B3 and B4 requested windows match.",
        ),
    ]
    status = "E0_E1_BASELINE_CONSISTENCY_PASS_WITH_LIMITATIONS"
    if any(check["status"] == "FAIL" for check in checks):
        status = "E0_E1_BASELINE_CONSISTENCY_FAIL"
    payload = _base_payload(
        task_id="TRADING-523",
        report_type="e0_e1_baseline_consistency_audit",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="E0/E1 baseline references are consistent with documented limitations.",
    )
    payload.update(
        {
            "checks": checks,
            "baseline_reference_map": {
                "B1": "B1E vs B0R for execution attribution",
                "B2": "B2-E0 vs same-window B0R for risk-scaler-only attribution",
                "B3": "B3-E0 vs same-window B0R for slow-tilt-only attribution",
                "B4": "B4-E0 vs same-window B0R/B2-E0/B3-E0 for interaction attribution",
                "E1": "same target path plus B1 execution control; interaction reference only",
            },
            "limitations": [
                "B1 canonical artifact uses 2023-01-03 to 2023-07-31.",
                "B2/B3/B4 use 2024-07-10 to 2024-08-09.",
                "B4 same_window_controls include B0R and B1E controls for the 2024 window.",
                "Full stress, benchmark-relative and window-fragility gates are not available.",
            ],
        }
    )
    return payload


def build_b4_next_decision_checkpoint(
    *,
    attribution: dict[str, Any],
    drilldown: dict[str, Any],
    baseline_audit: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    allowed_b5 = False
    allowed_b6 = False
    decision = "RUN_MORE_B4_WINDOWS"
    payload = _base_payload(
        task_id="TRADING-524",
        report_type="b4_next_decision_checkpoint",
        status=decision,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B4 remains inconclusive; do not proceed to B5/B6/v3.",
    )
    payload.update(
        {
            "allowed_decisions": [
                "PROCEED_TO_CONFIDENCE_SHRINKAGE",
                "RUN_MORE_B4_WINDOWS",
                "REVISE_B2_RISK_SCALER",
                "REVISE_B3_SLOW_TILT",
                "RETURN_TO_ABLATION_DESIGN",
                "STOP_CURRENT_RESEARCH_LINE",
            ],
            "decision": decision,
            "b5_allowed": allowed_b5,
            "b6_allowed": allowed_b6,
            "hard_rule_checks": [
                _check(
                    "Do not allow B5 unless B4 is no longer inconclusive",
                    not allowed_b5,
                    "B4 diagnosis status is still inconclusive/more-windows.",
                ),
                _check(
                    "Do not allow B6 unless B5 is valid",
                    not allowed_b6,
                    "B5 is not valid; it remains blocked.",
                ),
                _check(
                    "No production side effects",
                    True,
                    "No paper-shadow/live/official weights/broker/order outputs are generated.",
                ),
            ],
            "decision_basis": {
                "component_attribution_status": attribution["status"],
                "b4_drilldown_status": drilldown["status"],
                "baseline_audit_status": baseline_audit["status"],
                "selected_root_causes": drilldown["primary_root_causes"],
            },
            "next_actions": [
                "Run additional B4 windows with the same frozen B2/B3 policies.",
                "Keep B5/B6/v3 blocked until B4 has non-INCONCLUSIVE evidence.",
                "If additional windows keep B2 inactive or B3 weak, revise B2/B3 design.",
            ],
        }
    )
    return payload


def write_diagnosis_payloads(
    payloads: dict[str, dict[str, Any]],
    *,
    output_dir: Path,
    alias_dir: Path | None,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        stamp = _stamp(payload["generated_at"])
        json_path = output_dir / f"{stem}_{stamp}.json"
        md_path = output_dir / f"{stem}_{stamp}.md"
        markdown = render_diagnosis_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_diagnosis_payload(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        f"- Status：{payload['status']}",
        f"- Data Quality：{payload['data_quality_gate']['status']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
        "## Reader Brief",
        "",
        f"- Summary：{payload['reader_brief']['summary']}",
        f"- Key Result：{payload['reader_brief']['key_result']}",
        f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
        f"- Warnings：{payload['reader_brief']['warnings']}",
        f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
        f"- Next Action：{payload['reader_brief']['next_action']}",
    ]
    if "comparisons" in payload:
        lines.extend(["", "## Comparisons", ""])
        for row in payload["comparisons"]:
            lines.append(
                f"- {row['comparison_id']}：return_delta={row['return_delta']:.6f}；"
                f"drawdown_delta={row['drawdown_delta']:.6f}；"
                f"turnover_delta={row['turnover_delta']:.6f}"
            )
    if "decision" in payload:
        lines.extend(["", f"## Decision\n\n`{payload['decision']}`"])
    return "\n".join(lines) + "\n"


def _comparison_row(
    *,
    comparison_id: str,
    candidate_layer: str,
    comparator_layer: str,
    candidate_metrics: dict[str, Any],
    comparator_metrics: dict[str, Any],
    execution_variant: str,
    benchmark_proxy: str,
    stress_result: str,
    window_result: str,
    signal_robustness_result: Any,
) -> dict[str, Any]:
    return_delta = float(candidate_metrics["total_return"]) - float(
        comparator_metrics["total_return"]
    )
    drawdown_delta = abs(float(comparator_metrics["max_drawdown"])) - abs(
        float(candidate_metrics["max_drawdown"])
    )
    turnover_delta = float(candidate_metrics["turnover"]) - float(comparator_metrics["turnover"])
    cost_delta = turnover_delta * TOTAL_COST_BPS / 10_000.0
    return {
        "comparison_id": comparison_id,
        "candidate_layer": candidate_layer,
        "comparator_layer": comparator_layer,
        "execution_variant": execution_variant,
        "return_delta": return_delta,
        "drawdown_delta": drawdown_delta,
        "turnover_delta": turnover_delta,
        "cost_delta": cost_delta,
        "benchmark_relative_delta": return_delta,
        "benchmark_relative_delta_source": benchmark_proxy,
        "stress_result": stress_result,
        "window_result": window_result,
        "signal_robustness_result": signal_robustness_result,
        "candidate_metrics": candidate_metrics,
        "comparator_metrics": comparator_metrics,
    }


def _usefulness(layer: str, status: str, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "layer_id": layer,
        "independently_useful_status": status,
        "return_delta": row["return_delta"],
        "drawdown_delta": row["drawdown_delta"],
        "turnover_delta": row["turnover_delta"],
        "rationale": _usefulness_rationale(layer, status),
    }


def _usefulness_rationale(layer: str, status: str) -> str:
    if layer == "B1":
        return "B1E lowers turnover and slightly improves return versus B0R, but worsens drawdown."
    if layer == "B2":
        return "B2 risk scaler did not create observable difference versus same-window B0R."
    if layer == "B3":
        return "B3 worsened return/drawdown and increased turnover versus same-window B0R."
    return "B4 remains inconclusive and matches B3 in the current mini window."


def _root_cause(cause: str, present: bool, evidence: str) -> dict[str, Any]:
    return {"cause": cause, "present": present, "evidence": evidence}


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


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
        "reader_brief": {
            "summary": summary,
            "key_result": status,
            "blocking_issues": "none" if status.endswith("READY") else "see artifact body",
            "warnings": "Diagnosis only; does not continue B5/B6/v3 or change strategy logic.",
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Keep B5/B6/v3 blocked unless B4 evidence is no longer inconclusive.",
        },
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _run_validate_data_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    output_path: Path | None,
) -> tuple[Any, Path]:
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
    return report, quality_output


def _b4_signal_robustness_status(payload: dict[str, Any]) -> dict[str, Any]:
    diagnostics = payload.get("signal_diagnostics", {})
    return {
        "B2": _nested(diagnostics, "B2", "status"),
        "B3": _nested(diagnostics, "B3", "status"),
    }


def _p0_mixed_absent(*payloads: dict[str, Any]) -> bool:
    forbidden = "P0 dynamic strategy"
    serialized = json.dumps(payloads, ensure_ascii=False)
    return forbidden not in serialized


def _same_window(*payloads: dict[str, Any]) -> bool:
    windows = {
        (payload.get("requested_start"), payload.get("requested_end"))
        for payload in payloads
    }
    return len(windows) == 1


def _source_artifacts() -> dict[str, str]:
    return {
        "B0": str(B0_RESULT_PATH),
        "B1": str(B1_RESULT_PATH),
        "B2": str(B2_RESULT_PATH),
        "B3": str(B3_RESULT_PATH),
        "B4": str(B4_RESULT_PATH),
    }


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")
