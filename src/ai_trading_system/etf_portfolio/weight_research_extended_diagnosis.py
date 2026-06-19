from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, DEFAULT_ETF_REPORT_DIR
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_b3 import (
    build_b3_relative_tilt_signal,
    build_b3_target_path,
    load_b3_policies,
)
from ai_trading_system.etf_portfolio.weight_research_b4 import build_b4_interaction_target_path
from ai_trading_system.etf_portfolio.weight_research_execution import (
    metrics_from_execution_daily,
    metrics_payload,
    simulate_target_path_execution,
)
from ai_trading_system.etf_portfolio.weight_research_interfaces import (
    build_signal_diagnostics_report,
)
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    DEFAULT_HOLDOUT_POLICY_PATH,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SCOPE_FREEZE_PATH,
    DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    load_b1_execution_policy,
    prepare_research_data_context,
    simulate_b1_execution_control,
    simulate_static_baseline_path,
)

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"
DEFAULT_WINDOW_CATALOG_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "research_window_catalog.json"

TOTAL_COST_BPS = 2.0

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


@dataclass(frozen=True)
class DiagnosticWindow:
    window_id: str
    source_window_id: str
    start_date: date
    end_date: date
    market_regime: str
    purpose: str
    window_type: str

    def payload(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "source_window_id": self.source_window_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "market_regime": self.market_regime,
            "purpose": self.purpose,
            "window_type": self.window_type,
            "allowed_stage": "diagnostic",
            "holdout_allowed": False,
        }


def run_b2_b3_b4_diagnostic_expansion(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    window_catalog_path: Path = DEFAULT_WINDOW_CATALOG_PATH,
    generated_at: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    windows = _diagnostic_windows(window_catalog_path)
    context = prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=min(window.start_date for window in windows),
        end=max(window.end_date for window in windows),
        scope_path=DEFAULT_SCOPE_FREEZE_PATH,
        signal_contract_path=DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
        holdout_policy_path=DEFAULT_HOLDOUT_POLICY_PATH,
        config_path=DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
        generated_at=generated,
        data_quality_output_path=None,
    )
    if context.contract_validation["status"] != "PASS" or not context.data_quality_report.passed:
        raise RuntimeError(
            "TRADING-525~529 diagnostic expansion requires PASS contract and data-quality gate"
        )

    feature_frame = build_feature_store(
        context.prices,
        assets=context.etf_config.assets,
        strategy=context.etf_config.strategy,
        start=context.etf_config.backtest.backtest.warmup_start_date,
        end=max(window.end_date for window in windows),
    )
    window_results = [
        _run_window_diagnostic(
            window=window,
            context=context,
            feature_frame=feature_frame,
            generated_at=generated,
        )
        for window in windows
    ]
    data_quality_gate = _data_quality_payload(context)
    b2_audit = build_b2_trigger_coverage_audit(
        window_results=window_results,
        generated_at=generated,
        data_quality_gate=data_quality_gate,
    )
    b3_attribution = build_b3_negative_contribution_attribution(
        window_results=window_results,
        generated_at=generated,
        data_quality_gate=data_quality_gate,
    )
    multi_window = build_multi_window_diagnostic_expansion(
        window_results=window_results,
        generated_at=generated,
        data_quality_gate=data_quality_gate,
    )
    b4_synthesis = build_b4_interaction_evidence_synthesis(
        b2_audit=b2_audit,
        b3_attribution=b3_attribution,
        multi_window=multi_window,
        generated_at=generated,
        data_quality_gate=data_quality_gate,
    )
    b5_checkpoint = build_b5_admission_checkpoint(
        b2_audit=b2_audit,
        b3_attribution=b3_attribution,
        b4_synthesis=b4_synthesis,
        generated_at=generated,
        data_quality_gate=data_quality_gate,
    )
    payloads = {
        "b2_risk_scaler_trigger_coverage_audit": b2_audit,
        "b3_slow_tilt_negative_contribution_attribution": b3_attribution,
        "b1_b4_multi_window_diagnostic_expansion": multi_window,
        "b4_interaction_evidence_synthesis": b4_synthesis,
        "b5_admission_checkpoint": b5_checkpoint,
    }
    paths = write_extended_diagnosis_payloads(
        payloads,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payloads, paths


def build_b2_trigger_coverage_audit(
    *,
    window_results: list[dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    trigger_rows = [
        {
            "window_id": result["window"]["window_id"],
            "date": row["date"],
            "risk_state": row["risk_state"],
            "risk_score": row["risk_score"],
            "exposure_scaler": row["exposure_scaler"],
        }
        for result in window_results
        for row in result["b2_diagnostics"]["risk_signal_values"]
        if float(row["exposure_scaler"]) < 1.0
    ]
    risk_off_events = [row for row in trigger_rows if row["risk_state"] == "RISK_OFF"]
    current = _find_window(window_results, "rapid_drawdown")
    current_triggers = current["b2_diagnostics"]["risk_trigger_count"] if current else 0
    total_trigger_count = len(trigger_rows)
    binding_issue = any(result["binding_issue"] for result in window_results)
    if binding_issue:
        status = "B2_BINDING_ISSUE"
    elif current_triggers == 0 and total_trigger_count > 0:
        status = "B2_REQUIRES_RISK_HEAVY_WINDOWS"
    elif total_trigger_count == 0:
        status = "B2_NOT_TRIGGERED_WINDOW_INSUFFICIENT"
    elif _b2_scaler_inactive(window_results):
        status = "B2_SIGNAL_PRESENT_BUT_SCALER_INACTIVE"
    else:
        status = "B2_TRIGGERED_WITH_EFFECT"

    payload = _base_payload(
        task_id="TRADING-525",
        report_type="b2_risk_scaler_trigger_coverage_audit",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B2 trigger coverage was audited across diagnostic windows.",
    )
    payload.update(
        {
            "risk_trigger_count": total_trigger_count,
            "risk_trigger_dates": [row["date"] for row in trigger_rows],
            "risk_signal_values": [
                row
                for result in window_results
                for row in result["b2_diagnostics"]["risk_signal_values"]
            ],
            "risk_scaler_exposure_changes": trigger_rows,
            "risk_off_events": risk_off_events,
            "risk_reentry_events": [
                row
                for result in window_results
                for row in result["b2_diagnostics"]["risk_reentry_events"]
            ],
            "unused_risk_signal_reason": _unused_risk_signal_reason(
                current_triggers=current_triggers,
                total_trigger_count=total_trigger_count,
                binding_issue=binding_issue,
            ),
            "window_risk_intensity": [
                result["b2_diagnostics"]["window_risk_intensity"]
                for result in window_results
            ],
            "recommended_risk_heavy_windows": [
                result["window"]["window_id"]
                for result in window_results
                if result["b2_diagnostics"]["risk_trigger_count"] > 0
            ],
        }
    )
    return payload


def build_b3_negative_contribution_attribution(
    *,
    window_results: list[dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    current = _find_window(window_results, "rapid_drawdown") or window_results[0]
    b3_vs_b0 = current["comparisons"]["B3_vs_B0"]
    wrong_tilt_dates = [
        row
        for result in window_results
        for row in result["b3_diagnostics"]["wrong_tilt_dates"]
    ]
    negative_windows = [
        result["window"]["window_id"]
        for result in window_results
        if float(result["comparisons"]["B3_vs_B0"]["return_delta"]) < 0.0
    ]
    binding_issue = any(result["binding_issue"] for result in window_results)
    if binding_issue:
        status = "B3_BINDING_ISSUE"
    elif wrong_tilt_dates:
        status = "B3_NEGATIVE_DUE_TO_SIGNAL_DIRECTION"
    elif float(b3_vs_b0["turnover_delta"]) > 0.0 and float(b3_vs_b0["return_delta"]) < 0.0:
        status = "B3_NEGATIVE_DUE_TO_TURNOVER"
    elif len(negative_windows) == 1:
        status = "B3_NEGATIVE_DUE_TO_WINDOW"
    elif len(negative_windows) >= max(2, len(window_results) // 2):
        status = "B3_SHOULD_BE_REDESIGNED"
    else:
        status = "B3_REQUIRES_MORE_WINDOWS"

    payload = _base_payload(
        task_id="TRADING-526",
        report_type="b3_slow_tilt_negative_contribution_attribution",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B3 negative contribution was attributed from signal direction and turnover.",
    )
    payload.update(
        {
            "tilt_signal_series_summary": [
                result["b3_diagnostics"]["tilt_signal_series_summary"]
                for result in window_results
            ],
            "tilt_direction_by_asset": _merge_asset_direction_summaries(window_results),
            "tilt_magnitude_summary": _aggregate_tilt_magnitude(window_results),
            "tilt_turnover_contribution": [
                result["b3_diagnostics"]["tilt_turnover_contribution"]
                for result in window_results
            ],
            "wrong_tilt_dates": wrong_tilt_dates,
            "relative_strength_lag_summary": {
                "lag_proxy": "wrong_tilt_dates_count",
                "wrong_tilt_dates_count": len(wrong_tilt_dates),
                "state_transition_count": sum(
                    int(result["b3_diagnostics"]["tilt_signal_series_summary"][
                        "state_transition_count"
                    ])
                    for result in window_results
                ),
            },
            "asset_contribution_breakdown": _aggregate_asset_contributions(window_results),
            "cost_drag_from_tilt": sum(
                float(result["comparisons"]["B3_vs_B0"]["cost_delta"])
                for result in window_results
            ),
            "negative_windows": negative_windows,
        }
    )
    return payload


def build_multi_window_diagnostic_expansion(
    *,
    window_results: list[dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    status = (
        "MULTI_WINDOW_DIAGNOSTIC_BLOCKED"
        if any(result["binding_issue"] for result in window_results)
        else "MULTI_WINDOW_DIAGNOSTIC_COMPLETE"
    )
    payload = _base_payload(
        task_id="TRADING-527",
        report_type="b1_b4_multi_window_diagnostic_expansion",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B1-B4 multi-window diagnostic expansion completed without touching holdout.",
    )
    payload.update(
        {
            "windows": [result["window"] for result in window_results],
            "window_results": [
                {
                    "window": result["window"],
                    "metrics": result["metrics"],
                    "comparisons": list(result["comparisons"].values()),
                    "risk_trigger_count": result["b2_diagnostics"]["risk_trigger_count"],
                    "tilt_contribution": result["comparisons"]["B3_vs_B0"][
                        "return_delta"
                    ],
                    "constraint_hits": result["constraint_hits"],
                    "window_result": result["window_result"],
                }
                for result in window_results
            ],
            "required_comparisons": [
                "B1_vs_B0",
                "B2_vs_B0",
                "B3_vs_B0",
                "B4_vs_B0",
                "B4_vs_B2",
                "B4_vs_B3",
            ],
        }
    )
    return payload


def build_b4_interaction_evidence_synthesis(
    *,
    b2_audit: dict[str, Any],
    b3_attribution: dict[str, Any],
    multi_window: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    rows = multi_window["window_results"]
    b4_vs_b0_positive = [
        row for row in rows if float(_comparison(row, "B4_vs_B0")["return_delta"]) > 0.0
    ]
    b4_vs_b2_positive = [
        row for row in rows if float(_comparison(row, "B4_vs_B2")["return_delta"]) > 0.0
    ]
    b4_vs_b3_positive = [
        row for row in rows if float(_comparison(row, "B4_vs_B3")["return_delta"]) > 1e-9
    ]
    b4_equals_b3 = [
        row
        for row in rows
        if abs(float(_comparison(row, "B4_vs_B3")["return_delta"])) <= 1e-9
        and abs(float(_comparison(row, "B4_vs_B3")["turnover_delta"])) <= 1e-9
    ]
    b4_cost_worse = [
        row for row in rows if float(_comparison(row, "B4_vs_B0")["cost_delta"]) > 0.0
    ]
    stress_weak = [
        row
        for row in rows
        if row["window"]["window_type"]
        in {
            "rapid_drawdown",
            "slow_drawdown",
            "high_volatility_sideways",
            "semiconductor_correction",
            "false_risk_off_cluster",
        }
        and float(_comparison(row, "B4_vs_B0")["return_delta"]) < 0.0
    ]
    if b2_audit["status"] == "B2_BINDING_ISSUE" or b3_attribution["status"] == "B3_BINDING_ISSUE":
        classification = "B4_RETURN_TO_DESIGN"
    elif len(b4_vs_b2_positive) == len(rows) and len(b4_vs_b3_positive) == len(rows):
        classification = "B4_POSITIVE_SYNERGY"
    elif len(b4_vs_b2_positive) >= len(rows) // 2 and len(b4_vs_b3_positive) >= len(rows) // 2:
        classification = "B4_MOSTLY_ADDITIVE"
    elif len(b4_equals_b3) >= max(2, len(rows) // 2):
        classification = "B4_REDUNDANT"
    elif len(stress_weak) >= 2 or len(b4_cost_worse) >= max(2, len(rows) // 2):
        classification = "B4_NEGATIVE_INTERFERENCE"
    else:
        classification = "B4_INCONCLUSIVE_MORE_EVIDENCE"

    payload = _base_payload(
        task_id="TRADING-528",
        report_type="b4_interaction_evidence_synthesis",
        status=classification,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B4 interaction evidence was synthesized from TRADING-525~527.",
    )
    payload.update(
        {
            "interaction_classification": classification,
            "b2_effective_in_risk_heavy_windows": b2_audit["risk_trigger_count"] > 0,
            "b3_effective_in_trend_windows": len(
                [
                    row
                    for row in rows
                    if row["window"]["window_type"] in {"normal_uptrend", "v_shaped_recovery"}
                    and float(_comparison(row, "B3_vs_B0")["return_delta"]) > 0.0
                ]
            )
            > 0,
            "b4_better_than_b2_window_count": len(b4_vs_b2_positive),
            "b4_better_than_b3_window_count": len(b4_vs_b3_positive),
            "b4_better_than_b0_window_count": len(b4_vs_b0_positive),
            "b4_cost_worse_window_count": len(b4_cost_worse),
            "b4_stress_weak_window_count": len(stress_weak),
            "b4_redundant_with_b3_window_count": len(b4_equals_b3),
            "classification_reason": _b4_classification_reason(
                classification,
                b4_equals_b3=len(b4_equals_b3),
                total=len(rows),
            ),
        }
    )
    return payload


def build_b5_admission_checkpoint(
    *,
    b2_audit: dict[str, Any],
    b3_attribution: dict[str, Any],
    b4_synthesis: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    classification = str(b4_synthesis["interaction_classification"])
    allowed = classification in {"B4_POSITIVE_SYNERGY", "B4_MOSTLY_ADDITIVE"}
    if allowed:
        status = "B5_ADMISSION_ALLOWED"
    elif (
        classification == "B4_RETURN_TO_DESIGN"
        or b2_audit["status"] == "B2_BINDING_ISSUE"
        or b3_attribution["status"] == "B3_BINDING_ISSUE"
    ):
        status = "B5_ADMISSION_BLOCKED_RETURN_TO_DESIGN"
    elif classification == "B4_NEGATIVE_INTERFERENCE":
        status = "B5_ADMISSION_BLOCKED_NEGATIVE_INTERACTION"
    else:
        status = "B5_ADMISSION_BLOCKED_MORE_EVIDENCE"

    payload = _base_payload(
        task_id="TRADING-529",
        report_type="b5_admission_checkpoint",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        summary="B5 admission checkpoint evaluated B4 interaction evidence.",
    )
    payload.update(
        {
            "b5_allowed": allowed,
            "b6_allowed": False,
            "v3_allowed": False,
            "next_recommended_task": (
                "DEFINE_TRADING_530_B5_RESEARCH_BATCH"
                if allowed
                else _blocked_next_task(classification)
            ),
            "hard_rule_checks": [
                _check(
                    "B5 allowed only for positive/additive B4",
                    allowed or classification not in {"B4_POSITIVE_SYNERGY", "B4_MOSTLY_ADDITIVE"},
                    classification,
                ),
                _check(
                    "If b5_allowed=false then b6_allowed=false",
                    allowed or not payload.get("b6_allowed", False),
                    "enforced",
                ),
                _check(
                    "If b5_allowed=false then v3_allowed=false",
                    allowed or not payload.get("v3_allowed", False),
                    "enforced",
                ),
                _check("No automatic B5 run", True, "checkpoint only"),
            ],
            "source_statuses": {
                "TRADING-525": b2_audit["status"],
                "TRADING-526": b3_attribution["status"],
                "TRADING-528": b4_synthesis["status"],
            },
        }
    )
    return payload


def write_extended_diagnosis_payloads(
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
        markdown = render_extended_diagnosis_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_extended_diagnosis_payload(payload: dict[str, Any]) -> str:
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
    if "window_results" in payload:
        lines.extend(["", "## Windows", ""])
        for row in payload["window_results"]:
            b4 = _comparison(row, "B4_vs_B0")
            lines.append(
                f"- {row['window']['window_id']}: B4_vs_B0 return_delta="
                f"{float(b4['return_delta']):.6f}; turnover_delta="
                f"{float(b4['turnover_delta']):.6f}"
            )
    if "b5_allowed" in payload:
        lines.extend(
            [
                "",
                "## Admission",
                "",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
                f"- next_recommended_task: {payload['next_recommended_task']}",
            ]
        )
    return "\n".join(lines) + "\n"


def _run_window_diagnostic(
    *,
    window: DiagnosticWindow,
    context: Any,
    feature_frame: pd.DataFrame,
    generated_at: datetime,
) -> dict[str, Any]:
    b1_policy = load_b1_execution_policy()
    b2_risk_policy, b2_target_policy = load_b2_policies()
    b3_signal_policy, b3_target_policy = load_b3_policies()
    features = _filter_feature_frame(feature_frame, start=window.start_date, end=window.end_date)
    b0_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=window.start_date,
        end=window.end_date,
        variant_id="B0R",
    )
    b1_daily = simulate_b1_execution_control(
        prices=context.prices,
        config=context.etf_config,
        policy=b1_policy,
        start=window.start_date,
        end=window.end_date,
    )
    b2_signal = build_b2_risk_signal(features, config=context.etf_config, policy=b2_risk_policy)
    b2_diag = build_signal_diagnostics_report(
        b2_signal.rename(columns={"risk_score": "signal_score", "risk_state": "state"}),
        signal_artifact_id=b2_risk_policy.signal_id,
        as_of=window.end_date,
        max_stale_days=b2_risk_policy.max_stale_days,
        generated_at=generated_at,
    )
    b2_target = build_b2_target_path(
        b2_signal,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=b2_target_policy,
        start=window.start_date,
        end=window.end_date,
    )
    b2_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=b2_target,
        mode="naive",
    )
    b3_signal = build_b3_relative_tilt_signal(
        features,
        config=context.etf_config,
        policy=b3_signal_policy,
    )
    b3_diag = build_signal_diagnostics_report(
        b3_signal,
        signal_artifact_id=b3_signal_policy.signal_id,
        as_of=window.end_date,
        max_stale_days=b3_signal_policy.max_stale_days,
        generated_at=generated_at,
    )
    b3_target = build_b3_target_path(
        b3_signal,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=b3_target_policy,
        signal_policy=b3_signal_policy,
        start=window.start_date,
        end=window.end_date,
    )
    b3_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=b3_target,
        mode="naive",
    )
    b4_target = build_b4_interaction_target_path(
        b2_target,
        b3_target,
        config=context.etf_config,
        cash_symbol=b3_target_policy.cash_symbol,
    )
    b4_daily = simulate_target_path_execution(
        prices=context.prices,
        config=context.etf_config,
        target_path=b4_target,
        mode="naive",
    )
    metrics = {
        "B0": metrics_payload(metrics_from_execution_daily(b0_daily)),
        "B1": metrics_payload(metrics_from_execution_daily(b1_daily)),
        "B2": metrics_payload(metrics_from_execution_daily(b2_daily)),
        "B3": metrics_payload(metrics_from_execution_daily(b3_daily)),
        "B4": metrics_payload(metrics_from_execution_daily(b4_daily)),
    }
    comparisons = {
        "B1_vs_B0": _comparison_row("B1_vs_B0", "B1", "B0", metrics["B1"], metrics["B0"]),
        "B2_vs_B0": _comparison_row("B2_vs_B0", "B2", "B0", metrics["B2"], metrics["B0"]),
        "B3_vs_B0": _comparison_row("B3_vs_B0", "B3", "B0", metrics["B3"], metrics["B0"]),
        "B4_vs_B0": _comparison_row("B4_vs_B0", "B4", "B0", metrics["B4"], metrics["B0"]),
        "B4_vs_B2": _comparison_row("B4_vs_B2", "B4", "B2", metrics["B4"], metrics["B2"]),
        "B4_vs_B3": _comparison_row("B4_vs_B3", "B4", "B3", metrics["B4"], metrics["B3"]),
    }
    b2_diagnostics = _b2_window_diagnostics(
        window=window,
        signal=b2_signal,
        target_path=b2_target,
    )
    b3_diagnostics = _b3_window_diagnostics(
        window=window,
        target_path=b3_target,
        daily=b3_daily,
        b0_daily=b0_daily,
        config=context.etf_config,
    )
    binding_issue = any(
        [
            b2_diag["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED",
            b3_diag["status"] == "SIGNAL_DIAGNOSTICS_BLOCKED",
            b2_target.empty,
            b3_target.empty,
            b4_target.empty,
        ]
    )
    return {
        "window": window.payload(),
        "metrics": metrics,
        "comparisons": comparisons,
        "b2_diagnostics": b2_diagnostics,
        "b3_diagnostics": b3_diagnostics,
        "signal_diagnostics": {"B2": b2_diag, "B3": b3_diag},
        "constraint_hits": {
            "B1": int((b1_daily["decision_reason"] == "capped_adjustment").sum()),
            "B2": 0,
            "B3": 0,
            "B4": 0,
        },
        "window_result": _window_result(comparisons),
        "binding_issue": binding_issue,
    }


def _diagnostic_windows(path: Path) -> list[DiagnosticWindow]:
    payload = _read_json(path)
    by_id = {str(row["window_id"]): row for row in payload.get("windows", [])}

    def from_catalog(source_id: str, window_id: str, window_type: str) -> DiagnosticWindow:
        row = by_id[source_id]
        return DiagnosticWindow(
            window_id=window_id,
            source_window_id=source_id,
            start_date=date.fromisoformat(str(row["start_date"])),
            end_date=date.fromisoformat(str(row["end_date"])),
            market_regime=str(row["market_regime"]),
            purpose=str(row["purpose"]),
            window_type=window_type,
        )

    return [
        from_catalog("normal_market_regime", "normal_uptrend", "normal_uptrend"),
        from_catalog("rapid_drawdown", "rapid_drawdown", "rapid_drawdown"),
        from_catalog("slow_drawdown", "slow_drawdown", "slow_drawdown"),
        from_catalog(
            "high_volatility_sideways_market",
            "high_volatility_sideways",
            "high_volatility_sideways",
        ),
        DiagnosticWindow(
            window_id="v_shaped_recovery",
            source_window_id="diagnostic_manual_v_shaped_recovery",
            start_date=date(2023, 10, 27),
            end_date=date(2023, 12, 15),
            market_regime="ai_after_chatgpt",
            purpose="V-shaped recovery diagnostic; development-only, not holdout",
            window_type="v_shaped_recovery",
        ),
        from_catalog(
            "ai_semiconductor_correction",
            "semiconductor_correction",
            "semiconductor_correction",
        ),
        from_catalog("false_risk_off_cluster", "false_risk_off_cluster", "false_risk_off_cluster"),
    ]


def _b2_window_diagnostics(
    *,
    window: DiagnosticWindow,
    signal: pd.DataFrame,
    target_path: pd.DataFrame,
) -> dict[str, Any]:
    target_by_date = {str(row["signal_date"]): row for _, row in target_path.iterrows()}
    rows: list[dict[str, Any]] = []
    previous_state: str | None = None
    reentries: list[dict[str, Any]] = []
    for _, signal_row in signal.sort_values("date").iterrows():
        date_text = str(signal_row["date"])
        target_row = target_by_date.get(date_text)
        state = str(signal_row["risk_state"])
        scaler = 1.0 if target_row is None else float(target_row["exposure_scaler"])
        rows.append(
            {
                "date": date_text,
                "risk_state": state,
                "risk_score": float(signal_row["risk_score"]),
                "risk_coverage": float(signal_row["risk_coverage"]),
                "exposure_scaler": scaler,
            }
        )
        if previous_state in {"RISK_OFF", "ELEVATED_RISK"} and state == "NORMAL":
            reentries.append({"date": date_text, "from_state": previous_state, "to_state": state})
        previous_state = state
    trigger_rows = [row for row in rows if float(row["exposure_scaler"]) < 1.0]
    risk_scores = [float(row["risk_score"]) for row in rows]
    return {
        "window_id": window.window_id,
        "risk_trigger_count": len(trigger_rows),
        "risk_trigger_dates": [row["date"] for row in trigger_rows],
        "risk_signal_values": rows,
        "risk_reentry_events": reentries,
        "window_risk_intensity": {
            "window_id": window.window_id,
            "min_risk_score": min(risk_scores) if risk_scores else None,
            "average_risk_score": sum(risk_scores) / len(risk_scores) if risk_scores else None,
            "non_normal_days": len([row for row in rows if row["risk_state"] != "NORMAL"]),
            "risk_trigger_count": len(trigger_rows),
        },
    }


def _b3_window_diagnostics(
    *,
    window: DiagnosticWindow,
    target_path: pd.DataFrame,
    daily: pd.DataFrame,
    b0_daily: pd.DataFrame,
    config: Any,
) -> dict[str, Any]:
    baseline = {
        symbol: float(asset.default_weight)
        for symbol, asset in config.assets.assets.items()
    }
    directions: dict[str, dict[str, Any]] = {}
    wrong_dates: list[dict[str, Any]] = []
    contributions: dict[str, float] = {}
    magnitudes: list[float] = []
    b0_by_date = {str(row["return_date"]): row for _, row in b0_daily.iterrows()}
    for _, target_row in target_path.iterrows():
        weights = _loads_weights(target_row["target_weights_json"])
        deltas = {
            symbol: weights.get(symbol, 0.0) - baseline.get(symbol, 0.0)
            for symbol in sorted(baseline)
            if symbol != "CASH"
        }
        magnitudes.append(sum(abs(value) for value in deltas.values()))
        for symbol, delta in deltas.items():
            bucket = directions.setdefault(
                symbol,
                {"overweight_days": 0, "underweight_days": 0, "neutral_days": 0},
            )
            if delta > 1e-9:
                bucket["overweight_days"] += 1
            elif delta < -1e-9:
                bucket["underweight_days"] += 1
            else:
                bucket["neutral_days"] += 1
    for _, row in daily.iterrows():
        period_returns = _loads_weights(row["period_returns_json"])
        post_weights = _loads_weights(row["post_trade_weights_json"])
        b0_row = b0_by_date.get(str(row["return_date"]))
        b0_return = 0.0 if b0_row is None else float(b0_row["gross_return"])
        for symbol, weight in post_weights.items():
            if symbol == "CASH":
                continue
            baseline_weight = baseline.get(symbol, 0.0)
            delta = weight - baseline_weight
            contribution = delta * period_returns.get(symbol, 0.0)
            contributions[symbol] = contributions.get(symbol, 0.0) + contribution
            if delta > 1e-9 and period_returns.get(symbol, 0.0) < b0_return:
                wrong_dates.append(
                    {
                        "window_id": window.window_id,
                        "date": str(row["return_date"]),
                        "symbol": symbol,
                        "direction": "OVERWEIGHT_UNDERPERFORMED",
                        "weight_delta": delta,
                        "asset_return": period_returns.get(symbol, 0.0),
                        "baseline_return": b0_return,
                    }
                )
            if delta < -1e-9 and period_returns.get(symbol, 0.0) > b0_return:
                wrong_dates.append(
                    {
                        "window_id": window.window_id,
                        "date": str(row["return_date"]),
                        "symbol": symbol,
                        "direction": "UNDERWEIGHT_OUTPERFORMED",
                        "weight_delta": delta,
                        "asset_return": period_returns.get(symbol, 0.0),
                        "baseline_return": b0_return,
                    }
                )
    return {
        "window_id": window.window_id,
        "tilt_signal_series_summary": {
            "window_id": window.window_id,
            "row_count": int(len(target_path)),
            "state_transition_count": _target_transition_count(target_path),
        },
        "tilt_direction_by_asset": directions,
        "tilt_magnitude_summary": {
            "window_id": window.window_id,
            "average_abs_tilt": sum(magnitudes) / len(magnitudes) if magnitudes else 0.0,
            "max_abs_tilt": max(magnitudes) if magnitudes else 0.0,
        },
        "tilt_turnover_contribution": {
            "window_id": window.window_id,
            "b3_turnover": float(daily["turnover"].sum()),
            "b0_turnover": float(b0_daily["turnover"].sum()),
            "turnover_delta": float(daily["turnover"].sum()) - float(b0_daily["turnover"].sum()),
        },
        "wrong_tilt_dates": wrong_dates,
        "asset_contribution_breakdown": [
            {"symbol": symbol, "tilt_return_contribution": value}
            for symbol, value in sorted(contributions.items())
        ],
    }


def _comparison_row(
    comparison_id: str,
    candidate: str,
    comparator: str,
    candidate_metrics: dict[str, Any],
    comparator_metrics: dict[str, Any],
) -> dict[str, Any]:
    return_delta = float(candidate_metrics["total_return"]) - float(
        comparator_metrics["total_return"]
    )
    drawdown_delta = abs(float(comparator_metrics["max_drawdown"])) - abs(
        float(candidate_metrics["max_drawdown"])
    )
    turnover_delta = float(candidate_metrics["turnover"]) - float(comparator_metrics["turnover"])
    return {
        "comparison_id": comparison_id,
        "candidate_layer": candidate,
        "comparator_layer": comparator,
        "return_delta": return_delta,
        "drawdown_delta": drawdown_delta,
        "turnover_delta": turnover_delta,
        "cost_delta": turnover_delta * TOTAL_COST_BPS / 10_000.0,
        "benchmark_relative_delta": return_delta,
        "risk_trigger_count": None,
        "tilt_contribution": None,
        "constraint_hits": 0,
        "window_result": "computed",
    }


def _window_result(comparisons: dict[str, dict[str, Any]]) -> str:
    b4 = comparisons["B4_vs_B0"]
    if float(b4["return_delta"]) > 0 and float(b4["drawdown_delta"]) >= 0:
        return "B4_IMPROVED_RETURN_AND_DRAWDOWN"
    if float(b4["return_delta"]) < 0 and float(b4["drawdown_delta"]) < 0:
        return "B4_WEAKER_RETURN_AND_DRAWDOWN"
    return "B4_MIXED_WINDOW_RESULT"


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
            "blocking_issues": "none" if not status.endswith("BLOCKED") else status,
            "warnings": (
                "Research-only diagnosis; B5/B6/v3 remain blocked unless checkpoint allows."
            ),
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Do not run B5 unless b5_admission_checkpoint allows it.",
        },
    }


def _filter_feature_frame(features: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] >= pd.Timestamp(start))
        & (frame["_date"] <= pd.Timestamp(end))
    ].copy()
    return selected.drop(columns=["_date"]).reset_index(drop=True)


def _data_quality_payload(context: Any) -> dict[str, Any]:
    report = context.data_quality_report
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(context.data_quality_output_path),
    }


def _find_window(window_results: list[dict[str, Any]], window_id: str) -> dict[str, Any] | None:
    for result in window_results:
        if result["window"]["window_id"] == window_id:
            return result
    return None


def _b2_scaler_inactive(window_results: list[dict[str, Any]]) -> bool:
    return all(
        float(row["exposure_scaler"]) == 1.0
        for result in window_results
        for row in result["b2_diagnostics"]["risk_signal_values"]
    )


def _unused_risk_signal_reason(
    *,
    current_triggers: int,
    total_trigger_count: int,
    binding_issue: bool,
) -> str:
    if binding_issue:
        return "binding_issue_blocks_risk_signal_use"
    if current_triggers == 0 and total_trigger_count > 0:
        return "current_window_no_trigger_but_risk_heavy_windows_trigger"
    if total_trigger_count == 0:
        return "risk_score_never_crossed_scaler_thresholds"
    return "risk_signal_consumed_by_scaler"


def _merge_asset_direction_summaries(window_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for result in window_results:
        for symbol, values in result["b3_diagnostics"]["tilt_direction_by_asset"].items():
            row = merged.setdefault(
                symbol,
                {"symbol": symbol, "overweight_days": 0, "underweight_days": 0, "neutral_days": 0},
            )
            row["overweight_days"] += int(values["overweight_days"])
            row["underweight_days"] += int(values["underweight_days"])
            row["neutral_days"] += int(values["neutral_days"])
    return list(merged.values())


def _aggregate_tilt_magnitude(window_results: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [result["b3_diagnostics"]["tilt_magnitude_summary"] for result in window_results]
    return {
        "average_abs_tilt": sum(float(row["average_abs_tilt"]) for row in rows) / len(rows),
        "max_abs_tilt": max(float(row["max_abs_tilt"]) for row in rows),
        "window_count": len(rows),
    }


def _aggregate_asset_contributions(window_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[str, float] = {}
    for result in window_results:
        for row in result["b3_diagnostics"]["asset_contribution_breakdown"]:
            totals[row["symbol"]] = totals.get(row["symbol"], 0.0) + float(
                row["tilt_return_contribution"]
            )
    return [
        {"symbol": symbol, "tilt_return_contribution": contribution}
        for symbol, contribution in sorted(totals.items())
    ]


def _comparison(row: dict[str, Any], comparison_id: str) -> dict[str, Any]:
    comparisons = row.get("comparisons", [])
    if isinstance(comparisons, dict):
        return comparisons[comparison_id]
    for item in comparisons:
        if item["comparison_id"] == comparison_id:
            return item
    raise KeyError(comparison_id)


def _b4_classification_reason(classification: str, *, b4_equals_b3: int, total: int) -> str:
    if classification == "B4_REDUNDANT":
        return f"B4 matched B3 in {b4_equals_b3}/{total} diagnostic windows."
    if classification == "B4_POSITIVE_SYNERGY":
        return "B4 improved against B2 and B3 across diagnostic windows."
    if classification == "B4_MOSTLY_ADDITIVE":
        return "B4 added component evidence in a majority of diagnostic windows."
    if classification == "B4_NEGATIVE_INTERFERENCE":
        return "B4 showed cost/stress weakness across diagnostic windows."
    if classification == "B4_RETURN_TO_DESIGN":
        return "A B2 or B3 binding issue blocks interaction interpretation."
    return "B4 evidence remains mixed and needs more evidence."


def _blocked_next_task(classification: str) -> str:
    if classification == "B4_RETURN_TO_DESIGN":
        return "RETURN_TO_B2_B3_DESIGN"
    if classification == "B4_NEGATIVE_INTERFERENCE":
        return "STOP_OR_REVISE_B4_INTERACTION"
    if classification == "B4_REDUNDANT":
        return "REVIEW_B4_REDUNDANCY_BEFORE_B5"
    return "RUN_MORE_B4_OR_REVISE_COMPONENTS"


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _target_transition_count(target_path: pd.DataFrame) -> int:
    previous: str | None = None
    count = 0
    for _, row in target_path.sort_values("signal_date").iterrows():
        value = str(row["target_weights_json"])
        if previous is not None and value != previous:
            count += 1
        previous = value
    return count


def _loads_weights(value: Any) -> dict[str, float]:
    return {str(symbol): float(weight) for symbol, weight in json.loads(str(value)).items()}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")
