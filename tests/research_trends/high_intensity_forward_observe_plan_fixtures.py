from __future__ import annotations

import json
from pathlib import Path
from typing import Any

__all__ = [
    "build_high_intensity_forward_observe_plan_fixture",
    "read_json",
    "write_json",
]


SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "manual_review_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "real_portfolio_effect": "none",
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_high_intensity_forward_observe_plan_fixture(tmp_path: Path) -> dict[str, Path]:
    dynamic_diagnostics_dir = tmp_path / "dynamic_diagnostics"
    dynamic_dry_run_dir = tmp_path / "dynamic_dry_run"
    readiness_dir = tmp_path / "readiness"
    timestamp_remediation_dir = tmp_path / "timestamp_remediation"
    simulation_policy_dir = tmp_path / "simulation_policy"
    _write_2333_diagnostics(dynamic_diagnostics_dir)
    _write_2332_dynamic_dry_run(dynamic_dry_run_dir)
    _write_readiness(readiness_dir)
    _write_timestamp(timestamp_remediation_dir)
    _write_simulation_policy(simulation_policy_dir)
    return {
        "dynamic_diagnostics_dir": dynamic_diagnostics_dir,
        "dynamic_dry_run_dir": dynamic_dry_run_dir,
        "readiness_dir": readiness_dir,
        "timestamp_remediation_dir": timestamp_remediation_dir,
        "simulation_policy_dir": simulation_policy_dir,
    }


def _write_2333_diagnostics(root: Path) -> None:
    summary = {
        "task_id": "TRADING-2333_DYNAMIC_EXPOSURE_CAP_VS_NO_CAP_DIAGNOSTICS_REVIEW",
        "overall_recommendation": "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE",
        "recommended_policy_action": "HIGH_INTENSITY_ONLY_RISK_CAP",
        "next_task": "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan",
        "data_quality_status": "PASS_WITH_WARNINGS",
        "overbinding_label": "OVERBINDING_BLOCKING",
        "cap_binding_rate": 0.45,
        **SAFETY_FIELDS,
    }
    decision = {
        "overall_recommendation": "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE",
        "next_task_recommendation": (
            "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan"
        ),
        **SAFETY_FIELDS,
    }
    route = {
        "overall_recommendation": "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE",
        "next_task": "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan",
        **SAFETY_FIELDS,
    }
    payloads = {
        "dynamic_exposure_cap_diagnostics_review_summary.json": summary,
        "dynamic_cap_binding_diagnostics_matrix.json": {
            "cap_binding_rate": 0.45,
            **SAFETY_FIELDS,
        },
        "dynamic_overbinding_diagnostics.json": {
            "overbinding_label": "OVERBINDING_BLOCKING",
            **SAFETY_FIELDS,
        },
        "dynamic_exposure_reduction_diagnostics.json": SAFETY_FIELDS,
        "dynamic_return_drawdown_tradeoff_diagnostics.json": {
            "return_proxy_delta": -0.18,
            "drawdown_proxy_delta": 0.04,
            **SAFETY_FIELDS,
        },
        "dynamic_false_cost_missed_upside_diagnostics.json": {
            "false_cost_label": "FALSE_COST_BLOCKING",
            "false_risk_cap_cost_proxy": 0.75,
            **SAFETY_FIELDS,
        },
        "dynamic_downside_protection_diagnostics.json": {
            "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
            "downside_protection_proxy": 0.56,
            **SAFETY_FIELDS,
        },
        "dynamic_turnover_cooldown_diagnostics.json": SAFETY_FIELDS,
        "dynamic_strategy_overlap_diagnostics.json": SAFETY_FIELDS,
        "static_vs_dynamic_exposure_cap_evidence_comparison.json": SAFETY_FIELDS,
        "dynamic_cap_binding_period_attribution.json": {
            "rows": [],
            **SAFETY_FIELDS,
        },
        "dynamic_policy_sensitivity_recommendation_matrix.json": SAFETY_FIELDS,
        "dynamic_exposure_cap_decision_matrix.json": decision,
        "dynamic_2334_task_route.json": route,
        "dynamic_exposure_cap_interpretation_boundary.json": {
            "strict_pit_ready": False,
            **SAFETY_FIELDS,
        },
    }
    for name, payload in payloads.items():
        write_json(root / name, dict(payload))


def _write_2332_dynamic_dry_run(root: Path) -> None:
    rows = []
    for idx in range(30):
        score = 0.40 + idx * 0.02
        triggered = idx >= 6
        rows.append(
            {
                "date": f"2023-01-{idx + 1:02d}",
                "target_asset": "QQQ",
                "risk_cap_triggered": triggered,
                "risk_cap_intensity": "high" if triggered else "none",
                "risk_cap_score": round(score, 6) if triggered else 0.0,
                "scope_active": triggered,
                "signal_direction": "portfolio_level_risk_cap" if triggered else "none",
                "decision_timestamp": f"2023-01-{idx + 1:02d}T00:00:00Z",
                "risk_cap_decision_timestamp": (
                    f"2023-01-{idx + 1:02d}T00:00:00+00:00" if triggered else ""
                ),
                "trigger_source_hash": "fixture_hash",
                **SAFETY_FIELDS,
            }
        )
    payloads = {
        "dynamic_target_exposure_cap_dry_run_summary.json": {
            "task_id": (
                "TRADING-2332_SOURCE_BOUND_EXPOSURE_CAP_DRY_RUN_WITH_DYNAMIC_TARGET_BASELINE"
            ),
            "record_count": 30,
            "cap_binding_days": 12,
            "cap_binding_rate": 0.4,
            "return_proxy_delta": -0.18,
            "drawdown_proxy_delta": 0.04,
            "data_quality_status": "PASS_WITH_WARNINGS",
            "data_quality_gate_executed": True,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_policy": "PIT_APPROXIMATION_READY",
            **SAFETY_FIELDS,
        },
        "dynamic_target_risk_cap_trigger_alignment_matrix.json": {
            "rows": rows,
            "data_quality_status": "PASS_WITH_WARNINGS",
            **SAFETY_FIELDS,
        },
        "dynamic_target_exposure_cap_dry_run_result.json": {"rows": [], **SAFETY_FIELDS},
        "dynamic_target_cap_binding_day_matrix.json": {"rows": [], **SAFETY_FIELDS},
        "dynamic_target_strategy_overlap_report.json": SAFETY_FIELDS,
        "dynamic_target_false_risk_cap_cost_report.json": {
            "false_risk_cap_cost_label": "FALSE_COST_BLOCKING",
            "false_risk_cap_cost_proxy": 0.75,
            **SAFETY_FIELDS,
        },
        "dynamic_target_missed_upside_cost_report.json": SAFETY_FIELDS,
        "dynamic_target_downside_protection_proxy_report.json": {
            "downside_protection_label": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
            "downside_protection_proxy": 0.56,
            **SAFETY_FIELDS,
        },
        "dynamic_target_data_quality_report.json": {
            "data_quality_status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            **SAFETY_FIELDS,
        },
        "dynamic_target_pit_caveat_interpretation_boundary.json": {
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            **SAFETY_FIELDS,
        },
    }
    for name, payload in payloads.items():
        write_json(root / name, dict(payload))


def _write_readiness(root: Path) -> None:
    write_json(
        root / "dynamic_dry_run_pit_caveat_acceptance_report.json",
        {
            "pit_caveat_accepted": True,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            **SAFETY_FIELDS,
        },
    )
    write_json(
        root / "dynamic_dry_run_interpretation_boundary.json",
        {
            "not_trading_instruction": True,
            "simulation_executed": False,
            **SAFETY_FIELDS,
        },
    )


def _write_timestamp(root: Path) -> None:
    write_json(
        root / "dynamic_target_timestamp_pit_caveat_report.json",
        {
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            **SAFETY_FIELDS,
        },
    )
    write_json(
        root / "dynamic_target_known_at_semantics_report.json",
        {
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_approximation_ready": True,
            "strict_pit_ready": False,
            **SAFETY_FIELDS,
        },
    )
    write_json(
        root / "dynamic_target_latency_policy_report.json",
        {
            "decision_delay": "NEXT_TRADING_DAY_DECISION",
            **SAFETY_FIELDS,
        },
    )


def _write_simulation_policy(root: Path) -> None:
    write_json(
        root / "exposure_cap_mechanics_simulation_summary.json",
        {
            "task_id": "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION",
            **SAFETY_FIELDS,
        },
    )
    write_json(root / "exposure_cap_simulation_readiness.json", SAFETY_FIELDS)
