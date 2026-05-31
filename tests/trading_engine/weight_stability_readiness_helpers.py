from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any


def sample_weight_stability_readiness_payload(
    *,
    as_of: date = date(2026, 5, 29),
    status: str = "RECOVERY_FAILED",
    can_run: bool = False,
    blocking_checks: list[str] | None = None,
) -> dict[str, Any]:
    checks = blocking_checks or ["freshness", "backtest_manifest", "price_coverage"]
    return {
        "schema_version": 1,
        "report_type": "weight_stability_readiness",
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-stability-readiness-{as_of.isoformat()}",
            "generated_at": datetime(2026, 5, 31, tzinfo=UTC).isoformat(),
            "status": status,
            "reason": "price_missing_ratio_too_high" if not can_run else "",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": False,
            "recovery_mode": False,
            "source_task": "TRADING-061A",
            "config_path": "config/parameters/weight_tuning_v0_2_stability.yaml",
        },
        "input_context": {
            "stable_tuning_config": "config/parameters/weight_tuning_v0_2_stability.yaml",
            "previous_stable_tuning_status": "INSUFFICIENT_DATA",
            "previous_candidates_backtested": 0,
            "previous_reason": "validate-data gate failed before weight tuning.",
            "previous_artifact": (
                "artifacts/weight_stability/2026-05-28/weight_stability_summary.json"
            ),
        },
        "readiness_checks": {
            "freshness": {
                "status": "MISSING" if not can_run else "OK",
                "tracking_date": as_of.isoformat(),
                "effective_data_date": "2026-05-28",
                "latest_manifest_date": "2026-05-28",
                "tracking_readiness": "cannot_track" if not can_run else "active_tracking",
                "can_continue": can_run,
                "reason": "freshness_status_missing" if not can_run else "",
            },
            "recover_freshness": {
                "status": "COMPLETED_BUT_NOT_RECOVERED" if not can_run else "MISSING",
                "after_freshness_status": "MISSING" if not can_run else "",
                "remaining_limitations": ["market data freshness remains MISSING"]
                if not can_run
                else [],
                "can_continue": can_run,
                "reason": "recover_freshness_completed_but_freshness_not_ok"
                if not can_run
                else "",
            },
            "signal_snapshot": {
                "status": "LIMITED",
                "raw_status": "LIMITED",
                "snapshot_date": "2026-05-28",
                "real_signals": 2,
                "proxy_signals": 1,
                "fallback_signals": 3,
                "missing_signals": 0,
                "failed_signals": 0,
                "can_continue": True,
                "reason": "",
                "warning": "Signal quality is LIMITED; stable tuning remains shadow-only.",
            },
            "backtest_manifest": {
                "status": "FAILED" if not can_run else "LIMITED",
                "can_continue": can_run,
                "reason": "date_coverage_insufficient" if not can_run else "",
                "required_range": {"start": "2022-05-23", "end": as_of.isoformat()},
                "available_range": {"start": as_of.isoformat(), "end": as_of.isoformat()},
            },
            "price_coverage": {
                "status": "FAILED" if not can_run else "OK",
                "can_continue": can_run,
                "reason": "price_missing_ratio_too_high" if not can_run else "",
                "high_missing_ratio_symbols": ["GOOGL", "BRK.B", "SGOV"]
                if not can_run
                else [],
                "missing_symbols": [],
                "special_findings": ["SINGLE_DAY_PRICE_CACHE"] if not can_run else [],
            },
        },
        "stable_tuning_eligibility": {
            "status": status,
            "can_run": can_run,
            "candidates_backtest_allowed": can_run,
            "blocking_checks": checks if not can_run else [],
            "reason": "price_missing_ratio_too_high" if not can_run else "ready",
        },
        "blocking_errors": [
            {"check": check, "status": "FAILED", "reason": "blocked"} for check in checks
        ]
        if not can_run
        else [],
        "recovery_plan": [
            {
                "step": 1,
                "action": "diagnose_backtest_inputs",
                "command": "aits data diagnose-backtest-inputs --latest",
                "reason": "Confirm can_run_shadow_backtest=true.",
                "auto_executed": False,
            }
        ]
        if not can_run
        else [],
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "Stable weight tuning has not entered a valid backtest.",
        },
        "supporting_artifacts": {},
        "output_artifacts": {
            "weight_stability_readiness_summary_json": (
                "artifacts/weight_stability_readiness/2026-05-29/"
                "weight_stability_readiness_summary.json"
            ),
            "weight_stability_readiness_summary_md": (
                "artifacts/weight_stability_readiness/2026-05-29/"
                "weight_stability_readiness_summary.md"
            ),
        },
        "reader_brief": (
            "Stable weight tuning remains blocked before backtest because price coverage "
            "and freshness readiness are not satisfied."
            if not can_run
            else "Stable weight tuning input readiness is restored."
        ),
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "production_config_modified": False,
            "data_quality_gate_lowered": False,
            "mock_data_used": False,
            "synthetic_price_history_generated": False,
            "fallback_signals_relaxed": False,
            "candidate_backtest_run_when_blocked": False,
            "candidate_promotion_triggered": False,
            "broker_action": False,
            "trading_action": False,
        },
    }
