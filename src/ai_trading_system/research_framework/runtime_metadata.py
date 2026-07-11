from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY

PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "market_data_experiment_run",
    "historical_screen_run",
    "pit_replay_run",
    "pit_replay_executed",
    "backtest_run",
    "scoring_run",
    "daily_report_run",
    "fresh_market_data_read",
    "fresh_outcome_data_read",
    "forward_aging_observation_started",
    "forward_aging_observation_written",
    "candidate_tracking_started",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "outcome_backfilled",
    "outcome_store_mutated",
    "paper_shadow_candidate_found",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "paper_shadow_approved",
    "paper_shadow_schedule_enabled",
    "paper_shadow_daily_job_run",
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "production_enabled",
    "production_allowed",
    "broker_enabled",
    "broker_action_enabled",
    "broker_order_generated",
    "daily_report_generated",
    "new_feature_generated",
    "new_signal_generated",
    "generated_signal",
    "generated_trading_advice",
    "trading_advice_generated",
    "actionable_allocation_generated",
    "portfolio_weight_mutated",
    "automatic_execution_allowed",
)


def with_pit_replay_observe_only_runtime_metadata(
    payload: Mapping[str, Any],
    *,
    source_validation_errors: list[str],
    as_of_date: date | None,
    task_register_id: str,
    report_type: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Add the fixed observe-only runtime contract used by PIT replay closure reports."""
    enriched = dict(payload)
    enriched.update(
        {
            "as_of": str(as_of_date) if as_of_date else enriched.get("as_of"),
            "generated_at": generated_at or _utc_now_iso(),
            "market_regime": AI_REGIME_SUMMARY["market_regime"],
            "market_regime_summary": dict(AI_REGIME_SUMMARY),
            "source_validation_errors": source_validation_errors,
            "source_validation_error_count": len(source_validation_errors),
            "manual_review_required": True,
            "manual_review_only": True,
            "observe_only": True,
            "task_register_id": task_register_id,
            "report_type": report_type,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    for field in PIT_REPLAY_OBSERVE_ONLY_SAFETY_FALSE_FIELDS:
        enriched[field] = False
    return enriched


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
