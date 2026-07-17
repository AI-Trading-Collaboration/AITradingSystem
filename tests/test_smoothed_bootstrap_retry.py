from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    build_model_target_fixture,
    run_smoothed_forward_ops_chain_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def test_smoothed_bootstrap_retry_blocks_when_preflight_stale(tmp_path) -> None:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")

    retry = system_target.run_smoothed_bootstrap_retry(
        requested_as_of=date(2026, 1, 20),
        output_dir=tmp_path / "smoothed_bootstrap_retry",
        preflight_dir=tmp_path / "smoothed_data_preflight",
        model_target_dir=tmp_path / "model_target",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    summary = retry["retry_summary"]
    preflight = retry["retry_preflight_result"]
    steps = retry["retry_steps"]["steps"]

    assert preflight["preflight_status"] == "BLOCKED_STALE_DATA"
    assert {"prices_stale", "rates_stale"}.issubset(preflight["blocking_errors"])
    assert summary["retry_status"] == "BLOCKED"
    assert summary["updated_windows"] == 0
    assert summary["can_execute_switch"] is False
    assert not any(row["step"] == "outcome_update" and row["status"] == "PASS" for row in steps)
    assert summary["broker_action_allowed"] is False
    assert summary["production_effect"] == "none"

    check = system_target.validate_smoothed_bootstrap_retry_artifact(
        retry_id=retry["retry_id"],
        output_dir=tmp_path / "smoothed_bootstrap_retry",
    )
    assert check["status"] == "PASS"


@with_artifact_validation_session
def test_smoothed_bootstrap_retry_runs_full_chain_when_preflight_ready(tmp_path) -> None:
    build_model_target_fixture(tmp_path)
    ops = run_smoothed_forward_ops_chain_fixture(tmp_path)
    # The ops-chain artifacts bind tmp_path/market_cache as immutable evidence.
    # Keep retry input isolated so replay cannot mutate its upstream lineage.
    prices_path, rates_path = write_market_cache(tmp_path / "retry_market_cache")
    generated_at = max(
        datetime.fromisoformat(ops[key]["manifest"]["generated_at"])
        for key in ("binding", "switch_plan", "recorded_owner_promotion")
    ) + timedelta(seconds=1)

    retry = system_target.run_smoothed_bootstrap_retry(
        requested_as_of=EVALUATION_AS_OF,
        binding_id=ops["binding"]["binding_id"],
        switch_plan_id=ops["switch_plan"]["switch_plan_id"],
        owner_promotion_id=ops["recorded_owner_promotion"]["decision_id"],
        output_dir=tmp_path / "smoothed_bootstrap_retry",
        preflight_dir=tmp_path / "smoothed_data_preflight",
        model_target_dir=tmp_path / "model_target",
        emission_dir=tmp_path / "smoothed_daily_emission",
        due_dir=tmp_path / "smoothed_outcome_due",
        update_dir=tmp_path / "smoothed_outcome_update",
        classification_dir=tmp_path / "smoothed_forward_classification",
        binding_dir=tmp_path / "smoothed_forward_binding",
        progress_dir=tmp_path / "smoothed_forward_progress_retry",
        dashboard_dir=tmp_path / "smoothed_weekly_dashboard_retry",
        monitor_dir=tmp_path / "smoothed_event_monitor_retry",
        switch_plan_dir=tmp_path / "paper_shadow_primary_switch",
        recheck_dir=tmp_path / "smoothed_switch_readiness_retry",
        owner_promotion_dir=tmp_path / "smoothed_owner_promotion",
        renewal_dir=tmp_path / "smoothed_owner_renewal_retry",
        weekly_run_dir=tmp_path / "smoothed_forward_weekly_run",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated_at,
    )

    summary = retry["retry_summary"]
    steps = retry["retry_steps"]["steps"]

    assert retry["retry_preflight_result"]["preflight_status"] in {
        "READY",
        "READY_WITH_WARNINGS",
    }
    assert summary["retry_status"] == "COMPLETED"
    assert summary["emitted_events"] == 0
    assert summary["can_execute_switch"] is False
    assert {row["step"] for row in steps} >= {
        "daily_emission",
        "outcome_due_scan",
        "outcome_update",
        "forward_classification",
        "weekly_dashboard",
    }
    assert any(row["step"] == "outcome_update" and row["status"] == "PASS" for row in steps)

    check = system_target.validate_smoothed_bootstrap_retry_artifact(
        retry_id=retry["retry_id"],
        output_dir=tmp_path / "smoothed_bootstrap_retry",
    )
    assert check["status"] == "PASS"
