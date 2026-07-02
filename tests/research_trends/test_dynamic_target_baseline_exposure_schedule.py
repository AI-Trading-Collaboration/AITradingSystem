from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_baseline_exposure_schedule,
)


def test_dynamic_target_exposure_schedule_uses_next_session_effective_date() -> None:
    rows = [
        dry_run_wrapper_row(
            row_date="2023-01-06",
            target_asset="QQQ",
        )
    ]

    schedule = build_dynamic_target_baseline_exposure_schedule(wrapper_rows=rows)

    assert schedule[0]["date"] == "2023-01-09"
    assert schedule[0]["source_signal_date"] == "2023-01-06"
    assert schedule[0]["target_exposure"] == 0.7
    assert schedule[0]["risk_asset_exposure"] == 0.7
    assert schedule[0]["known_at_policy"] == "NEXT_SESSION_DECISION_POLICY"
    assert schedule[0]["promotion_allowed"] is False
    assert "target_weight" not in schedule[0]
