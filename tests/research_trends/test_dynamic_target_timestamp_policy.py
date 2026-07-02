from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_timestamp_remediation_policy,
    build_dynamic_target_timestamp_source_priority_matrix,
)


def test_timestamp_source_priority_blocks_filename_for_strict_pit() -> None:
    rows = build_dynamic_target_timestamp_source_priority_matrix()
    filename_row = next(
        row for row in rows if row["timestamp_source_type"] == "filename_or_path_date"
    )

    assert filename_row["allowed_for_strict_pit"] is False
    assert filename_row["allowed_for_pit_approximation"] is True
    assert filename_row["requires_caveat"] is True


def test_timestamp_policy_blocks_future_outcome_inference() -> None:
    policy = build_dynamic_target_timestamp_remediation_policy()

    assert "native_timestamp_copy" in policy["allowed_derivation_modes"]
    assert "future_outcome_inference" in policy["blocked_derivation_modes"]
    assert "market_result_based_timestamp" in policy["blocked_derivation_modes"]
    assert policy["promotion_allowed"] is False
    assert policy["broker_action"] == "none"
