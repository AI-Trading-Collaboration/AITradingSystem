from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    TARGET_AS_OF,
    build_model_target_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_daily_emission_generates_active_forward_event(tmp_path) -> None:
    target = build_model_target_fixture(tmp_path)
    prices_path, _ = write_market_cache(tmp_path / "market_cache")

    emission = system_target.run_smoothed_daily_emission(
        as_of=TARGET_AS_OF,
        target_id=target["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
    )

    manifest = emission["manifest"]
    event = emission["smoothed_forward_events"][0]
    weights = emission["smoothed_event_weights"]
    validation = weights["weight_validation"]

    assert manifest["emitted_event_count"] == 1
    assert event["event_status"] == "ACTIVE"
    assert event["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert event["baseline_method"] == "limited_adjustment"
    assert event["outcome_windows"] == [1, 5, 10, 20]
    assert emission["smoothed_emission_data_quality"]["future_data_used"] is False
    assert validation["all_weights_sum_to_one"] is True
    assert validation["no_negative_weights"] is True
    assert weights["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"
    assert "Dynamic Rescue Smoothed Daily Emission" in emission["reader_brief_section"]

    check = system_target.validate_smoothed_daily_emission_artifact(
        emission_id=emission["emission_id"],
        output_dir=tmp_path / "smoothed_daily_emission",
    )
    assert check["status"] == "PASS"
