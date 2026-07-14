from __future__ import annotations

from datetime import UTC, date, datetime

from dynamic_v3_system_target_helpers import build_model_target_fixture, write_market_cache

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_latest_emission_uses_preflight_latest_valid_as_of_only(
    tmp_path,
) -> None:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    preflight = system_target.run_smoothed_data_preflight(
        requested_as_of=date(2026, 1, 20),
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    latest = system_target.run_smoothed_latest_emission(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_latest_emission",
        model_target_dir=tmp_path / "model_target",
        emission_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )

    resolution = latest["latest_emission_resolution"]
    links = latest["latest_emission_artifact_links"]

    assert resolution["requested_as_of"] == "2026-01-20"
    assert resolution["resolved_as_of"] == "2026-01-08"
    assert resolution["resolution_reason"] == "latest_valid_as_of_fallback"
    assert resolution["fallback_scope"] == "daily_emission_only"
    assert resolution["due_scan_allowed"] is False
    assert resolution["outcome_update_allowed"] is False
    assert resolution["future_data_used"] is False
    assert links["emitted_event_count"] == 0
    assert links["event_status"] == "NOT_REGISTERED"

    check = system_target.validate_smoothed_latest_emission_artifact(
        latest_emission_id=latest["latest_emission_id"],
        output_dir=tmp_path / "smoothed_latest_emission",
    )
    assert check["status"] == "PASS"
