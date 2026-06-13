from __future__ import annotations

from datetime import UTC, date, datetime

from dynamic_v3_system_target_helpers import build_model_target_fixture, write_market_cache

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_blocked_explain_generates_owner_readable_commands(
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

    explain = system_target.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )

    commands = explain["blocked_command_explanations"]["blocked_commands"]
    command_text = "\n".join(row["command"] for row in commands)

    assert "smoothed-outcome-due scan --as-of 2026-01-20" in command_text
    assert "smoothed-forward-weekly-run run --week-ending 2026-01-20" in command_text
    assert all(row["human_explanation"] for row in commands)
    assert {
        "refresh_sources_then_retry",
        "run_latest_available_emission_or_wait_for_refresh",
    }.issubset({row["safe_next_action"] for row in commands})
    assert "Data freshness gate failed" in explain["blocked_owner_summary"]

    check = system_target.validate_smoothed_blocked_explain_artifact(
        explain_id=explain["explain_id"],
        output_dir=tmp_path / "smoothed_blocked_explain",
    )
    assert check["status"] == "PASS"
