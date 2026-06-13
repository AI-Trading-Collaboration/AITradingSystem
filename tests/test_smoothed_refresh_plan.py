from __future__ import annotations

from datetime import UTC, date, datetime

from dynamic_v3_system_target_helpers import build_model_target_fixture, write_market_cache

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_refresh_plan_lists_sources_and_rerun_commands(tmp_path) -> None:
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

    plan = system_target.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=datetime(2026, 1, 20, 2, tzinfo=UTC),
    )

    requirements = plan["source_refresh_requirements"]
    rerun = plan["rerun_command_plan"]
    rows = {row["source"]: row for row in requirements["source_requirements"]}

    assert rows["prices_daily.csv"]["status"] == "STALE"
    assert rows["prices_daily.csv"]["required_through"] == "2026-01-20"
    assert rows["rates_daily.csv"]["status"] == "STALE"
    assert requirements["all_required_sources_fresh"] is False
    assert rerun["rerun_allowed_now"] is False
    assert rerun["external_refresh_executed"] is False
    assert any(
        "smoothed-outcome-due scan --as-of 2026-01-20" in row["command"]
        for row in rerun["rerun_after_refresh"]
    )

    check = system_target.validate_smoothed_refresh_plan_artifact(
        refresh_plan_id=plan["refresh_plan_id"],
        output_dir=tmp_path / "smoothed_refresh_plan",
    )
    assert check["status"] == "PASS"
