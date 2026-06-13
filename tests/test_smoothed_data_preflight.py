from __future__ import annotations

from datetime import UTC, date, datetime

from dynamic_v3_system_target_helpers import build_model_target_fixture, write_market_cache

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief

REQUESTED_STALE_AS_OF = date(2026, 1, 20)
LATEST_FIXTURE_AS_OF = "2026-01-08"


def test_smoothed_data_preflight_detects_stale_sources_and_latest_valid_as_of(
    tmp_path,
) -> None:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")

    preflight = system_target.run_smoothed_data_preflight(
        requested_as_of=REQUESTED_STALE_AS_OF,
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    snapshot = preflight["data_freshness_snapshot"]
    matrix = preflight["runnable_command_matrix"]["commands"]
    due_row = next(row for row in matrix if row["command"] == "smoothed-outcome-due")
    latest_row = next(
        row
        for row in matrix
        if row["command"] == "smoothed-daily-emission"
        and row["requested_as_of"] == "latest_valid_as_of"
    )

    assert snapshot["freshness_status"] == "BLOCKED_STALE_DATA"
    assert snapshot["latest_valid_as_of"] == LATEST_FIXTURE_AS_OF
    assert {"prices_stale", "rates_stale"}.issubset(snapshot["blocking_errors"])
    assert due_row["status"] == "BLOCKED"
    assert latest_row["status"] == "RUNNABLE_WITH_LATEST_AVAILABLE"
    assert latest_row["resolved_as_of"] == LATEST_FIXTURE_AS_OF
    assert preflight["reader_brief_section"].startswith(
        "## Dynamic Rescue Smoothed Data Preflight"
    )

    check = system_target.validate_smoothed_data_preflight_artifact(
        preflight_id=preflight["preflight_id"],
        output_dir=tmp_path / "smoothed_data_preflight",
    )
    assert check["status"] == "PASS"


def test_reader_brief_surfaces_smoothed_freshness_bootstrap(tmp_path) -> None:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    preflight = system_target.run_smoothed_data_preflight(
        requested_as_of=REQUESTED_STALE_AS_OF,
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
    explain = system_target.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=datetime(2026, 1, 20, 2, tzinfo=UTC),
    )
    refresh = system_target.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
    )
    retry = system_target.run_smoothed_bootstrap_retry(
        requested_as_of=REQUESTED_STALE_AS_OF,
        output_dir=tmp_path / "smoothed_bootstrap_retry",
        preflight_dir=tmp_path / "retry_smoothed_data_preflight",
        model_target_dir=tmp_path / "model_target",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_data_preflight",
                "latest_artifact_path": str(
                    preflight["preflight_dir"] / "smoothed_data_preflight_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_latest_emission",
                "latest_artifact_path": str(
                    latest["latest_emission_dir"]
                    / "smoothed_latest_emission_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_blocked_explain",
                "latest_artifact_path": str(
                    explain["explain_dir"] / "smoothed_blocked_explain_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_refresh_plan",
                "latest_artifact_path": str(
                    refresh["refresh_plan_dir"] / "smoothed_refresh_plan_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_bootstrap_retry",
                "latest_artifact_path": str(
                    retry["retry_dir"] / "smoothed_bootstrap_retry_manifest.json"
                ),
            },
        ]
    }

    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["smoothed_data_preflight_freshness_status"] == "BLOCKED_STALE_DATA"
    assert summary["smoothed_data_preflight_latest_valid_as_of"] == LATEST_FIXTURE_AS_OF
    assert summary["smoothed_latest_emission_resolved_as_of"] == LATEST_FIXTURE_AS_OF
    assert summary["smoothed_latest_emission_outcome_update_allowed"] is False
    assert "smoothed-outcome-due" in summary["smoothed_blocked_explain_commands"]
    assert summary["smoothed_refresh_plan_rerun_allowed_now"] is False
    assert summary["smoothed_bootstrap_retry_status"] == "BLOCKED"
    assert summary["smoothed_bootstrap_retry_can_execute_switch"] is False
    assert summary["production_effect"] == "none"
