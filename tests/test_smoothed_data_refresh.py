from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    build_model_target_fixture,
    run_smoothed_forward_ops_chain_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief

REQUESTED_STALE_AS_OF = date(2026, 1, 20)


def test_smoothed_source_refresh_plan_is_dry_run_and_validates(tmp_path) -> None:
    fixture = _stale_refresh_plan_fixture(tmp_path)

    refresh = system_target.run_smoothed_source_refresh(
        refresh_plan_id=fixture["refresh_plan"]["refresh_plan_id"],
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )

    results = refresh["source_refresh_results"]
    assert results["refresh_status"] == "DRY_RUN_ONLY"
    assert results["external_refresh_executed"] is False
    assert {row["status"] for row in results["sources"]} == {"DRY_RUN_ONLY"}
    assert refresh["refresh_execution_request"]["dry_run"] is True
    assert refresh["manifest"]["broker_action_allowed"] is False
    assert refresh["manifest"]["production_effect"] == "none"

    check = system_target.validate_smoothed_source_refresh_artifact(
        refresh_execution_id=refresh["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )
    assert check["status"] == "PASS"


def test_smoothed_source_refresh_execute_then_post_refresh_ready(tmp_path) -> None:
    fixture = _stale_refresh_plan_fixture(tmp_path)

    refresh = system_target.run_smoothed_source_refresh(
        refresh_plan_id=fixture["refresh_plan"]["refresh_plan_id"],
        execute_refresh=True,
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        refresh_executor=_append_requested_cache_rows,
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )

    results = refresh["source_refresh_results"]
    required_sources = [row for row in results["sources"] if row["required"]]
    assert results["refresh_status"] == "COMPLETED"
    assert all(row["freshness_after_refresh"] == "READY" for row in required_sources)
    assert all(row["after_latest_date"] == "2026-01-20" for row in required_sources)
    assert results["all_sources_refreshed"] is True

    refresh_check = system_target.validate_smoothed_source_refresh_artifact(
        refresh_execution_id=refresh["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )
    assert refresh_check["status"] == "PASS"

    post = system_target.run_smoothed_post_refresh_validation(
        refresh_execution_id=refresh["refresh_execution_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        output_dir=tmp_path / "smoothed_post_refresh_validation",
        preflight_dir=tmp_path / "post_refresh_preflight",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 5, tzinfo=UTC),
    )

    assert post["post_refresh_data_validation"]["validate_data_status"] in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }
    assert post["post_refresh_preflight_result"]["can_run_full_retry"] is True
    assert post["post_refresh_decision"]["retry_decision"] == "RETRY_READY"

    post_check = system_target.validate_smoothed_post_refresh_artifact(
        post_refresh_id=post["post_refresh_id"],
        output_dir=tmp_path / "smoothed_post_refresh_validation",
    )
    assert post_check["status"] == "PASS"


def test_smoothed_retry_resume_blocks_and_readiness_requires_refresh(tmp_path) -> None:
    fixture = _stale_refresh_plan_fixture(tmp_path)
    refresh = system_target.run_smoothed_source_refresh(
        refresh_plan_id=fixture["refresh_plan"]["refresh_plan_id"],
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )
    post = system_target.run_smoothed_post_refresh_validation(
        refresh_execution_id=refresh["refresh_execution_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        output_dir=tmp_path / "smoothed_post_refresh_validation",
        preflight_dir=tmp_path / "post_refresh_preflight",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 5, tzinfo=UTC),
    )

    resume = system_target.run_smoothed_retry_resume(
        post_refresh_id=post["post_refresh_id"],
        post_refresh_dir=tmp_path / "smoothed_post_refresh_validation",
        output_dir=tmp_path / "smoothed_retry_resume",
        progress_dir=tmp_path / "smoothed_forward_progress",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 6, tzinfo=UTC),
    )
    growth = system_target.build_smoothed_sample_growth(
        resume_id=resume["resume_id"],
        resume_dir=tmp_path / "smoothed_retry_resume",
        output_dir=tmp_path / "smoothed_sample_growth",
        generated_at=datetime(2026, 1, 20, 7, tzinfo=UTC),
    )
    readiness = system_target.pack_smoothed_data_readiness(
        refresh_execution_id=refresh["refresh_execution_id"],
        post_refresh_id=post["post_refresh_id"],
        resume_id=resume["resume_id"],
        growth_id=growth["growth_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        post_refresh_dir=tmp_path / "smoothed_post_refresh_validation",
        resume_dir=tmp_path / "smoothed_retry_resume",
        growth_dir=tmp_path / "smoothed_sample_growth",
        output_dir=tmp_path / "smoothed_data_readiness",
        generated_at=datetime(2026, 1, 20, 8, tzinfo=UTC),
    )

    assert post["post_refresh_decision"]["retry_decision"] == "STILL_BLOCKED"
    assert resume["resume_precondition_check"]["can_resume"] is False
    assert resume["resume_summary"]["resume_status"] == "BLOCKED"
    assert growth["sample_growth_summary"]["growth_status"] == "INSUFFICIENT_DATA"
    assert readiness["owner_data_readiness_summary"]["current_status"] == "REFRESH_REQUIRED"
    assert readiness["owner_data_readiness_summary"]["recommended_owner_action"] == "run_refresh"
    assert "no broker" in readiness["owner_data_readiness_checklist"].lower()

    assert system_target.validate_smoothed_retry_resume_artifact(
        resume_id=resume["resume_id"],
        output_dir=tmp_path / "smoothed_retry_resume",
    )["status"] == "PASS"
    assert system_target.validate_smoothed_sample_growth_artifact(
        growth_id=growth["growth_id"],
        output_dir=tmp_path / "smoothed_sample_growth",
    )["status"] == "PASS"
    assert system_target.validate_smoothed_data_readiness_artifact(
        readiness_id=readiness["readiness_id"],
        output_dir=tmp_path / "smoothed_data_readiness",
    )["status"] == "PASS"


def test_smoothed_retry_resume_completes_after_ready_post_refresh(tmp_path) -> None:
    ops = run_smoothed_forward_ops_chain_fixture(tmp_path)
    # The ops-chain artifacts bind tmp_path/market_cache as immutable evidence.
    # Refresh inputs must not overwrite that lineage before retry revalidation.
    prices_path, rates_path = write_market_cache(tmp_path / "refresh_market_cache")
    generated_at = max(
        datetime.fromisoformat(ops[key]["manifest"]["generated_at"])
        for key in ("binding", "switch_plan", "recorded_owner_promotion")
    ) + timedelta(seconds=1)
    preflight = system_target.run_smoothed_data_preflight(
        requested_as_of=EVALUATION_AS_OF,
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=tmp_path / "model_target",
        generated_at=generated_at,
    )
    explain = system_target.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=generated_at + timedelta(seconds=1),
    )
    refresh_plan = system_target.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=generated_at + timedelta(seconds=2),
    )
    refresh = system_target.run_smoothed_source_refresh(
        refresh_plan_id=refresh_plan["refresh_plan_id"],
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated_at + timedelta(seconds=3),
    )
    post = system_target.run_smoothed_post_refresh_validation(
        refresh_execution_id=refresh["refresh_execution_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        output_dir=tmp_path / "smoothed_post_refresh_validation",
        preflight_dir=tmp_path / "post_refresh_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated_at + timedelta(seconds=4),
    )

    resume = system_target.run_smoothed_retry_resume(
        post_refresh_id=post["post_refresh_id"],
        post_refresh_dir=tmp_path / "smoothed_post_refresh_validation",
        output_dir=tmp_path / "smoothed_retry_resume",
        bootstrap_retry_dir=tmp_path / "smoothed_bootstrap_retry",
        preflight_dir=tmp_path / "retry_preflight",
        latest_emission_dir=tmp_path / "smoothed_latest_emission",
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
        binding_id=ops["binding"]["binding_id"],
        switch_plan_id=ops["switch_plan"]["switch_plan_id"],
        owner_promotion_id=ops["recorded_owner_promotion"]["decision_id"],
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated_at + timedelta(seconds=5),
    )
    growth = system_target.build_smoothed_sample_growth(
        resume_id=resume["resume_id"],
        resume_dir=tmp_path / "smoothed_retry_resume",
        output_dir=tmp_path / "smoothed_sample_growth",
        generated_at=generated_at + timedelta(seconds=6),
    )

    assert post["post_refresh_decision"]["retry_decision"] == "RETRY_READY"
    assert resume["resume_precondition_check"]["can_resume"] is True
    assert resume["resume_summary"]["resume_status"] == "COMPLETED"
    assert resume["resume_summary"]["updated_windows"] >= 0
    assert resume["resume_summary"]["can_execute_switch"] is False
    assert growth["sample_growth_summary"]["delta"]["forward_events"] >= 0


def test_reader_brief_surfaces_smoothed_data_readiness(tmp_path) -> None:
    fixture = _stale_refresh_plan_fixture(tmp_path)
    refresh = system_target.run_smoothed_source_refresh(
        refresh_plan_id=fixture["refresh_plan"]["refresh_plan_id"],
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )
    post = system_target.run_smoothed_post_refresh_validation(
        refresh_execution_id=refresh["refresh_execution_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        output_dir=tmp_path / "smoothed_post_refresh_validation",
        preflight_dir=tmp_path / "post_refresh_preflight",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 5, tzinfo=UTC),
    )
    resume = system_target.run_smoothed_retry_resume(
        post_refresh_id=post["post_refresh_id"],
        post_refresh_dir=tmp_path / "smoothed_post_refresh_validation",
        output_dir=tmp_path / "smoothed_retry_resume",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 6, tzinfo=UTC),
    )
    growth = system_target.build_smoothed_sample_growth(
        resume_id=resume["resume_id"],
        resume_dir=tmp_path / "smoothed_retry_resume",
        output_dir=tmp_path / "smoothed_sample_growth",
        generated_at=datetime(2026, 1, 20, 7, tzinfo=UTC),
    )
    readiness = system_target.pack_smoothed_data_readiness(
        refresh_execution_id=refresh["refresh_execution_id"],
        post_refresh_id=post["post_refresh_id"],
        resume_id=resume["resume_id"],
        growth_id=growth["growth_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        post_refresh_dir=tmp_path / "smoothed_post_refresh_validation",
        resume_dir=tmp_path / "smoothed_retry_resume",
        growth_dir=tmp_path / "smoothed_sample_growth",
        output_dir=tmp_path / "smoothed_data_readiness",
        generated_at=datetime(2026, 1, 20, 8, tzinfo=UTC),
    )

    summary = reader_brief._etf_dynamic_v3_system_target_summary(
        _readiness_report_index(refresh, post, resume, growth, readiness)
    )

    assert summary["smoothed_source_refresh_status"] == "DRY_RUN_ONLY"
    assert summary["smoothed_post_refresh_retry_decision"] == "STILL_BLOCKED"
    assert summary["smoothed_retry_resume_status"] == "BLOCKED"
    assert summary["smoothed_sample_growth_status"] == "INSUFFICIENT_DATA"
    assert summary["smoothed_data_readiness_current_status"] == "REFRESH_REQUIRED"
    assert summary["smoothed_data_readiness_recommended_owner_action"] == "run_refresh"
    assert "prices_daily" in summary["smoothed_data_readiness_source_statuses"]
    assert summary["production_effect"] == "none"


def _stale_refresh_plan_fixture(tmp_path: Path) -> dict[str, Any]:
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
    explain = system_target.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )
    refresh_plan = system_target.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=datetime(2026, 1, 20, 2, tzinfo=UTC),
    )
    return {
        "prices_path": prices_path,
        "rates_path": rates_path,
        "preflight": preflight,
        "explain": explain,
        "refresh_plan": refresh_plan,
    }


def _append_requested_cache_rows(context: Any) -> None:
    price_cache_path = Path(context["price_cache_path"])
    rates_path = Path(context["rates_path"])
    price_lines = price_cache_path.read_text(encoding="utf-8").splitlines()
    rate_lines = rates_path.read_text(encoding="utf-8").splitlines()
    symbols = ("QQQ", "SMH", "SOXX", "TLT")
    requested_days = (
        "2026-01-09",
        "2026-01-12",
        "2026-01-13",
        "2026-01-14",
        "2026-01-15",
        "2026-01-16",
        "2026-01-19",
        "2026-01-20",
    )
    for day_index, day in enumerate(requested_days, start=1):
        for symbol_index, symbol in enumerate(symbols):
            close = 110.0 + symbol_index * 3.0 + day_index
            price_lines.append(
                f"{day},{symbol},{close:.4f},{close * 1.01:.4f},"
                f"{close * 0.99:.4f},{close:.4f},{close:.4f},1000000"
            )
        rate_lines.append(f"{day},FEDFUNDS,4.0")
    price_cache_path.write_text("\n".join(price_lines) + "\n", encoding="utf-8")
    rates_path.write_text("\n".join(rate_lines) + "\n", encoding="utf-8")


def _readiness_report_index(
    refresh: dict[str, Any],
    post: dict[str, Any],
    resume: dict[str, Any],
    growth: dict[str, Any],
    readiness: dict[str, Any],
) -> dict[str, Any]:
    return {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_source_refresh",
                "latest_artifact_path": str(
                    refresh["refresh_execution_dir"]
                    / "smoothed_source_refresh_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_post_refresh_validation",
                "latest_artifact_path": str(
                    post["post_refresh_dir"] / "smoothed_post_refresh_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_retry_resume",
                "latest_artifact_path": str(
                    resume["resume_dir"] / "smoothed_retry_resume_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_sample_growth",
                "latest_artifact_path": str(
                    growth["growth_dir"] / "smoothed_sample_growth_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_data_readiness",
                "latest_artifact_path": str(
                    readiness["readiness_dir"] / "smoothed_data_readiness_manifest.json"
                ),
            },
        ]
    }
