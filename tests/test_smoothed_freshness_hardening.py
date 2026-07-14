from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    build_model_target_fixture,
    run_smoothed_forward_ops_chain_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_freshness as freshness,
)


def _assert_every_view_is_content_derived(
    root: Path,
    names: tuple[str, ...],
    validator: Callable[..., dict[str, Any]],
    **kwargs: Any,
) -> None:
    for name in names:
        path = root / name
        original = path.read_bytes()
        path.write_bytes(original + b"\n ")
        assert validator(**kwargs)["status"] == "FAIL", name
        path.write_bytes(original)
    assert validator(**kwargs)["status"] == "PASS"


def _stale_preflight(
    tmp_path: Path, *, generated_hour: int = 0
) -> tuple[dict[str, Any], Path, Path]:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    preflight = freshness.run_smoothed_data_preflight(
        requested_as_of=date(2026, 1, 20),
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 20, generated_hour, tzinfo=UTC),
    )
    return preflight, prices_path, rates_path


def test_preflight_v2_snapshot_replays_live_quality_and_rebuilds_every_view(tmp_path: Path) -> None:
    preflight, prices_path, _ = _stale_preflight(tmp_path)
    root = preflight["preflight_dir"]
    snapshot = preflight["manifest"]

    assert snapshot["schema_version"] == 2
    assert preflight["data_freshness_snapshot"]["freshness_status"] == "BLOCKED_STALE_DATA"
    assert (
        freshness.smoothed_data_preflight_report_payload(
            preflight_id=preflight["preflight_id"],
            output_dir=tmp_path / "smoothed_data_preflight",
        )["input_snapshot"]["schema_version"]
        == freshness.PREFLIGHT_SNAPSHOT_SCHEMA
    )
    _assert_every_view_is_content_derived(
        root,
        (
            "smoothed_data_preflight_input_snapshot.json",
            "smoothed_data_preflight_manifest.json",
            "data_freshness_snapshot.json",
            "runnable_command_matrix.json",
            "blocked_reason_matrix.json",
            "smoothed_data_preflight_report.md",
            "reader_brief_section.md",
        ),
        freshness.validate_smoothed_data_preflight_artifact,
        preflight_id=preflight["preflight_id"],
        output_dir=tmp_path / "smoothed_data_preflight",
    )

    original_prices = prices_path.read_bytes()
    prices_path.write_bytes(original_prices + b"\n")
    assert (
        freshness.validate_smoothed_data_preflight_artifact(
            preflight_id=preflight["preflight_id"],
            output_dir=tmp_path / "smoothed_data_preflight",
        )["status"]
        == "FAIL"
    )


def test_latest_rejects_invalid_preflight_then_rebuilds_every_view(tmp_path: Path) -> None:
    preflight, prices_path, _ = _stale_preflight(tmp_path)
    source_report = preflight["preflight_dir"] / "smoothed_data_preflight_report.md"
    original = source_report.read_bytes()
    source_report.write_bytes(original + b"\nsource tamper")
    with pytest.raises(ValueError, match="validation failed"):
        freshness.run_smoothed_latest_emission(
            preflight_id=preflight["preflight_id"],
            preflight_dir=tmp_path / "smoothed_data_preflight",
            output_dir=tmp_path / "smoothed_latest_emission",
            model_target_dir=tmp_path / "model_target",
            emission_dir=tmp_path / "smoothed_daily_emission",
            price_cache_path=prices_path,
            generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
        )
    assert not (tmp_path / "smoothed_latest_emission").exists()
    source_report.write_bytes(original)

    latest = freshness.run_smoothed_latest_emission(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_latest_emission",
        model_target_dir=tmp_path / "model_target",
        emission_dir=tmp_path / "smoothed_daily_emission",
        price_cache_path=prices_path,
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )
    assert (
        freshness.smoothed_latest_emission_report_payload(
            latest_emission_id=latest["latest_emission_id"],
            output_dir=tmp_path / "smoothed_latest_emission",
        )["input_snapshot"]["schema_version"]
        == freshness.LATEST_SNAPSHOT_SCHEMA
    )
    _assert_every_view_is_content_derived(
        latest["latest_emission_dir"],
        (
            "smoothed_latest_emission_input_snapshot.json",
            "smoothed_latest_emission_manifest.json",
            "latest_emission_resolution.json",
            "latest_emission_artifact_links.json",
            "smoothed_latest_emission_report.md",
            "reader_brief_section.md",
        ),
        freshness.validate_smoothed_latest_emission_artifact,
        latest_emission_id=latest["latest_emission_id"],
        output_dir=tmp_path / "smoothed_latest_emission",
    )


def test_ready_explain_allows_zero_blockers_and_rebuilds_every_view(tmp_path: Path) -> None:
    build_model_target_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    generated = datetime(
        EVALUATION_AS_OF.year,
        EVALUATION_AS_OF.month,
        EVALUATION_AS_OF.day,
        tzinfo=UTC,
    )
    preflight = freshness.run_smoothed_data_preflight(
        requested_as_of=EVALUATION_AS_OF,
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=tmp_path / "model_target",
        generated_at=generated,
    )
    explain = freshness.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=generated + timedelta(hours=1),
    )

    assert explain["blocked_command_explanations"]["blocked_commands"] == []
    assert (
        freshness.smoothed_blocked_explain_report_payload(
            explain_id=explain["explain_id"],
            output_dir=tmp_path / "smoothed_blocked_explain",
        )["input_snapshot"]["schema_version"]
        == freshness.EXPLAIN_SNAPSHOT_SCHEMA
    )
    _assert_every_view_is_content_derived(
        explain["explain_dir"],
        (
            "smoothed_blocked_explain_input_snapshot.json",
            "smoothed_blocked_explain_manifest.json",
            "blocked_command_explanations.json",
            "blocked_owner_summary.md",
            "smoothed_blocked_explain_report.md",
            "reader_brief_section.md",
        ),
        freshness.validate_smoothed_blocked_explain_artifact,
        explain_id=explain["explain_id"],
        output_dir=tmp_path / "smoothed_blocked_explain",
    )


def test_refresh_rejects_cross_preflight_lineage_and_rebuilds_every_view(tmp_path: Path) -> None:
    preflight_a, _, _ = _stale_preflight(tmp_path)
    preflight_b = freshness.run_smoothed_data_preflight(
        requested_as_of=date(2026, 1, 20),
        output_dir=tmp_path / "smoothed_data_preflight",
        price_cache_path=tmp_path / "market_cache" / "prices_daily.csv",
        rates_path=tmp_path / "market_cache" / "rates_daily.csv",
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )
    explain = freshness.run_smoothed_blocked_explain(
        preflight_id=preflight_a["preflight_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        output_dir=tmp_path / "smoothed_blocked_explain",
        generated_at=datetime(2026, 1, 20, 2, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="lineage mismatch"):
        freshness.run_smoothed_refresh_plan(
            preflight_id=preflight_b["preflight_id"],
            explain_id=explain["explain_id"],
            preflight_dir=tmp_path / "smoothed_data_preflight",
            explain_dir=tmp_path / "smoothed_blocked_explain",
            output_dir=tmp_path / "smoothed_refresh_plan",
            generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
        )

    refresh = freshness.run_smoothed_refresh_plan(
        preflight_id=preflight_a["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
    )
    assert (
        freshness.smoothed_refresh_plan_report_payload(
            refresh_plan_id=refresh["refresh_plan_id"],
            output_dir=tmp_path / "smoothed_refresh_plan",
        )["input_snapshot"]["schema_version"]
        == freshness.REFRESH_SNAPSHOT_SCHEMA
    )
    _assert_every_view_is_content_derived(
        refresh["refresh_plan_dir"],
        (
            "smoothed_refresh_plan_input_snapshot.json",
            "smoothed_refresh_plan_manifest.json",
            "source_refresh_requirements.json",
            "rerun_command_plan.json",
            "smoothed_refresh_plan_report.md",
            "reader_brief_section.md",
        ),
        freshness.validate_smoothed_refresh_plan_artifact,
        refresh_plan_id=refresh["refresh_plan_id"],
        output_dir=tmp_path / "smoothed_refresh_plan",
    )


def test_retry_rebuilds_views_and_rejects_tampered_validated_weekly_child(tmp_path: Path) -> None:
    build_model_target_fixture(tmp_path)
    ops = run_smoothed_forward_ops_chain_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "retry_market_cache")
    generated = max(
        datetime.fromisoformat(ops[key]["manifest"]["generated_at"])
        for key in ("binding", "switch_plan", "recorded_owner_promotion")
    ) + timedelta(seconds=1)
    retry = freshness.run_smoothed_bootstrap_retry(
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
        generated_at=generated,
    )
    payload = freshness.smoothed_bootstrap_retry_report_payload(
        retry_id=retry["retry_id"], output_dir=tmp_path / "smoothed_bootstrap_retry"
    )
    assert payload["input_snapshot"]["schema_version"] == freshness.RETRY_SNAPSHOT_SCHEMA
    assert payload["input_snapshot"]["child_type"] == "weekly"
    _assert_every_view_is_content_derived(
        retry["retry_dir"],
        (
            "smoothed_bootstrap_retry_input_snapshot.json",
            "smoothed_bootstrap_retry_manifest.json",
            "retry_preflight_result.json",
            "retry_steps.json",
            "retry_artifacts.json",
            "retry_summary.json",
            "smoothed_bootstrap_retry_report.md",
            "reader_brief_section.md",
        ),
        freshness.validate_smoothed_bootstrap_retry_artifact,
        retry_id=retry["retry_id"],
        output_dir=tmp_path / "smoothed_bootstrap_retry",
    )

    child_dir = Path(payload["input_snapshot"]["child_source"]["bundle"]["source_dir"])
    child_summary = child_dir / "weekly_run_summary.json"
    original = child_summary.read_bytes()
    child_summary.write_bytes(original + b"\n ")
    assert (
        freshness.validate_smoothed_bootstrap_retry_artifact(
            retry_id=retry["retry_id"], output_dir=tmp_path / "smoothed_bootstrap_retry"
        )["status"]
        == "FAIL"
    )
