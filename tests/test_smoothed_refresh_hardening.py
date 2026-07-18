from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_system_target_helpers import (
    EVALUATION_AS_OF,
    build_model_target_fixture,
    run_smoothed_recorded_owner_authority_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_freshness as freshness,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_refresh as refresh,
)
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)

STALE_AS_OF = date(2026, 1, 20)


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


def _stale_refresh_plan(root: Path) -> dict[str, Any]:
    build_model_target_fixture(root)
    prices_path, rates_path = write_market_cache(root / "market_cache")
    preflight = freshness.run_smoothed_data_preflight(
        requested_as_of=STALE_AS_OF,
        output_dir=root / "smoothed_data_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        model_target_dir=root / "model_target",
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    explain = freshness.run_smoothed_blocked_explain(
        preflight_id=preflight["preflight_id"],
        preflight_dir=root / "smoothed_data_preflight",
        output_dir=root / "smoothed_blocked_explain",
        generated_at=datetime(2026, 1, 20, 1, tzinfo=UTC),
    )
    plan = freshness.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=root / "smoothed_data_preflight",
        explain_dir=root / "smoothed_blocked_explain",
        output_dir=root / "smoothed_refresh_plan",
        generated_at=datetime(2026, 1, 20, 2, tzinfo=UTC),
    )
    return {
        "prices_path": prices_path,
        "rates_path": rates_path,
        "preflight": preflight,
        "plan": plan,
    }


def _blocked_chain(root: Path) -> dict[str, Any]:
    fixture = _stale_refresh_plan(root)
    source_refresh = refresh.run_smoothed_source_refresh(
        refresh_plan_id=fixture["plan"]["refresh_plan_id"],
        refresh_plan_dir=root / "smoothed_refresh_plan",
        output_dir=root / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
    )
    post = refresh.run_smoothed_post_refresh_validation(
        refresh_execution_id=source_refresh["refresh_execution_id"],
        refresh_execution_dir=root / "smoothed_source_refresh",
        output_dir=root / "smoothed_post_refresh_validation",
        preflight_dir=root / "post_refresh_preflight",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 4, tzinfo=UTC),
    )
    resume = refresh.run_smoothed_retry_resume(
        post_refresh_id=post["post_refresh_id"],
        post_refresh_dir=root / "smoothed_post_refresh_validation",
        output_dir=root / "smoothed_retry_resume",
        progress_dir=root / "smoothed_forward_progress",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        generated_at=datetime(2026, 1, 20, 5, tzinfo=UTC),
    )
    growth = refresh.build_smoothed_sample_growth(
        resume_id=resume["resume_id"],
        resume_dir=root / "smoothed_retry_resume",
        output_dir=root / "smoothed_sample_growth",
        generated_at=datetime(2026, 1, 20, 6, tzinfo=UTC),
    )
    readiness = refresh.pack_smoothed_data_readiness(
        refresh_execution_id=source_refresh["refresh_execution_id"],
        post_refresh_id=post["post_refresh_id"],
        resume_id=resume["resume_id"],
        growth_id=growth["growth_id"],
        refresh_execution_dir=root / "smoothed_source_refresh",
        post_refresh_dir=root / "smoothed_post_refresh_validation",
        resume_dir=root / "smoothed_retry_resume",
        growth_dir=root / "smoothed_sample_growth",
        output_dir=root / "smoothed_data_readiness",
        generated_at=datetime(2026, 1, 20, 7, tzinfo=UTC),
    )
    return {
        **fixture,
        "refresh": source_refresh,
        "post": post,
        "resume": resume,
        "growth": growth,
        "readiness": readiness,
    }


def test_blocked_chain_freezes_v2_inputs_and_rebuilds_every_view(tmp_path: Path) -> None:
    chain = _blocked_chain(tmp_path)

    source_input = refresh.smoothed_source_refresh_report_payload(
        refresh_execution_id=chain["refresh"]["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )["input_snapshot"]
    post_input = refresh.smoothed_post_refresh_validation_report_payload(
        post_refresh_id=chain["post"]["post_refresh_id"],
        output_dir=tmp_path / "smoothed_post_refresh_validation",
    )["input_snapshot"]
    resume_input = refresh.smoothed_retry_resume_report_payload(
        resume_id=chain["resume"]["resume_id"],
        output_dir=tmp_path / "smoothed_retry_resume",
    )["input_snapshot"]
    growth_input = refresh.smoothed_sample_growth_report_payload(
        growth_id=chain["growth"]["growth_id"],
        output_dir=tmp_path / "smoothed_sample_growth",
    )["input_snapshot"]
    readiness_input = refresh.smoothed_data_readiness_report_payload(
        readiness_id=chain["readiness"]["readiness_id"],
        output_dir=tmp_path / "smoothed_data_readiness",
    )["input_snapshot"]
    assert source_input["schema_version"] == refresh.SOURCE_REFRESH_SNAPSHOT_SCHEMA
    assert post_input["schema_version"] == refresh.POST_REFRESH_SNAPSHOT_SCHEMA
    assert resume_input["schema_version"] == refresh.RETRY_RESUME_SNAPSHOT_SCHEMA
    assert growth_input["schema_version"] == refresh.SAMPLE_GROWTH_SNAPSHOT_SCHEMA
    assert readiness_input["schema_version"] == refresh.DATA_READINESS_SNAPSHOT_SCHEMA
    assert source_input["before_states"] == source_input["after_states"]
    assert resume_input["bootstrap_retry_source"] is None
    assert not (tmp_path / "smoothed_bootstrap_retry").exists()

    _assert_every_view_is_content_derived(
        chain["refresh"]["refresh_execution_dir"],
        (
            "smoothed_source_refresh_input_snapshot.json",
            "smoothed_source_refresh_manifest.json",
            "refresh_execution_request.json",
            "source_refresh_results.json",
            "source_refresh_audit.json",
            "smoothed_source_refresh_report.md",
            "reader_brief_section.md",
        ),
        refresh.validate_smoothed_source_refresh_artifact,
        refresh_execution_id=chain["refresh"]["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )
    _assert_every_view_is_content_derived(
        chain["post"]["post_refresh_dir"],
        (
            "smoothed_post_refresh_validation_input_snapshot.json",
            "smoothed_post_refresh_manifest.json",
            "post_refresh_data_validation.json",
            "post_refresh_preflight_result.json",
            "post_refresh_decision.json",
            "smoothed_post_refresh_report.md",
            "reader_brief_section.md",
        ),
        refresh.validate_smoothed_post_refresh_artifact,
        post_refresh_id=chain["post"]["post_refresh_id"],
        output_dir=tmp_path / "smoothed_post_refresh_validation",
    )
    _assert_every_view_is_content_derived(
        chain["resume"]["resume_dir"],
        (
            "smoothed_retry_resume_input_snapshot.json",
            "smoothed_retry_resume_manifest.json",
            "resume_precondition_check.json",
            "resume_steps.json",
            "resume_artifacts.json",
            "resume_summary.json",
            "smoothed_retry_resume_report.md",
            "reader_brief_section.md",
        ),
        refresh.validate_smoothed_retry_resume_artifact,
        resume_id=chain["resume"]["resume_id"],
        output_dir=tmp_path / "smoothed_retry_resume",
    )
    _assert_every_view_is_content_derived(
        chain["growth"]["growth_dir"],
        (
            "smoothed_sample_growth_input_snapshot.json",
            "smoothed_sample_growth_manifest.json",
            "sample_growth_summary.json",
            "sample_growth_by_target.json",
            "sample_growth_dashboard_report.md",
            "reader_brief_section.md",
        ),
        refresh.validate_smoothed_sample_growth_artifact,
        growth_id=chain["growth"]["growth_id"],
        output_dir=tmp_path / "smoothed_sample_growth",
    )
    _assert_every_view_is_content_derived(
        chain["readiness"]["readiness_dir"],
        (
            "smoothed_data_readiness_input_snapshot.json",
            "smoothed_data_readiness_manifest.json",
            "owner_data_readiness_summary.json",
            "owner_data_readiness_checklist.md",
            "smoothed_data_readiness_report.md",
            "reader_brief_section.md",
        ),
        refresh.validate_smoothed_data_readiness_artifact,
        readiness_id=chain["readiness"]["readiness_id"],
        output_dir=tmp_path / "smoothed_data_readiness",
    )


def test_source_refresh_validator_does_not_replay_provider_and_detects_live_drift(
    tmp_path: Path,
) -> None:
    fixture = _stale_refresh_plan(tmp_path)
    calls = 0

    def executor(_context: Mapping[str, Any]) -> None:
        nonlocal calls
        calls += 1

    source_refresh = refresh.run_smoothed_source_refresh(
        refresh_plan_id=fixture["plan"]["refresh_plan_id"],
        execute_refresh=True,
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        refresh_executor=executor,
        generated_at=datetime(2026, 1, 20, 3, tzinfo=UTC),
    )
    assert calls == 1
    assert refresh.validate_smoothed_source_refresh_artifact(
        refresh_execution_id=source_refresh["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )["status"] == "PASS"
    assert calls == 1

    original = fixture["prices_path"].read_bytes()
    fixture["prices_path"].write_bytes(original + b"\n")
    assert refresh.validate_smoothed_source_refresh_artifact(
        refresh_execution_id=source_refresh["refresh_execution_id"],
        output_dir=tmp_path / "smoothed_source_refresh",
    )["status"] == "FAIL"
    assert calls == 1


def test_data_readiness_rejects_cross_chain_composition(tmp_path: Path) -> None:
    left = _blocked_chain(tmp_path / "left")
    right = _blocked_chain(tmp_path / "right")

    with pytest.raises(ValueError, match="lineage mismatch"):
        refresh.pack_smoothed_data_readiness(
            refresh_execution_id=left["refresh"]["refresh_execution_id"],
            post_refresh_id=right["post"]["post_refresh_id"],
            resume_id=right["resume"]["resume_id"],
            growth_id=right["growth"]["growth_id"],
            refresh_execution_dir=tmp_path / "left" / "smoothed_source_refresh",
            post_refresh_dir=tmp_path / "right" / "smoothed_post_refresh_validation",
            resume_dir=tmp_path / "right" / "smoothed_retry_resume",
            growth_dir=tmp_path / "right" / "smoothed_sample_growth",
            output_dir=tmp_path / "mismatched_readiness",
            generated_at=datetime(2026, 1, 20, 8, tzinfo=UTC),
        )
    assert not (tmp_path / "mismatched_readiness").exists()


@with_artifact_validation_session
def test_retry_resume_revalidates_exact_bootstrap_child(tmp_path: Path) -> None:
    authority = run_smoothed_recorded_owner_authority_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "refresh_market_cache")
    generated = authority["authority_ready_at"] + timedelta(seconds=1)
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
        generated_at=generated + timedelta(seconds=1),
    )
    plan = freshness.run_smoothed_refresh_plan(
        preflight_id=preflight["preflight_id"],
        explain_id=explain["explain_id"],
        preflight_dir=tmp_path / "smoothed_data_preflight",
        explain_dir=tmp_path / "smoothed_blocked_explain",
        output_dir=tmp_path / "smoothed_refresh_plan",
        generated_at=generated + timedelta(seconds=2),
    )
    source_refresh = refresh.run_smoothed_source_refresh(
        refresh_plan_id=plan["refresh_plan_id"],
        refresh_plan_dir=tmp_path / "smoothed_refresh_plan",
        output_dir=tmp_path / "smoothed_source_refresh",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated + timedelta(seconds=3),
    )
    post = refresh.run_smoothed_post_refresh_validation(
        refresh_execution_id=source_refresh["refresh_execution_id"],
        refresh_execution_dir=tmp_path / "smoothed_source_refresh",
        output_dir=tmp_path / "smoothed_post_refresh_validation",
        preflight_dir=tmp_path / "post_refresh_preflight",
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated + timedelta(seconds=4),
    )
    resume = refresh.run_smoothed_retry_resume(
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
        binding_id=authority["binding"]["binding_id"],
        switch_plan_id=authority["switch_plan"]["switch_plan_id"],
        owner_promotion_id=authority["recorded_owner_promotion"]["decision_id"],
        price_cache_path=prices_path,
        rates_path=rates_path,
        generated_at=generated + timedelta(seconds=5),
    )
    resume_payload = refresh.smoothed_retry_resume_report_payload(
        resume_id=resume["resume_id"], output_dir=tmp_path / "smoothed_retry_resume"
    )
    child = resume_payload["input_snapshot"]["bootstrap_retry_source"]
    assert child["artifact_id"] == resume["manifest"]["bootstrap_retry_id"]
    child_dir = Path(child["bundle"]["source_dir"])
    child_summary = child_dir / "retry_summary.json"
    original = child_summary.read_bytes()
    child_summary.write_bytes(original + b"\n ")
    assert refresh.validate_smoothed_retry_resume_artifact(
        resume_id=resume["resume_id"], output_dir=tmp_path / "smoothed_retry_resume"
    )["status"] == "FAIL"
