from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_manual_run_replay_validation_fixtures import (
    build_high_intensity_scheduler_manual_run_replay_validation_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    NEXT_2352_ROUTE,
    REPLAY_COUNT,
    STATUS,
    HighIntensitySchedulerManualRunReplayValidationError,
    build_high_intensity_2352_task_route,
    build_replay_semantic_checks,
    build_replay_side_effect_assertions,
    build_replay_source_artifact_review,
    build_replay_validation_evidence,
    build_replay_validation_package,
    load_high_intensity_scheduler_manual_run_replay_validation_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_manual_run_replay_validation_fixture(
        tmp_path
    )
    return load_high_intensity_scheduler_manual_run_replay_validation_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
    )


def test_manual_run_replay_loader_reads_2347_2348_2349_and_2350_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)

    assert inputs["disabled_wiring"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert inputs["smoke_dry_run"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_SMOKE_DRY_RUN_PASSED_WITH_"
        "CAVEATS_PROMOTION_BLOCKED"
    )
    assert inputs["manual_review_gate"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_MANUAL_REVIEW_GATE_READY_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert inputs["manual_run_dry_run"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN_READY_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert inputs["manual_run_dry_run"]["summary"]["readiness"] == (
        "READY_FOR_2351_WITH_CAVEATS"
    )


def test_manual_run_replay_loader_fails_closed_on_bad_2350_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_run_replay_validation_fixture(
        tmp_path
    )
    route_path = (
        fixture["manual_run_dry_run_dir"]
        / "high_intensity_2351_manual_run_replay_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2351_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerManualRunReplayValidationError):
        load_high_intensity_scheduler_manual_run_replay_validation_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        )


def test_manual_run_replay_loader_fails_closed_on_2350_manual_run_executed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_run_replay_validation_fixture(
        tmp_path
    )
    preview_path = (
        fixture["manual_run_dry_run_dir"]
        / "high_intensity_scheduler_manual_run_dry_run_preview.json"
    )
    preview = read_json(preview_path)
    preview["manual_run_executed"] = True
    write_json(preview_path, preview)

    with pytest.raises(HighIntensitySchedulerManualRunReplayValidationError):
        load_high_intensity_scheduler_manual_run_replay_validation_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        )


def test_manual_run_replay_package_blocks_promotion_and_routes_to_2352(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    generated_at = datetime.now(tz=UTC)
    source_review = build_replay_source_artifact_review(inputs=inputs)
    semantic_checks = build_replay_semantic_checks(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        replay_count=REPLAY_COUNT,
    )
    side_effects = build_replay_side_effect_assertions(
        generated_at=generated_at,
        inputs=inputs,
        semantic_checks=semantic_checks,
    )
    evidence = build_replay_validation_evidence(
        generated_at=generated_at,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effects,
    )
    package = build_replay_validation_package(
        generated_at=generated_at,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effects,
        evidence=evidence,
    )
    route = build_high_intensity_2352_task_route(
        package=package,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effects,
    )

    assert package["status"] == STATUS
    assert package["source_tasks"] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
    ]
    assert package["replay_count"] == 3
    assert package["stable_semantic_replay_passed"] is True
    assert package["side_effect_assertions_passed"] is True
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["manual_run_executed"] is False
    assert package["promotion_allowed"] is False
    assert side_effects["side_effect_assertions"]["real_scheduler_created"] is False
    assert side_effects["side_effect_assertions"]["cron_created"] is False
    assert side_effects["side_effect_assertions"]["windows_task_created"] is False
    assert (
        side_effects["side_effect_assertions"]["github_actions_schedule_created"]
        is False
    )
    assert side_effects["side_effect_assertions"]["event_append_attempted"] is False
    assert (
        side_effects["side_effect_assertions"]["outcome_binding_attempted"]
        is False
    )
    assert side_effects["side_effect_assertions"]["paper_shadow_attempted"] is False
    assert side_effects["side_effect_assertions"]["production_attempted"] is False
    assert side_effects["side_effect_assertions"]["broker_action_attempted"] is False
    assert route["readiness"] == "READY_FOR_2352_WITH_CAVEATS"
    assert route["next_route"] == NEXT_2352_ROUTE


def test_manual_run_replay_validation_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-manual-run-replay-validation"
        in command_names
    )


def test_manual_run_replay_validation_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_manual_run_replay_validation_fixture(
        tmp_path
    )
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-manual-run-replay-validation",
            "--disabled-wiring-dir",
            str(fixture["disabled_wiring_dir"]),
            "--smoke-dry-run-dir",
            str(fixture["smoke_dry_run_dir"]),
            "--manual-review-gate-dir",
            str(fixture["manual_review_gate_dir"]),
            "--manual-run-dry-run-dir",
            str(fixture["manual_run_dry_run_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--replay-count",
            "3",
            "--mode",
            "observe_only_scheduler_manual_run_replay_validation",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_manual_run_replay_validation_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.json",
        "high_intensity_scheduler_manual_run_replay_source_artifact_review.json",
        "high_intensity_scheduler_manual_run_replay_semantic_checks.json",
        "high_intensity_scheduler_manual_run_replay_side_effect_assertions.json",
        "high_intensity_scheduler_manual_run_replay_evidence.json",
        "high_intensity_2352_scheduler_audit_package_route.json",
        "high_intensity_scheduler_manual_run_replay_interpretation_boundary.json",
        "high_intensity_scheduler_manual_run_replay_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir
        / "high_intensity_scheduler_manual_run_replay_validation_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.json"
    )
    semantic_checks = read_json(
        output_dir / "high_intensity_scheduler_manual_run_replay_semantic_checks.json"
    )
    side_effects = read_json(
        output_dir
        / "high_intensity_scheduler_manual_run_replay_side_effect_assertions.json"
    )
    route = read_json(
        output_dir / "high_intensity_2352_scheduler_audit_package_route.json"
    )
    assert summary["status"] == STATUS
    assert summary["replay_count"] == 3
    assert semantic_checks["stable_semantic_replay_passed"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert side_effects["side_effect_assertions"]["broker_action_attempted"] is False
    assert route["next_route"] == NEXT_2352_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.md"
    ).exists()
    assert (docs_root / "high_intensity_2352_scheduler_audit_package_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_replay_validation() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation"
        in registry
    )
    assert (
        "TRADING-2351 High-Intensity Risk-Cap Observe-Only Scheduler Manual-Run "
        "Replay No-Side-Effect Validation"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-manual-run-replay-validation"
        in system_flow
    )
    assert (
        "TRADING-2351_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION"
        in task_register
    )
