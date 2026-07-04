from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_manual_run_dry_run_fixtures import (
    build_high_intensity_scheduler_manual_run_dry_run_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    NEXT_2351_TASK,
    STATUS,
    HighIntensitySchedulerManualRunDryRunError,
    build_high_intensity_2351_task_route,
    build_manual_run_dry_run_evidence,
    build_manual_run_dry_run_package,
    build_manual_run_preview,
    build_manual_run_side_effect_assertions,
    build_manual_run_source_artifact_review,
    load_high_intensity_scheduler_manual_run_dry_run_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_manual_run_dry_run_fixture(tmp_path)
    return load_high_intensity_scheduler_manual_run_dry_run_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
    )


def test_manual_run_dry_run_loader_reads_2347_2348_and_2349_artifacts(
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
    assert inputs["manual_review_gate"]["summary"]["readiness"] == (
        "READY_FOR_2350_WITH_CAVEATS"
    )


def test_manual_run_dry_run_loader_fails_closed_on_bad_2349_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_run_dry_run_fixture(tmp_path)
    route_path = (
        fixture["manual_review_gate_dir"]
        / "high_intensity_2350_manual_run_interface_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2350_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerManualRunDryRunError):
        load_high_intensity_scheduler_manual_run_dry_run_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
        )


def test_manual_run_dry_run_loader_fails_closed_on_2349_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_run_dry_run_fixture(tmp_path)
    decision_path = (
        fixture["manual_review_gate_dir"]
        / "high_intensity_scheduler_manual_review_gate_promotion_decision.json"
    )
    decision = read_json(decision_path)
    decision["promotion_allowed"] = True
    write_json(decision_path, decision)

    with pytest.raises(HighIntensitySchedulerManualRunDryRunError):
        load_high_intensity_scheduler_manual_run_dry_run_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
        )


def test_manual_run_dry_run_package_blocks_promotion_and_routes_to_2351(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    generated_at = datetime.now(tz=UTC)
    source_review = build_manual_run_source_artifact_review(inputs=inputs)
    preview = build_manual_run_preview(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
    )
    side_effects = build_manual_run_side_effect_assertions(
        generated_at=generated_at,
        preview=preview,
    )
    evidence = build_manual_run_dry_run_evidence(
        generated_at=generated_at,
        preview=preview,
        side_effect_assertions=side_effects,
        source_review=source_review,
    )
    package = build_manual_run_dry_run_package(
        generated_at=generated_at,
        source_review=source_review,
        preview=preview,
        evidence=evidence,
        side_effect_assertions=side_effects,
    )
    route = build_high_intensity_2351_task_route(
        package=package,
        evidence=evidence,
        side_effect_assertions=side_effects,
    )

    assert package["status"] == STATUS
    assert package["source_tasks"] == ["TRADING-2347", "TRADING-2348", "TRADING-2349"]
    assert package["manual_run_interface_present"] is True
    assert package["manual_run_preview_generated"] is True
    assert package["manual_run_executed"] is False
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["manual_review_required"] is True
    assert package["promotion_allowed"] is False
    assert package["event_append_attempted"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_attempted"] is False
    assert package["broker_action_attempted"] is False
    assert side_effects["side_effect_assertions_passed"] is True
    assert side_effects["side_effect_assertions"]["cron_created"] is False
    assert side_effects["side_effect_assertions"]["windows_task_created"] is False
    assert (
        side_effects["side_effect_assertions"]["github_actions_schedule_created"]
        is False
    )
    assert evidence["promotion_decision"] == "BLOCKED"
    assert route["readiness"] == "READY_FOR_2351_WITH_CAVEATS"
    assert route["next_route"] == NEXT_2351_TASK


def test_manual_run_dry_run_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-manual-run-dry-run"
        in result.output
    )


def test_manual_run_dry_run_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_manual_run_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-manual-run-dry-run",
            "--disabled-wiring-dir",
            str(fixture["disabled_wiring_dir"]),
            "--smoke-dry-run-dir",
            str(fixture["smoke_dry_run_dir"]),
            "--manual-review-gate-dir",
            str(fixture["manual_review_gate_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_manual_run_dry_run",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_manual_run_dry_run_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.json",
        "high_intensity_scheduler_manual_run_dry_run_source_artifact_review.json",
        "high_intensity_scheduler_manual_run_dry_run_preview.json",
        "high_intensity_scheduler_manual_run_dry_run_evidence.json",
        "high_intensity_scheduler_manual_run_dry_run_side_effect_assertions.json",
        "high_intensity_2351_manual_run_replay_route.json",
        "high_intensity_scheduler_manual_run_dry_run_interpretation_boundary.json",
        "high_intensity_scheduler_manual_run_dry_run_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_scheduler_manual_run_dry_run_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.json"
    )
    evidence = read_json(
        output_dir / "high_intensity_scheduler_manual_run_dry_run_evidence.json"
    )
    side_effects = read_json(
        output_dir
        / "high_intensity_scheduler_manual_run_dry_run_side_effect_assertions.json"
    )
    route = read_json(output_dir / "high_intensity_2351_manual_run_replay_route.json")
    assert summary["status"] == STATUS
    assert summary["manual_run_interface_present"] is True
    assert summary["manual_run_preview_generated"] is True
    assert summary["manual_run_executed"] is False
    assert package["promotion_decision"] == "BLOCKED"
    assert evidence["event_append_attempted"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert side_effects["side_effect_assertions"]["broker_action_attempted"] is False
    assert route["next_route"] == NEXT_2351_TASK
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.md"
    ).exists()
    assert (docs_root / "high_intensity_2351_manual_run_replay_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_manual_run_dry_run() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run" in registry
    assert (
        "TRADING-2350 High-Intensity Risk-Cap Observe-Only Scheduler Manual-Run "
        "Interface Dry-Run"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-manual-run-dry-run"
        in system_flow
    )
    assert (
        "TRADING-2350_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN"
        in task_register
    )
