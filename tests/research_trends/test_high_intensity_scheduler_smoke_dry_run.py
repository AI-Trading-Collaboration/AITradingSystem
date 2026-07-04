from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_smoke_dry_run_fixtures import (
    build_high_intensity_scheduler_smoke_dry_run_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    NEXT_2349_TASK,
    STATUS,
    HighIntensitySchedulerSmokeDryRunError,
    build_high_intensity_2349_task_route,
    build_smoke_dry_run_evidence,
    build_smoke_dry_run_guardrail_assertions,
    build_smoke_dry_run_side_effect_assertions,
    build_source_artifact_assertion,
    load_high_intensity_scheduler_smoke_dry_run_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_smoke_dry_run_fixture(tmp_path)
    return load_high_intensity_scheduler_smoke_dry_run_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
    )


def test_smoke_dry_run_loader_reads_2347_artifacts(tmp_path: Path) -> None:
    inputs = _load_fixture_inputs(tmp_path)

    assert inputs["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert inputs["guardrails"]["guardrail_status"] == "PASS"
    assert inputs["no_real_scheduler"]["assertion_status"] == "PASS"
    assert inputs["readiness"]["readiness_status"] == "READY_FOR_2348_WITH_CAVEATS"


def test_smoke_dry_run_loader_fails_closed_on_bad_2347_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_smoke_dry_run_fixture(tmp_path)
    route_path = fixture["disabled_wiring_dir"] / "high_intensity_2348_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2348_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerSmokeDryRunError):
        load_high_intensity_scheduler_smoke_dry_run_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
        )


def test_smoke_dry_run_loader_fails_closed_on_scheduler_enabled(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_smoke_dry_run_fixture(tmp_path)
    manifest_path = (
        fixture["disabled_wiring_dir"]
        / "high_intensity_scheduler_disabled_wiring_implementation_manifest.json"
    )
    manifest = read_json(manifest_path)
    manifest["scheduler_enabled"] = True
    write_json(manifest_path, manifest)

    with pytest.raises(HighIntensitySchedulerSmokeDryRunError):
        load_high_intensity_scheduler_smoke_dry_run_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
        )


def test_smoke_dry_run_guardrails_and_side_effects_pass(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_assertion = build_source_artifact_assertion(inputs=inputs)
    guardrails = build_smoke_dry_run_guardrail_assertions(
        inputs=inputs,
        source_assertion=source_assertion,
    )
    side_effects = build_smoke_dry_run_side_effect_assertions(
        inputs=inputs,
        guardrails=guardrails,
    )

    assert source_assertion["source_artifacts_read"] is True
    assert guardrails["guardrail_assertions_passed"] is True
    assert guardrails["guardrail_assertions"]["scheduler_enabled"] is False
    assert guardrails["guardrail_assertions"]["manual_run_only"] is True
    assert guardrails["guardrail_assertions"]["dry_run_only"] is True
    assert guardrails["guardrail_assertions"]["promotion_allowed"] is False
    assert side_effects["side_effect_assertions_passed"] is True
    assert side_effects["side_effect_assertions"]["real_scheduler_created"] is False
    assert side_effects["side_effect_assertions"]["cron_created"] is False
    assert side_effects["side_effect_assertions"]["windows_task_created"] is False
    assert (
        side_effects["side_effect_assertions"]["github_actions_schedule_created"]
        is False
    )
    assert side_effects["side_effect_assertions"]["event_append_attempted"] is False
    assert side_effects["side_effect_assertions"]["outcome_binding_attempted"] is False
    assert side_effects["side_effect_assertions"]["paper_shadow_attempted"] is False
    assert side_effects["side_effect_assertions"]["production_attempted"] is False
    assert side_effects["side_effect_assertions"]["broker_action_attempted"] is False


def test_smoke_dry_run_evidence_routes_to_2349(tmp_path: Path) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_assertion = build_source_artifact_assertion(inputs=inputs)
    guardrails = build_smoke_dry_run_guardrail_assertions(
        inputs=inputs,
        source_assertion=source_assertion,
    )
    side_effects = build_smoke_dry_run_side_effect_assertions(
        inputs=inputs,
        guardrails=guardrails,
    )
    evidence = build_smoke_dry_run_evidence(
        generated_at=datetime.now(tz=UTC),
        inputs=inputs,
        source_assertion=source_assertion,
        guardrails=guardrails,
        side_effects=side_effects,
    )
    route = build_high_intensity_2349_task_route(evidence=evidence)

    assert evidence["status"] == STATUS
    assert evidence["task_id"] == "TRADING-2348"
    assert evidence["readiness"] == "READY_FOR_2349_WITH_CAVEATS"
    assert route["next_route"] == NEXT_2349_TASK
    assert "MANUAL_REVIEW_REQUIRED_BEFORE_PROMOTION_GATE" in route["route_caveats"]


def test_smoke_dry_run_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-smoke-dry-run"
        in result.output
    )


def test_smoke_dry_run_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_smoke_dry_run_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-smoke-dry-run",
            "--disabled-wiring-dir",
            str(fixture["disabled_wiring_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_smoke_dry_run",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_smoke_dry_run_summary.json",
        "high_intensity_scheduler_smoke_dry_run_evidence.json",
        "high_intensity_scheduler_smoke_dry_run_source_artifact_assertion.json",
        "high_intensity_scheduler_smoke_dry_run_guardrail_assertions.json",
        "high_intensity_scheduler_smoke_dry_run_side_effect_assertions.json",
        "high_intensity_2349_manual_review_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_smoke_dry_run_summary.json")
    evidence = read_json(output_dir / "high_intensity_scheduler_smoke_dry_run_evidence.json")
    assert summary["status"] == STATUS
    assert evidence["guardrail_assertions_passed"] is True
    assert evidence["scheduler_enabled"] is False
    assert evidence["manual_run_only"] is True
    assert evidence["dry_run_only"] is True
    assert evidence["promotion_allowed"] is False
    assert evidence["next_route"] == NEXT_2349_TASK
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run_evidence.md"
    ).exists()


def test_registry_catalog_and_task_register_reference_smoke_dry_run() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run" in registry
    assert (
        "TRADING-2348 High-Intensity Risk-Cap Observe-Only Scheduler Smoke Dry-Run"
        in catalog
    )
    assert (
        "TRADING-2348_DISABLED_SCHEDULER_WIRING_SMOKE_DRY_RUN_AND_GUARDRAIL_EVIDENCE"
        in task_register
    )
