from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_scheduler_disabled_wiring_fixtures import (
    build_high_intensity_scheduler_disabled_wiring_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    NEXT_2348_TASK,
    STATUS,
    HighIntensitySchedulerDisabledWiringError,
    build_disabled_wiring_guardrail_status,
    build_disabled_wiring_implementation_manifest,
    build_high_intensity_2348_readiness_checklist,
    build_high_intensity_2348_task_route,
    build_no_real_scheduler_assertion,
    load_high_intensity_scheduler_disabled_wiring_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_disabled_wiring_fixture(tmp_path)
    return load_high_intensity_scheduler_disabled_wiring_inputs(
        wiring_plan_dir=fixture["wiring_plan_dir"],
        scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
    )


def test_disabled_wiring_loader_reads_required_artifacts(tmp_path: Path) -> None:
    inputs = _load_fixture_inputs(tmp_path)

    summary = inputs["wiring_plan"]["summary"]
    scheduler_summary = inputs["scheduler_dry_run"]["summary"]
    assert summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert summary["2347_readiness_status"] == "READY_FOR_2347_WITH_CAVEATS"
    assert scheduler_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )


def test_disabled_wiring_loader_fails_closed_on_bad_2346_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_disabled_wiring_fixture(tmp_path)
    route_path = fixture["wiring_plan_dir"] / "high_intensity_2347_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2347_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerDisabledWiringError):
        load_high_intensity_scheduler_disabled_wiring_inputs(
            wiring_plan_dir=fixture["wiring_plan_dir"],
            scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        )


def test_disabled_wiring_loader_fails_closed_on_scheduler_enabled(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_disabled_wiring_fixture(tmp_path)
    summary_path = (
        fixture["wiring_plan_dir"]
        / "high_intensity_scheduler_wiring_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["scheduler_enabled"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerDisabledWiringError):
        load_high_intensity_scheduler_disabled_wiring_inputs(
            wiring_plan_dir=fixture["wiring_plan_dir"],
            scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        )


def test_disabled_wiring_manifest_keeps_all_guardrails_disabled(
    tmp_path: Path,
) -> None:
    manifest = build_disabled_wiring_implementation_manifest(
        inputs=_load_fixture_inputs(tmp_path)
    )

    assert manifest["status"] == STATUS
    assert manifest["scheduler_enabled"] is False
    assert manifest["manual_run_only"] is True
    assert manifest["dry_run_only"] is True
    assert manifest["event_append_enabled"] is False
    assert manifest["outcome_binding_enabled"] is False
    assert manifest["paper_shadow_enabled"] is False
    assert manifest["production_enabled"] is False
    assert manifest["broker_action_enabled"] is False
    assert manifest["promotion_allowed"] is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("scheduler_enabled", True),
        ("event_append_enabled", True),
        ("outcome_binding_enabled", True),
        ("paper_shadow_enabled", True),
        ("production_enabled", True),
        ("broker_action_enabled", True),
        ("broker_action", "send_order"),
        ("target_weight", {"QQQ": 1.0}),
        ("rebalance_instruction", "buy QQQ"),
        ("real_scheduler_created", True),
    ],
)
def test_disabled_wiring_guardrail_fails_closed_on_forbidden_fields(
    field: str,
    value: object,
) -> None:
    guardrails = build_disabled_wiring_guardrail_status(
        manifest={"scheduler_enabled": False},
        extra_payloads=[{field: value}],
    )

    assert guardrails["guardrail_status"] == "FAIL_CLOSED_TRIGGERED"
    assert guardrails["safety_error_count"] >= 1


def test_no_real_scheduler_assertion_requires_all_scheduler_surfaces_false(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    manifest = build_disabled_wiring_implementation_manifest(inputs=inputs)
    guardrails = build_disabled_wiring_guardrail_status(
        manifest=manifest,
        inputs=inputs,
    )
    assertion = build_no_real_scheduler_assertion(
        manifest=manifest,
        guardrails=guardrails,
    )

    assert assertion["assertion_status"] == "PASS"
    assert assertion["real_scheduler_fields"]["cron_entry_created"] is False
    assert assertion["real_scheduler_fields"]["windows_task_created"] is False
    assert assertion["real_scheduler_fields"]["github_action_schedule_created"] is False
    assert assertion["scheduled_tasks_config_modified"] is False


def test_2348_route_points_to_smoke_dry_run_with_caveats(tmp_path: Path) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    manifest = build_disabled_wiring_implementation_manifest(inputs=inputs)
    guardrails = build_disabled_wiring_guardrail_status(
        manifest=manifest,
        inputs=inputs,
    )
    assertion = build_no_real_scheduler_assertion(
        manifest=manifest,
        guardrails=guardrails,
    )
    readiness = build_high_intensity_2348_readiness_checklist(
        manifest=manifest,
        guardrails=guardrails,
        no_real_scheduler=assertion,
    )
    route = build_high_intensity_2348_task_route(readiness=readiness)

    assert readiness["readiness_status"] == "READY_FOR_2348_WITH_CAVEATS"
    assert route["next_task"] == NEXT_2348_TASK
    assert "NO_BROKER_ACTION" in route["route_caveats"]


def test_disabled_wiring_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-disabled-wiring"
        in result.output
    )


def test_disabled_wiring_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_disabled_wiring_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-disabled-wiring",
            "--wiring-plan-dir",
            str(fixture["wiring_plan_dir"]),
            "--scheduler-dry-run-dir",
            str(fixture["scheduler_dry_run_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_disabled_wiring",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_disabled_wiring_summary.json",
        "high_intensity_scheduler_disabled_wiring_implementation_manifest.json",
        "high_intensity_scheduler_disabled_wiring_guardrail_status.json",
        "high_intensity_scheduler_disabled_wiring_referenced_artifacts.json",
        "high_intensity_scheduler_no_real_scheduler_assertion.json",
        "high_intensity_2348_readiness_checklist.json",
        "high_intensity_2348_task_route.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_disabled_wiring_summary.json")
    manifest = read_json(
        output_dir
        / "high_intensity_scheduler_disabled_wiring_implementation_manifest.json"
    )
    assert summary["status"] == STATUS
    assert summary["scheduler_enabled"] is False
    assert summary["event_append_enabled"] is False
    assert summary["outcome_binding_enabled"] is False
    assert summary["paper_shadow_enabled"] is False
    assert summary["production_enabled"] is False
    assert summary["broker_action_enabled"] is False
    assert summary["next_task"] == NEXT_2348_TASK
    assert manifest["real_scheduler_created"] is False
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_disabled_wiring_implementation.md"
    ).exists()


def test_registry_catalog_docs_reference_disabled_wiring_report() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_disabled_wiring" in registry
    assert "TRADING-2347 High-Intensity Risk-Cap Observe-Only Scheduler Disabled Wiring" in catalog
