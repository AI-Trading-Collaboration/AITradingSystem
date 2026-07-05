from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_audit_package_fixtures import (
    build_high_intensity_scheduler_audit_package_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    NEXT_2353_ROUTE,
    STATUS,
    HighIntensitySchedulerAuditPackageError,
    build_audit_evidence_chain,
    build_audit_guardrail_summary,
    build_audit_package,
    build_audit_promotion_decision,
    build_audit_side_effect_summary,
    build_audit_source_artifact_review,
    build_high_intensity_2353_owner_review_decision_route,
    build_owner_review_checklist,
    load_high_intensity_scheduler_audit_package_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_audit_package_fixture(tmp_path)
    return load_high_intensity_scheduler_audit_package_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
    )


def _build_package_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_audit_source_artifact_review(inputs=inputs)
    evidence_chain = build_audit_evidence_chain(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
    )
    guardrail_summary = build_audit_guardrail_summary(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
    )
    side_effect_summary = build_audit_side_effect_summary(
        generated_at=generated_at,
        inputs=inputs,
        evidence_chain=evidence_chain,
    )
    owner_review_checklist = build_owner_review_checklist(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
    )
    promotion_decision = build_audit_promotion_decision(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
        owner_review_checklist=owner_review_checklist,
    )
    package = build_audit_package(
        generated_at=generated_at,
        source_review=source_review,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
        owner_review_checklist=owner_review_checklist,
        promotion_decision=promotion_decision,
    )
    route = build_high_intensity_2353_owner_review_decision_route(
        package=package,
        evidence_chain=evidence_chain,
        promotion_decision=promotion_decision,
    )
    return (
        evidence_chain,
        guardrail_summary,
        side_effect_summary,
        owner_review_checklist,
        promotion_decision,
        package,
        route,
    )


def test_audit_package_loader_reads_2347_to_2351_artifacts(tmp_path: Path) -> None:
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
    assert inputs["replay_validation"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION_PASSED_"
        "WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert inputs["replay_validation"]["summary"]["readiness"] == (
        "READY_FOR_2352_WITH_CAVEATS"
    )


def test_audit_package_loader_fails_closed_on_bad_2351_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_audit_package_fixture(tmp_path)
    route_path = (
        fixture["replay_validation_dir"]
        / "high_intensity_2352_scheduler_audit_package_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2352_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerAuditPackageError):
        load_high_intensity_scheduler_audit_package_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
        )


def test_audit_package_loader_fails_closed_on_2351_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_audit_package_fixture(tmp_path)
    side_effect_path = (
        fixture["replay_validation_dir"]
        / "high_intensity_scheduler_manual_run_replay_side_effect_assertions.json"
    )
    side_effects = read_json(side_effect_path)
    side_effects["side_effect_assertions"]["broker_action_attempted"] = True
    write_json(side_effect_path, side_effects)

    with pytest.raises(HighIntensitySchedulerAuditPackageError):
        load_high_intensity_scheduler_audit_package_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
        )


def test_audit_package_blocks_promotion_and_routes_to_2353(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    (
        evidence_chain,
        guardrails,
        side_effects,
        checklist,
        promotion_decision,
        package,
        route,
    ) = _build_package_stack(inputs)

    assert package["status"] == STATUS
    assert package["source_tasks"] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
    ]
    assert evidence_chain["evidence_chain_complete"] is True
    assert package["evidence_chain"]["disabled_wiring_implemented"] is True
    assert package["evidence_chain"]["smoke_dry_run_passed"] is True
    assert package["evidence_chain"]["manual_review_gate_ready"] is True
    assert package["evidence_chain"]["manual_run_dry_run_ready"] is True
    assert (
        package["evidence_chain"]["manual_run_replay_no_side_effect_passed"]
        is True
    )
    assert checklist["owner_review_required"] is True
    assert package["manual_review_required"] is True
    assert promotion_decision["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert guardrails["guardrail_summary"]["scheduler_enabled"] is False
    assert guardrails["guardrail_summary"]["manual_run_only"] is True
    assert guardrails["guardrail_summary"]["dry_run_only"] is True
    assert guardrails["guardrail_summary"]["manual_run_executed"] is False
    assert guardrails["guardrail_summary"]["event_append_enabled"] is False
    assert guardrails["guardrail_summary"]["outcome_binding_enabled"] is False
    assert guardrails["guardrail_summary"]["paper_shadow_enabled"] is False
    assert guardrails["guardrail_summary"]["production_enabled"] is False
    assert guardrails["guardrail_summary"]["broker_action_enabled"] is False
    assert side_effects["side_effect_summary"]["event_append_attempted"] is False
    assert side_effects["side_effect_summary"]["outcome_binding_attempted"] is False
    assert side_effects["side_effect_summary"]["paper_shadow_attempted"] is False
    assert side_effects["side_effect_summary"]["production_attempted"] is False
    assert side_effects["side_effect_summary"]["broker_action_attempted"] is False
    assert set(checklist["owner_review_checklist"]) == {
        "review_2347_disabled_wiring",
        "review_2348_smoke_evidence",
        "review_2349_manual_gate",
        "review_2350_manual_run_dry_run",
        "review_2351_replay_validation",
        "confirm_no_scheduler_enablement",
        "confirm_no_event_outcome_mutation",
        "confirm_no_paper_shadow_or_production_path",
        "confirm_no_broker_action",
        "confirm_next_step_scope",
    }
    assert route["readiness"] == "READY_FOR_2353_WITH_CAVEATS"
    assert route["next_route"] == NEXT_2353_ROUTE


def test_audit_package_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert "high-intensity-risk-cap-observe-only-scheduler-audit-package" in command_names


def test_audit_package_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_audit_package_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-audit-package",
            "--disabled-wiring-dir",
            str(fixture["disabled_wiring_dir"]),
            "--smoke-dry-run-dir",
            str(fixture["smoke_dry_run_dir"]),
            "--manual-review-gate-dir",
            str(fixture["manual_review_gate_dir"]),
            "--manual-run-dry-run-dir",
            str(fixture["manual_run_dry_run_dir"]),
            "--replay-validation-dir",
            str(fixture["replay_validation_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_audit_package",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_audit_package_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_audit_package.json",
        "high_intensity_scheduler_audit_package_source_artifact_review.json",
        "high_intensity_scheduler_audit_package_evidence_chain.json",
        "high_intensity_scheduler_audit_package_guardrail_summary.json",
        "high_intensity_scheduler_audit_package_side_effect_summary.json",
        "high_intensity_scheduler_owner_review_checklist.json",
        "high_intensity_scheduler_audit_package_promotion_decision.json",
        "high_intensity_2353_owner_review_decision_route.json",
        "high_intensity_scheduler_audit_package_interpretation_boundary.json",
        "high_intensity_scheduler_audit_package_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_audit_package_summary.json")
    package = read_json(
        output_dir / "high_intensity_risk_cap_observe_only_scheduler_audit_package.json"
    )
    checklist = read_json(
        output_dir / "high_intensity_scheduler_owner_review_checklist.json"
    )
    route = read_json(output_dir / "high_intensity_2353_owner_review_decision_route.json")
    assert summary["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert checklist["owner_review_required"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2353_ROUTE
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_scheduler_audit_package.md"
    ).exists()
    assert (docs_root / "high_intensity_2353_owner_review_decision_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_audit_package() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_audit_package" in registry
    assert (
        "TRADING-2352 High-Intensity Risk-Cap Observe-Only Scheduler Audit Package"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-audit-package" in system_flow
    )
    assert (
        "TRADING-2352_OBSERVE_ONLY_SCHEDULER_AUDIT_PACKAGE_AND_OWNER_REVIEW_CHECKLIST"
        in task_register
    )
