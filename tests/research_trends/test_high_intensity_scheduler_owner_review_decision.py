from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_owner_review_decision_fixtures import (
    build_high_intensity_scheduler_owner_review_decision_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    EXPLICIT_NON_APPROVAL_LIST,
    NEXT_2354_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensitySchedulerOwnerReviewDecisionError,
    build_explicit_non_approval_list,
    build_high_intensity_2354_gap_closure_route,
    build_owner_decision_reasons,
    build_owner_decision_source_artifact_review,
    build_owner_review_decision_record,
    load_high_intensity_scheduler_owner_review_decision_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_owner_review_decision_fixture(tmp_path)
    return load_high_intensity_scheduler_owner_review_decision_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
    )


def _build_decision_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_owner_decision_source_artifact_review(inputs=inputs)
    explicit_non_approval = build_explicit_non_approval_list(
        generated_at=generated_at,
        source_review=source_review,
    )
    decision_reasons = build_owner_decision_reasons(
        generated_at=generated_at,
        explicit_non_approval=explicit_non_approval,
    )
    decision_record = build_owner_review_decision_record(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        explicit_non_approval=explicit_non_approval,
        decision_reasons=decision_reasons,
        owner_decision=OWNER_DECISION,
    )
    route = build_high_intensity_2354_gap_closure_route(
        decision_record=decision_record,
        source_review=source_review,
    )
    return (
        source_review,
        explicit_non_approval,
        decision_reasons,
        decision_record,
        route,
    )


def test_owner_review_decision_loader_reads_2347_to_2352_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    audit_summary = inputs["audit_package"]["summary"]

    assert audit_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_AUDIT_PACKAGE_READY_FOR_OWNER_REVIEW_WITH_"
        "CAVEATS_PROMOTION_BLOCKED"
    )
    assert audit_summary["readiness"] == "READY_FOR_2353_WITH_CAVEATS"
    assert inputs["audit_package"]["route"]["next_route"] == (
        "TRADING-2353_Observe_Only_Scheduler_Owner_Review_Decision_Record"
    )


def test_owner_review_decision_loader_fails_closed_on_bad_2352_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_owner_review_decision_fixture(tmp_path)
    route_path = (
        fixture["audit_package_dir"]
        / "high_intensity_2353_owner_review_decision_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2353_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerOwnerReviewDecisionError):
        load_high_intensity_scheduler_owner_review_decision_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
        )


def test_owner_review_decision_loader_fails_closed_on_2352_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_owner_review_decision_fixture(tmp_path)
    side_effect_path = (
        fixture["audit_package_dir"]
        / "high_intensity_scheduler_audit_package_side_effect_summary.json"
    )
    side_effects = read_json(side_effect_path)
    side_effects["side_effect_summary"]["paper_shadow_attempted"] = True
    write_json(side_effect_path, side_effects)

    with pytest.raises(HighIntensitySchedulerOwnerReviewDecisionError):
        load_high_intensity_scheduler_owner_review_decision_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
        )


def test_owner_review_decision_keeps_scheduler_disabled_and_routes_to_2354(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_review, explicit_non_approval, reasons, decision_record, route = (
        _build_decision_stack(inputs)
    )

    assert source_review["source_tasks"] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
    ]
    assert decision_record["status"] == STATUS
    assert decision_record["evidence_chain_complete"] is True
    assert decision_record["owner_review_recorded"] is True
    assert decision_record["owner_decision_recorded"] is True
    assert decision_record["owner_decision"] == OWNER_DECISION
    assert decision_record["promotion_decision"] == "BLOCKED"
    assert decision_record["promotion_allowed"] is False
    assert decision_record["scheduler_enabled"] is False
    assert decision_record["manual_run_only"] is True
    assert decision_record["dry_run_only"] is True
    assert decision_record["manual_run_executed"] is False
    assert decision_record["event_append_attempted"] is False
    assert decision_record["outcome_binding_attempted"] is False
    assert decision_record["paper_shadow_attempted"] is False
    assert decision_record["production_attempted"] is False
    assert decision_record["broker_action_attempted"] is False
    assert explicit_non_approval["explicit_non_approval_list"] == (
        EXPLICIT_NON_APPROVAL_LIST
    )
    assert reasons["explicit_non_approval_complete"] is True
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2354_ROUTE


def test_owner_review_decision_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-owner-review-decision"
        in command_names
    )


def test_owner_review_decision_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_owner_review_decision_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-owner-review-decision",
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
            "--audit-package-dir",
            str(fixture["audit_package_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--owner-decision",
            OWNER_DECISION,
            "--mode",
            "observe_only_scheduler_owner_review_decision",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_owner_review_decision_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.json",
        "high_intensity_scheduler_owner_review_decision_source_artifact_review.json",
        "high_intensity_scheduler_owner_review_explicit_non_approval.json",
        "high_intensity_scheduler_owner_review_decision_reasons.json",
        "high_intensity_2354_gap_closure_route.json",
        "high_intensity_scheduler_owner_review_decision_interpretation_boundary.json",
        "high_intensity_scheduler_owner_review_decision_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_scheduler_owner_review_decision_summary.json"
    )
    decision_record = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.json"
    )
    route = read_json(output_dir / "high_intensity_2354_gap_closure_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert decision_record["owner_review_recorded"] is True
    assert decision_record["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2354_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision.md"
    ).exists()
    assert (docs_root / "high_intensity_2354_gap_closure_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_owner_decision() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_scheduler_owner_review_decision"
        in registry
    )
    assert (
        "TRADING-2353 High-Intensity Risk-Cap Observe-Only Scheduler Owner "
        "Review Decision"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-owner-review-decision"
        in system_flow
    )
    assert (
        "TRADING-2353_OBSERVE_ONLY_SCHEDULER_OWNER_REVIEW_DECISION_RECORD"
        in task_register
    )
