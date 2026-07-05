from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_gap_closure_plan_fixtures import (
    build_high_intensity_scheduler_gap_closure_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_gap_closure_plan import (
    GAP_CLOSURE_MATRIX,
    NEXT_2355_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensitySchedulerGapClosurePlanError,
    build_blocked_promotion_rationale,
    build_gap_closure_matrix,
    build_gap_closure_plan,
    build_gap_closure_source_artifact_review,
    build_high_intensity_2355_hardening_backlog_route,
    build_readiness_hardening_plan,
    load_high_intensity_scheduler_gap_closure_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_gap_closure_plan_fixture(tmp_path)
    return load_high_intensity_scheduler_gap_closure_plan_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
    )


def _build_plan_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_gap_closure_source_artifact_review(inputs=inputs)
    matrix = build_gap_closure_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    hardening_plan = build_readiness_hardening_plan(
        generated_at=generated_at,
        gap_closure_matrix=matrix,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
        gap_closure_matrix=matrix,
    )
    plan = build_gap_closure_plan(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        gap_closure_matrix=matrix,
        readiness_hardening_plan=hardening_plan,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2355_hardening_backlog_route(plan=plan)
    return source_review, matrix, hardening_plan, rationale, plan, route


def test_gap_closure_loader_reads_2352_and_2353_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    audit_summary = inputs["audit_package"]["summary"]
    owner_summary = inputs["owner_decision"]["summary"]

    assert audit_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_AUDIT_PACKAGE_READY_FOR_OWNER_REVIEW_WITH_"
        "CAVEATS_PROMOTION_BLOCKED"
    )
    assert owner_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_OWNER_REVIEW_DECISION_RECORDED_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert owner_summary["owner_decision"] == OWNER_DECISION
    assert owner_summary["readiness"] == "READY_FOR_2354_WITH_CAVEATS"
    assert inputs["owner_decision"]["route"]["next_route"] == (
        "TRADING-2354_Observe_Only_Scheduler_Gap_Closure_And_Readiness_Hardening_Plan"
    )


def test_gap_closure_loader_fails_closed_on_bad_2353_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_gap_closure_plan_fixture(tmp_path)
    route_path = fixture["owner_decision_dir"] / "high_intensity_2354_gap_closure_route.json"
    route = read_json(route_path)
    route["next_route"] = "TRADING-2354_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerGapClosurePlanError):
        load_high_intensity_scheduler_gap_closure_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
        )


def test_gap_closure_loader_fails_closed_on_2353_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_gap_closure_plan_fixture(tmp_path)
    summary_path = (
        fixture["owner_decision_dir"]
        / "high_intensity_scheduler_owner_review_decision_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["broker_action_attempted"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerGapClosurePlanError):
        load_high_intensity_scheduler_gap_closure_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
        )


def test_gap_closure_plan_blocks_promotion_and_routes_to_2355(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_review, matrix, hardening_plan, rationale, plan, route = (
        _build_plan_stack(inputs)
    )

    assert source_review["source_tasks"] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
        "TRADING-2353",
    ]
    assert [row["task"] for row in source_review["source_task_evidence"]] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
        "TRADING-2353",
    ]
    assert [row["task"] for row in plan["source_task_evidence"]] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
        "TRADING-2353",
    ]
    assert plan["status"] == STATUS
    assert plan["evidence_chain_complete"] is True
    assert plan["owner_decision"] == OWNER_DECISION
    assert plan["gap_closure_plan_ready"] is True
    assert plan["readiness_hardening_plan_ready"] is True
    assert plan["promotion_decision"] == "BLOCKED"
    assert plan["promotion_allowed"] is False
    assert plan["scheduler_enabled"] is False
    assert plan["manual_run_only"] is True
    assert plan["dry_run_only"] is True
    assert plan["manual_run_executed"] is False
    assert plan["event_append_attempted"] is False
    assert plan["outcome_binding_attempted"] is False
    assert plan["paper_shadow_attempted"] is False
    assert plan["production_attempted"] is False
    assert plan["broker_action_attempted"] is False
    assert set(matrix["gap_closure_matrix"]) == set(GAP_CLOSURE_MATRIX)
    for item in matrix["gap_closure_matrix"].values():
        assert item["current_status"] == "BLOCKED"
        assert item["allowed_in_this_task"] is False
    assert hardening_plan["readiness_hardening_plan_ready"] is True
    assert rationale["promotion_allowed"] is False
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2355_ROUTE


def test_gap_closure_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-gap-closure-plan"
        in command_names
    )


def test_gap_closure_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_gap_closure_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-gap-closure-plan",
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
            "--owner-decision-dir",
            str(fixture["owner_decision_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_gap_closure_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_gap_closure_plan_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.json",
        "high_intensity_scheduler_gap_closure_plan_source_artifact_review.json",
        "high_intensity_scheduler_gap_closure_matrix.json",
        "high_intensity_scheduler_readiness_hardening_plan.json",
        "high_intensity_scheduler_gap_closure_blocked_promotion_rationale.json",
        "high_intensity_2355_hardening_backlog_route.json",
        "high_intensity_scheduler_gap_closure_interpretation_boundary.json",
        "high_intensity_scheduler_gap_closure_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_gap_closure_plan_summary.json")
    plan = read_json(
        output_dir / "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.json"
    )
    route = read_json(output_dir / "high_intensity_2355_hardening_backlog_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["gap_closure_plan_ready"] is True
    assert summary["readiness_hardening_plan_ready"] is True
    assert plan["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2355_ROUTE
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_2355_hardening_backlog_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_gap_closure() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_gap_closure_plan" in registry
    assert (
        "TRADING-2354 High-Intensity Risk-Cap Observe-Only Scheduler Gap "
        "Closure Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-gap-closure-plan"
        in system_flow
    )
    assert (
        "TRADING-2354_OBSERVE_ONLY_SCHEDULER_GAP_CLOSURE_AND_READINESS_"
        "HARDENING_PLAN"
        in task_register
    )
