from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_kill_switch_plan_fixtures import (
    build_high_intensity_scheduler_kill_switch_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    DISABLED_ENFORCEMENT_MATRIX,
    KILL_SWITCH_CONTRACT,
    NEXT_2357_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensitySchedulerKillSwitchPlanError,
    build_blocked_promotion_rationale,
    build_disabled_enforcement_matrix,
    build_high_intensity_2357_scheduler_idempotency_route,
    build_kill_switch_contract,
    build_kill_switch_plan_package,
    build_kill_switch_source_artifact_review,
    build_manual_review_required_assertions,
    build_no_real_scheduler_creation_assertions,
    load_high_intensity_scheduler_kill_switch_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_kill_switch_plan_fixture(tmp_path)
    return load_high_intensity_scheduler_kill_switch_plan_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
        hardening_backlog_dir=fixture["hardening_backlog_dir"],
    )


def _build_package_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_kill_switch_source_artifact_review(inputs=inputs)
    contract = build_kill_switch_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    matrix = build_disabled_enforcement_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    no_scheduler = build_no_real_scheduler_creation_assertions(
        generated_at=generated_at,
        source_review=source_review,
    )
    manual_review = build_manual_review_required_assertions(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_kill_switch_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        kill_switch_contract=contract,
        disabled_enforcement_matrix=matrix,
        no_real_scheduler_assertions=no_scheduler,
        manual_review_assertions=manual_review,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2357_scheduler_idempotency_route(package=package)
    return source_review, contract, matrix, no_scheduler, manual_review, package, route


def test_kill_switch_loader_reads_2355_2354_and_2353_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    owner_summary = inputs["owner_decision"]["summary"]
    gap_summary = inputs["gap_closure"]["summary"]
    hardening_summary = inputs["hardening_backlog"]["summary"]

    assert owner_summary["owner_decision"] == OWNER_DECISION
    assert gap_summary["owner_decision"] == OWNER_DECISION
    assert gap_summary["gap_closure_plan_ready"] is True
    assert hardening_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_HARDENING_BACKLOG_AND_EVIDENCE_MATRIX_READY_"
        "WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert hardening_summary["owner_decision"] == OWNER_DECISION
    assert hardening_summary["evidence_chain_complete"] is True
    assert hardening_summary["hardening_backlog_ready"] is True
    assert hardening_summary["evidence_matrix_ready"] is True
    assert inputs["hardening_backlog"]["route"]["next_route"] == (
        "TRADING-2356_Observe_Only_Scheduler_Kill_Switch_And_Disabled_"
        "Enforcement_Evidence_Plan"
    )


def test_kill_switch_loader_fails_closed_on_bad_2355_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_kill_switch_plan_fixture(tmp_path)
    route_path = (
        fixture["hardening_backlog_dir"]
        / "high_intensity_2356_scheduler_kill_switch_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2356_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerKillSwitchPlanError):
        load_high_intensity_scheduler_kill_switch_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
            hardening_backlog_dir=fixture["hardening_backlog_dir"],
        )


def test_kill_switch_loader_fails_closed_on_2355_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_kill_switch_plan_fixture(tmp_path)
    summary_path = (
        fixture["hardening_backlog_dir"]
        / "high_intensity_scheduler_hardening_backlog_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["cron_created"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerKillSwitchPlanError):
        load_high_intensity_scheduler_kill_switch_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
            hardening_backlog_dir=fixture["hardening_backlog_dir"],
        )


def test_kill_switch_plan_blocks_promotion_and_routes_to_2357(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_review, contract, matrix, no_scheduler, manual_review, package, route = (
        _build_package_stack(inputs)
    )

    assert source_review["source_tasks"] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
        "TRADING-2353",
        "TRADING-2354",
        "TRADING-2355",
    ]
    assert [row["task"] for row in source_review["source_task_evidence"]] == [
        "TRADING-2347",
        "TRADING-2348",
        "TRADING-2349",
        "TRADING-2350",
        "TRADING-2351",
        "TRADING-2352",
        "TRADING-2353",
        "TRADING-2354",
        "TRADING-2355",
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["kill_switch_contract_ready"] is True
    assert package["disabled_enforcement_evidence_plan_ready"] is True
    assert package["no_real_scheduler_creation_assertions_ready"] is True
    assert package["manual_review_required_assertions_ready"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["manual_run_executed"] is False
    assert package["event_append_enabled"] is False
    assert package["event_append_attempted"] is False
    assert package["outcome_binding_enabled"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["paper_shadow_enabled"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_enabled"] is False
    assert package["production_attempted"] is False
    assert package["broker_action_enabled"] is False
    assert package["broker_action_attempted"] is False
    assert set(contract["kill_switch_contract"]) == set(KILL_SWITCH_CONTRACT)
    assert set(matrix["disabled_enforcement_matrix"]) == set(
        DISABLED_ENFORCEMENT_MATRIX
    )
    assert no_scheduler["no_real_scheduler_creation_assertions_ready"] is True
    assert manual_review["manual_review_required_assertions_ready"] is True
    for item in matrix["disabled_enforcement_matrix"].values():
        assert item["side_effect_allowed"] is False
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2357_ROUTE


def test_kill_switch_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-kill-switch-plan"
        in command_names
    )


def test_kill_switch_plan_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_kill_switch_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-kill-switch-plan",
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
            "--gap-closure-dir",
            str(fixture["gap_closure_dir"]),
            "--hardening-backlog-dir",
            str(fixture["hardening_backlog_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_kill_switch_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_kill_switch_plan_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.json",
        "high_intensity_scheduler_kill_switch_plan_source_artifact_review.json",
        "high_intensity_scheduler_kill_switch_contract.json",
        "high_intensity_scheduler_disabled_enforcement_matrix.json",
        "high_intensity_scheduler_no_real_scheduler_creation_assertions.json",
        "high_intensity_scheduler_manual_review_required_assertions.json",
        "high_intensity_scheduler_kill_switch_blocked_promotion_rationale.json",
        "high_intensity_2357_scheduler_idempotency_route.json",
        "high_intensity_scheduler_kill_switch_plan_interpretation_boundary.json",
        "high_intensity_scheduler_kill_switch_plan_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_scheduler_kill_switch_plan_summary.json")
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.json"
    )
    route = read_json(output_dir / "high_intensity_2357_scheduler_idempotency_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["kill_switch_contract_ready"] is True
    assert summary["disabled_enforcement_evidence_plan_ready"] is True
    assert package["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2357_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_2357_scheduler_idempotency_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_kill_switch() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan" in registry
    assert (
        "TRADING-2356 High-Intensity Risk-Cap Observe-Only Scheduler "
        "Kill-Switch Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-kill-switch-plan"
        in system_flow
    )
    assert (
        "TRADING-2356_OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_"
        "ENFORCEMENT_EVIDENCE_PLAN"
        in task_register
    )
