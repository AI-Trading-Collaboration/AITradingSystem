from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_idempotency_replay_contract_plan_fixtures import (
    build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_idempotency_replay_contract_plan import (
    DUPLICATE_DETECTION_PLAN,
    NEXT_2358_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    REPLAY_NO_SIDE_EFFECT_CONTRACT,
    STABLE_SEMANTIC_HASH_CONTRACT,
    STATUS,
    VOLATILE_FIELD_EXCLUSION_RULE,
    HighIntensitySchedulerIdempotencyReplayContractPlanError,
    build_blocked_promotion_rationale,
    build_duplicate_detection_plan,
    build_high_intensity_2358_event_append_contract_route,
    build_idempotency_contract,
    build_idempotency_replay_contract_plan_package,
    build_idempotency_replay_source_artifact_review,
    build_replay_no_side_effect_contract,
    build_stable_semantic_hash_contract,
    build_volatile_field_exclusion_rule,
    load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture(
        tmp_path
    )
    return load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
        hardening_backlog_dir=fixture["hardening_backlog_dir"],
        kill_switch_dir=fixture["kill_switch_dir"],
    )


def _build_package_stack(
    inputs: dict,
) -> tuple[dict, dict, dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_idempotency_replay_source_artifact_review(inputs=inputs)
    idempotency_contract = build_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    stable_semantic_hash_contract = build_stable_semantic_hash_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    volatile_field_exclusion_rule = build_volatile_field_exclusion_rule(
        generated_at=generated_at,
        source_review=source_review,
    )
    duplicate_detection_plan = build_duplicate_detection_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    replay_no_side_effect_contract = build_replay_no_side_effect_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_idempotency_replay_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        idempotency_contract=idempotency_contract,
        stable_semantic_hash_contract=stable_semantic_hash_contract,
        volatile_field_exclusion_rule=volatile_field_exclusion_rule,
        duplicate_detection_plan=duplicate_detection_plan,
        replay_no_side_effect_contract=replay_no_side_effect_contract,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2358_event_append_contract_route(package=package)
    return (
        source_review,
        idempotency_contract,
        stable_semantic_hash_contract,
        volatile_field_exclusion_rule,
        duplicate_detection_plan,
        replay_no_side_effect_contract,
        package,
        route,
    )


def test_idempotency_replay_loader_reads_2356_2355_and_2354_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    kill_switch_summary = inputs["kill_switch_plan"]["summary"]
    hardening_summary = inputs["hardening_backlog"]["summary"]
    gap_summary = inputs["gap_closure"]["summary"]

    assert kill_switch_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_KILL_SWITCH_AND_DISABLED_ENFORCEMENT_EVIDENCE_"
        "PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert kill_switch_summary["owner_decision"] == OWNER_DECISION
    assert kill_switch_summary["evidence_chain_complete"] is True
    assert kill_switch_summary["kill_switch_contract_ready"] is True
    assert hardening_summary["hardening_backlog_ready"] is True
    assert hardening_summary["evidence_matrix_ready"] is True
    assert gap_summary["gap_closure_plan_ready"] is True
    assert inputs["kill_switch_plan"]["route"]["next_route"] == (
        "TRADING-2357_Observe_Only_Scheduler_Idempotency_And_Replay_Contract_Plan"
    )


def test_idempotency_replay_loader_fails_closed_on_bad_2356_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture(
        tmp_path
    )
    route_path = (
        fixture["kill_switch_dir"]
        / "high_intensity_2357_scheduler_idempotency_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2357_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerIdempotencyReplayContractPlanError):
        load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
            hardening_backlog_dir=fixture["hardening_backlog_dir"],
            kill_switch_dir=fixture["kill_switch_dir"],
        )


def test_idempotency_replay_loader_fails_closed_on_2356_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture(
        tmp_path
    )
    summary_path = (
        fixture["kill_switch_dir"]
        / "high_intensity_scheduler_kill_switch_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["cron_created"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerIdempotencyReplayContractPlanError):
        load_high_intensity_scheduler_idempotency_replay_contract_plan_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
            hardening_backlog_dir=fixture["hardening_backlog_dir"],
            kill_switch_dir=fixture["kill_switch_dir"],
        )


def test_idempotency_replay_contract_plan_blocks_promotion_and_routes_to_2358(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    (
        source_review,
        idempotency_contract,
        stable_contract,
        volatile_rule,
        duplicate_plan,
        replay_contract,
        package,
        route,
    ) = _build_package_stack(inputs)

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
        "TRADING-2356",
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
        "TRADING-2356",
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["idempotency_contract_ready"] is True
    assert package["replay_contract_ready"] is True
    assert package["stable_semantic_hash_contract_ready"] is True
    assert package["volatile_field_exclusion_rule_ready"] is True
    assert package["duplicate_detection_plan_ready"] is True
    assert package["replay_no_side_effect_contract_ready"] is True
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
    assert idempotency_contract["idempotency_contract_ready"] is True
    assert stable_contract["stable_semantic_hash_contract"] == (
        STABLE_SEMANTIC_HASH_CONTRACT
    )
    assert volatile_rule["volatile_field_exclusion_rule"] == (
        VOLATILE_FIELD_EXCLUSION_RULE
    )
    assert duplicate_plan["duplicate_detection_plan"] == DUPLICATE_DETECTION_PLAN
    assert replay_contract["replay_no_side_effect_contract"] == (
        REPLAY_NO_SIDE_EFFECT_CONTRACT
    )
    stable_fields = set(
        package["stable_semantic_hash_contract"]["stable_semantic_fields"]
    )
    volatile_fields = set(package["volatile_field_exclusion_rule"]["excluded_fields"])
    assert stable_fields.isdisjoint(volatile_fields)
    assert {"generated_at", "runtime_artifact", "duration_ms", "local_path"}.issubset(
        volatile_fields
    )
    assert package["duplicate_detection_plan"]["side_effect_allowed"] is False
    assert package["replay_no_side_effect_contract"]["must_not_append_event"] is True
    assert package["replay_no_side_effect_contract"]["must_not_bind_outcome"] is True
    assert package["replay_no_side_effect_contract"]["must_not_call_broker"] is True
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2358_ROUTE


def test_idempotency_replay_contract_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-idempotency-replay-"
        "contract-plan"
        in command_names
    )


def test_idempotency_replay_contract_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture(
        tmp_path
    )
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-idempotency-replay-"
            "contract-plan",
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
            "--kill-switch-dir",
            str(fixture["kill_switch_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_idempotency_replay_contract_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_idempotency_replay_contract_plan_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan.json",
        "high_intensity_scheduler_idempotency_replay_contract_plan_source_artifact_review.json",
        "high_intensity_scheduler_idempotency_contract.json",
        "high_intensity_scheduler_stable_semantic_hash_contract.json",
        "high_intensity_scheduler_volatile_field_exclusion_rule.json",
        "high_intensity_scheduler_duplicate_detection_plan.json",
        "high_intensity_scheduler_replay_no_side_effect_contract.json",
        "high_intensity_scheduler_idempotency_replay_blocked_promotion_rationale.json",
        "high_intensity_2358_event_append_contract_route.json",
        "high_intensity_scheduler_idempotency_replay_interpretation_boundary.json",
        "high_intensity_scheduler_idempotency_replay_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir
        / "high_intensity_scheduler_idempotency_replay_contract_plan_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan.json"
    )
    route = read_json(output_dir / "high_intensity_2358_event_append_contract_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["idempotency_contract_ready"] is True
    assert summary["stable_semantic_hash_contract_ready"] is True
    assert package["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2358_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_2358_event_append_contract_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_idempotency_replay() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_scheduler_idempotency_replay_contract_plan"
        in registry
    )
    assert (
        "TRADING-2357 High-Intensity Risk-Cap Observe-Only Scheduler "
        "Idempotency Replay Contract Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-idempotency-replay-"
        "contract-plan"
        in system_flow
    )
    assert (
        "TRADING-2357_OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_"
        "CONTRACT_PLAN"
        in task_register
    )
