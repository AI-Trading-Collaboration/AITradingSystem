from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_event_append_contract_plan_fixtures import (
    build_high_intensity_event_append_contract_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    APPEND_IDEMPOTENCY_CONTRACT,
    APPEND_ROLLBACK_PLAN,
    DUPLICATE_DETECTION_CONTRACT,
    EVENT_APPEND_CONTRACT,
    EVENT_MUTATION_GUARDRAIL,
    EVENT_SCHEMA_APPEND_CONTRACT,
    NEXT_2359_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensityEventAppendContractPlanError,
    build_append_idempotency_contract,
    build_append_rollback_plan,
    build_blocked_promotion_rationale,
    build_duplicate_detection_contract,
    build_event_append_contract,
    build_event_append_contract_plan_package,
    build_event_append_source_artifact_review,
    build_event_mutation_guardrail,
    build_event_schema_append_contract,
    build_high_intensity_2359_outcome_binding_contract_route,
    load_high_intensity_event_append_contract_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_event_append_contract_plan_fixture(tmp_path)
    return load_high_intensity_event_append_contract_plan_inputs(
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
        idempotency_replay_dir=fixture["idempotency_replay_dir"],
    )


def _build_package_stack(
    inputs: dict,
) -> tuple[dict, dict, dict, dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_event_append_source_artifact_review(inputs=inputs)
    event_append_contract = build_event_append_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    schema_contract = build_event_schema_append_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    idempotency_contract = build_append_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    duplicate_contract = build_duplicate_detection_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    rollback_plan = build_append_rollback_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    mutation_guardrail = build_event_mutation_guardrail(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_event_append_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        event_append_contract=event_append_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        duplicate_contract=duplicate_contract,
        rollback_plan=rollback_plan,
        mutation_guardrail=mutation_guardrail,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2359_outcome_binding_contract_route(
        package=package
    )
    return (
        source_review,
        event_append_contract,
        schema_contract,
        idempotency_contract,
        duplicate_contract,
        rollback_plan,
        mutation_guardrail,
        package,
        route,
    )


def test_event_append_loader_reads_2357_2356_and_2355_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    idempotency_summary = inputs["idempotency_replay_plan"]["summary"]
    kill_switch_summary = inputs["kill_switch_plan"]["summary"]
    hardening_summary = inputs["hardening_backlog"]["summary"]

    assert idempotency_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_IDEMPOTENCY_AND_REPLAY_CONTRACT_PLAN_READY_"
        "WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert idempotency_summary["owner_decision"] == OWNER_DECISION
    assert idempotency_summary["evidence_chain_complete"] is True
    assert idempotency_summary["idempotency_contract_ready"] is True
    assert idempotency_summary["duplicate_detection_plan_ready"] is True
    assert inputs["idempotency_replay_plan"]["route"]["next_route"] == (
        "TRADING-2358_Observe_Only_Event_Append_Contract_Plan"
    )
    assert kill_switch_summary["kill_switch_contract_ready"] is True
    assert hardening_summary["hardening_backlog_ready"] is True


def test_event_append_loader_fails_closed_on_bad_2357_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_event_append_contract_plan_fixture(tmp_path)
    route_path = (
        fixture["idempotency_replay_dir"]
        / "high_intensity_2358_event_append_contract_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2358_Enable_Event_Append"
    write_json(route_path, route)

    with pytest.raises(HighIntensityEventAppendContractPlanError):
        load_high_intensity_event_append_contract_plan_inputs(
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
            idempotency_replay_dir=fixture["idempotency_replay_dir"],
        )


def test_event_append_loader_fails_closed_on_2357_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_event_append_contract_plan_fixture(tmp_path)
    summary_path = (
        fixture["idempotency_replay_dir"]
        / "high_intensity_scheduler_idempotency_replay_contract_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["event_append_attempted"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityEventAppendContractPlanError):
        load_high_intensity_event_append_contract_plan_inputs(
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
            idempotency_replay_dir=fixture["idempotency_replay_dir"],
        )


def test_event_append_contract_plan_blocks_promotion_and_routes_to_2359(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    (
        source_review,
        event_append_contract,
        schema_contract,
        idempotency_contract,
        duplicate_contract,
        rollback_plan,
        mutation_guardrail,
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
        "TRADING-2357",
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
        "TRADING-2357",
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["event_append_contract_ready"] is True
    assert package["event_schema_append_contract_ready"] is True
    assert package["append_idempotency_contract_ready"] is True
    assert package["duplicate_detection_contract_ready"] is True
    assert package["append_rollback_plan_ready"] is True
    assert package["event_mutation_guardrail_ready"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["manual_run_executed"] is False
    assert package["event_append_enabled"] is False
    assert package["event_append_attempted"] is False
    assert package["historical_event_log_mutated"] is False
    assert package["outcome_binding_enabled"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["paper_shadow_enabled"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_enabled"] is False
    assert package["production_attempted"] is False
    assert package["broker_action_enabled"] is False
    assert package["broker_action_attempted"] is False
    assert package["event_append_contract"] == EVENT_APPEND_CONTRACT
    assert package["event_schema_append_contract"] == EVENT_SCHEMA_APPEND_CONTRACT
    assert package["append_idempotency_contract"] == APPEND_IDEMPOTENCY_CONTRACT
    assert package["duplicate_detection_contract"] == DUPLICATE_DETECTION_CONTRACT
    assert package["append_rollback_plan"] == APPEND_ROLLBACK_PLAN
    assert package["event_mutation_guardrail"] == EVENT_MUTATION_GUARDRAIL
    assert event_append_contract["event_append_contract_ready"] is True
    assert schema_contract["event_schema_append_contract_ready"] is True
    assert idempotency_contract["append_idempotency_contract_ready"] is True
    assert duplicate_contract["duplicate_detection_contract_ready"] is True
    assert rollback_plan["append_rollback_plan_ready"] is True
    assert mutation_guardrail["event_mutation_guardrail_ready"] is True
    assert "stable_semantic_hash" in (
        package["append_idempotency_contract"]["idempotency_key_fields"]
    )
    assert (
        package["duplicate_detection_contract"]["duplicate_resolution"]
        == "BLOCK_APPEND_AND_REPORT_DUPLICATE"
    )
    assert package["duplicate_detection_contract"]["side_effect_allowed"] is False
    assert package["append_rollback_plan"]["rollback_strategy"] == (
        "PLAN_ONLY_NO_MUTATION"
    )
    assert package["event_mutation_guardrail"]["event_append_enabled"] is False
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2359_ROUTE


def test_event_append_contract_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-event-append-contract-plan"
        in command_names
    )


def test_event_append_contract_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_event_append_contract_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-event-append-contract-plan",
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
            "--idempotency-replay-dir",
            str(fixture["idempotency_replay_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_event_append_contract_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_event_append_contract_plan_summary.json",
        "high_intensity_risk_cap_observe_only_event_append_contract_plan.json",
        "high_intensity_event_append_contract_plan_source_artifact_review.json",
        "high_intensity_event_append_contract.json",
        "high_intensity_event_schema_append_contract.json",
        "high_intensity_append_idempotency_contract.json",
        "high_intensity_event_duplicate_detection_contract.json",
        "high_intensity_append_rollback_plan.json",
        "high_intensity_event_mutation_guardrail.json",
        "high_intensity_event_append_blocked_promotion_rationale.json",
        "high_intensity_2359_outcome_binding_contract_route.json",
        "high_intensity_event_append_interpretation_boundary.json",
        "high_intensity_event_append_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_event_append_contract_plan_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_event_append_contract_plan.json"
    )
    route = read_json(
        output_dir / "high_intensity_2359_outcome_binding_contract_route.json"
    )
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["event_append_contract_ready"] is True
    assert summary["event_schema_append_contract_ready"] is True
    assert summary["append_idempotency_contract_ready"] is True
    assert summary["duplicate_detection_contract_ready"] is True
    assert summary["append_rollback_plan_ready"] is True
    assert summary["event_mutation_guardrail_ready"] is True
    assert package["promotion_allowed"] is False
    assert package["event_append_enabled"] is False
    assert package["historical_event_log_mutated"] is False
    assert route["next_route"] == NEXT_2359_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_event_append_contract_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_2359_outcome_binding_contract_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_event_append() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_event_append_contract_plan"
        in registry
    )
    assert (
        "TRADING-2358 High-Intensity Risk-Cap Observe-Only Event Append Contract Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-event-append-contract-plan"
        in system_flow
    )
    assert "TRADING-2358_OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN" in task_register
