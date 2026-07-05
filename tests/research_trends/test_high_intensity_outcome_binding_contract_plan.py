from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_outcome_binding_contract_plan_fixtures import (
    build_high_intensity_outcome_binding_contract_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    BINDING_IDEMPOTENCY_CONTRACT,
    BINDING_REPLAY_CONTRACT,
    BINDING_ROLLBACK_PLAN,
    NEXT_2360_ROUTE,
    OUTCOME_BINDING_CONTRACT,
    OUTCOME_BINDING_SCHEMA_CONTRACT,
    OUTCOME_STORE_MUTATION_GUARDRAIL,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensityOutcomeBindingContractPlanError,
    build_binding_idempotency_contract,
    build_binding_replay_contract,
    build_binding_rollback_plan,
    build_blocked_promotion_rationale,
    build_high_intensity_2360_paper_shadow_scope_route,
    build_outcome_binding_contract,
    build_outcome_binding_contract_plan_package,
    build_outcome_binding_schema_contract,
    build_outcome_binding_source_artifact_review,
    build_outcome_store_mutation_guardrail,
    load_high_intensity_outcome_binding_contract_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_outcome_binding_contract_plan_fixture(tmp_path)
    return load_high_intensity_outcome_binding_contract_plan_inputs(
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
        event_append_dir=fixture["event_append_dir"],
    )


def _build_package_stack(
    inputs: dict,
) -> tuple[dict, dict, dict, dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_outcome_binding_source_artifact_review(inputs=inputs)
    outcome_binding_contract = build_outcome_binding_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    schema_contract = build_outcome_binding_schema_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    idempotency_contract = build_binding_idempotency_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    replay_contract = build_binding_replay_contract(
        generated_at=generated_at,
        source_review=source_review,
    )
    mutation_guardrail = build_outcome_store_mutation_guardrail(
        generated_at=generated_at,
        source_review=source_review,
    )
    rollback_plan = build_binding_rollback_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_outcome_binding_contract_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        outcome_binding_contract=outcome_binding_contract,
        schema_contract=schema_contract,
        idempotency_contract=idempotency_contract,
        replay_contract=replay_contract,
        mutation_guardrail=mutation_guardrail,
        rollback_plan=rollback_plan,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2360_paper_shadow_scope_route(package=package)
    return (
        source_review,
        outcome_binding_contract,
        schema_contract,
        idempotency_contract,
        replay_contract,
        mutation_guardrail,
        rollback_plan,
        package,
        route,
    )


def test_outcome_binding_loader_reads_2358_2357_and_2356_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    event_append_summary = inputs["event_append_plan"]["summary"]
    idempotency_summary = inputs["idempotency_replay_plan"]["summary"]
    kill_switch_summary = inputs["kill_switch_plan"]["summary"]

    assert event_append_summary["status"] == (
        "OBSERVE_ONLY_EVENT_APPEND_CONTRACT_PLAN_READY_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert event_append_summary["owner_decision"] == OWNER_DECISION
    assert event_append_summary["evidence_chain_complete"] is True
    assert event_append_summary["event_append_contract_ready"] is True
    assert event_append_summary["event_schema_append_contract_ready"] is True
    assert inputs["event_append_plan"]["route"]["next_route"] == (
        "TRADING-2359_Observe_Only_Outcome_Binding_Contract_Plan"
    )
    assert idempotency_summary["idempotency_contract_ready"] is True
    assert idempotency_summary["replay_no_side_effect_contract_ready"] is True
    assert kill_switch_summary["kill_switch_contract_ready"] is True


def test_outcome_binding_loader_fails_closed_on_bad_2358_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binding_contract_plan_fixture(tmp_path)
    route_path = (
        fixture["event_append_dir"]
        / "high_intensity_2359_outcome_binding_contract_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2359_Bind_Outcome"
    write_json(route_path, route)

    with pytest.raises(HighIntensityOutcomeBindingContractPlanError):
        load_high_intensity_outcome_binding_contract_plan_inputs(
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
            event_append_dir=fixture["event_append_dir"],
        )


def test_outcome_binding_loader_fails_closed_on_2358_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binding_contract_plan_fixture(tmp_path)
    summary_path = (
        fixture["event_append_dir"]
        / "high_intensity_event_append_contract_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["outcome_binding_attempted"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityOutcomeBindingContractPlanError):
        load_high_intensity_outcome_binding_contract_plan_inputs(
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
            event_append_dir=fixture["event_append_dir"],
        )


def test_outcome_binding_contract_plan_blocks_promotion_and_routes_to_2360(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    (
        source_review,
        outcome_binding_contract,
        schema_contract,
        idempotency_contract,
        replay_contract,
        mutation_guardrail,
        rollback_plan,
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
        "TRADING-2358",
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
        "TRADING-2358",
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["outcome_binding_contract_ready"] is True
    assert package["outcome_binding_schema_contract_ready"] is True
    assert package["binding_idempotency_contract_ready"] is True
    assert package["binding_replay_contract_ready"] is True
    assert package["outcome_store_mutation_guardrail_ready"] is True
    assert package["binding_rollback_plan_ready"] is True
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
    assert package["outcome_store_mutated"] is False
    assert package["paper_shadow_enabled"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_enabled"] is False
    assert package["production_attempted"] is False
    assert package["broker_action_enabled"] is False
    assert package["broker_action_attempted"] is False
    assert package["outcome_binding_contract"] == OUTCOME_BINDING_CONTRACT
    assert package["outcome_binding_schema_contract"] == (
        OUTCOME_BINDING_SCHEMA_CONTRACT
    )
    assert package["binding_idempotency_contract"] == BINDING_IDEMPOTENCY_CONTRACT
    assert package["binding_replay_contract"] == BINDING_REPLAY_CONTRACT
    assert package["outcome_store_mutation_guardrail"] == (
        OUTCOME_STORE_MUTATION_GUARDRAIL
    )
    assert package["binding_rollback_plan"] == BINDING_ROLLBACK_PLAN
    assert outcome_binding_contract["outcome_binding_contract_ready"] is True
    assert schema_contract["outcome_binding_schema_contract_ready"] is True
    assert idempotency_contract["binding_idempotency_contract_ready"] is True
    assert replay_contract["binding_replay_contract_ready"] is True
    assert mutation_guardrail["outcome_store_mutation_guardrail_ready"] is True
    assert rollback_plan["binding_rollback_plan_ready"] is True
    assert "stable_semantic_hash" in (
        package["binding_idempotency_contract"]["idempotency_key_fields"]
    )
    assert package["binding_replay_contract"]["side_effect_allowed"] is False
    assert package["binding_replay_contract"]["must_not_bind_outcome"] is True
    assert package["binding_replay_contract"]["must_not_mutate_outcome_store"] is True
    assert package["binding_rollback_plan"]["rollback_strategy"] == (
        "PLAN_ONLY_NO_MUTATION"
    )
    assert package["outcome_store_mutation_guardrail"][
        "outcome_binding_enabled"
    ] is False
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2360_ROUTE


def test_outcome_binding_contract_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-outcome-binding-contract-plan"
        in command_names
    )


def test_outcome_binding_contract_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_outcome_binding_contract_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-outcome-binding-contract-plan",
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
            "--event-append-dir",
            str(fixture["event_append_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_outcome_binding_contract_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_outcome_binding_contract_plan_summary.json",
        "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.json",
        "high_intensity_outcome_binding_contract_plan_source_artifact_review.json",
        "high_intensity_outcome_binding_contract.json",
        "high_intensity_outcome_binding_schema_contract.json",
        "high_intensity_binding_idempotency_contract.json",
        "high_intensity_binding_replay_contract.json",
        "high_intensity_outcome_store_mutation_guardrail.json",
        "high_intensity_binding_rollback_plan.json",
        "high_intensity_outcome_binding_blocked_promotion_rationale.json",
        "high_intensity_2360_paper_shadow_scope_route.json",
        "high_intensity_outcome_binding_interpretation_boundary.json",
        "high_intensity_outcome_binding_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_outcome_binding_contract_plan_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.json"
    )
    route = read_json(output_dir / "high_intensity_2360_paper_shadow_scope_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["outcome_binding_contract_ready"] is True
    assert summary["outcome_binding_schema_contract_ready"] is True
    assert summary["binding_idempotency_contract_ready"] is True
    assert summary["binding_replay_contract_ready"] is True
    assert summary["outcome_store_mutation_guardrail_ready"] is True
    assert summary["binding_rollback_plan_ready"] is True
    assert package["promotion_allowed"] is False
    assert package["outcome_binding_enabled"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["outcome_store_mutated"] is False
    assert route["next_route"] == NEXT_2360_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.md"
    ).exists()
    assert (docs_root / "high_intensity_2360_paper_shadow_scope_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_outcome_binding() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan"
        in registry
    )
    assert (
        "TRADING-2359 High-Intensity Risk-Cap Observe-Only Outcome Binding "
        "Contract Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-outcome-binding-contract-plan"
        in system_flow
    )
    assert "TRADING-2359_OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN" in task_register
