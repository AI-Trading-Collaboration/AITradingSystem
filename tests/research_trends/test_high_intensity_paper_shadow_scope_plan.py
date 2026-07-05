from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_paper_shadow_scope_plan_fixtures import (
    build_high_intensity_paper_shadow_scope_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    NEXT_2361_ROUTE,
    NO_BROKER_GUARDRAIL,
    OWNER_DECISION,
    PAPER_SHADOW_DAILY_REVIEW_PLAN,
    PAPER_SHADOW_OWNER_APPROVAL_REQUIREMENT,
    PAPER_SHADOW_SCOPE_DEFINITION,
    READINESS_STATUS,
    STATUS,
    HighIntensityPaperShadowScopePlanError,
    build_blocked_promotion_rationale,
    build_high_intensity_2361_production_broker_hard_blocker_route,
    build_no_broker_guardrail_plan,
    build_paper_shadow_daily_review_plan,
    build_paper_shadow_owner_approval_requirement,
    build_paper_shadow_scope_definition,
    build_paper_shadow_scope_plan_package,
    build_paper_shadow_scope_source_artifact_review,
    load_high_intensity_paper_shadow_scope_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_paper_shadow_scope_plan_fixture(tmp_path)
    return load_high_intensity_paper_shadow_scope_plan_inputs(
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
        outcome_binding_dir=fixture["outcome_binding_dir"],
    )


def _build_package_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_paper_shadow_scope_source_artifact_review(inputs=inputs)
    scope_definition = build_paper_shadow_scope_definition(
        generated_at=generated_at,
        source_review=source_review,
    )
    no_broker_guardrail = build_no_broker_guardrail_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    daily_review_plan = build_paper_shadow_daily_review_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    owner_approval_requirement = build_paper_shadow_owner_approval_requirement(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_paper_shadow_scope_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        scope_definition=scope_definition,
        no_broker_guardrail=no_broker_guardrail,
        daily_review_plan=daily_review_plan,
        owner_approval_requirement=owner_approval_requirement,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2361_production_broker_hard_blocker_route(
        package=package
    )
    return (
        source_review,
        scope_definition,
        no_broker_guardrail,
        daily_review_plan,
        package,
        route,
    )


def test_paper_shadow_loader_reads_2359_2358_and_2357_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    outcome_binding_summary = inputs["outcome_binding_plan"]["summary"]
    event_append_summary = inputs["event_append_plan"]["summary"]
    idempotency_summary = inputs["idempotency_replay_plan"]["summary"]

    assert outcome_binding_summary["status"] == (
        "OBSERVE_ONLY_OUTCOME_BINDING_CONTRACT_PLAN_READY_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert outcome_binding_summary["owner_decision"] == OWNER_DECISION
    assert outcome_binding_summary["evidence_chain_complete"] is True
    assert outcome_binding_summary["outcome_binding_contract_ready"] is True
    assert outcome_binding_summary["outcome_binding_schema_contract_ready"] is True
    assert outcome_binding_summary["binding_idempotency_contract_ready"] is True
    assert outcome_binding_summary["binding_replay_contract_ready"] is True
    assert outcome_binding_summary["outcome_store_mutation_guardrail_ready"] is True
    assert outcome_binding_summary["binding_rollback_plan_ready"] is True
    assert inputs["outcome_binding_plan"]["route"]["next_route"] == (
        "TRADING-2360_Observe_Only_Paper_Shadow_Scope_And_No_Broker_Guardrail_Plan"
    )
    assert event_append_summary["event_append_contract_ready"] is True
    assert idempotency_summary["idempotency_contract_ready"] is True
    assert idempotency_summary["replay_no_side_effect_contract_ready"] is True


def test_paper_shadow_loader_fails_closed_on_bad_2359_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_paper_shadow_scope_plan_fixture(tmp_path)
    route_path = (
        fixture["outcome_binding_dir"]
        / "high_intensity_2360_paper_shadow_scope_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2360_Enable_Paper_Shadow"
    write_json(route_path, route)

    with pytest.raises(HighIntensityPaperShadowScopePlanError):
        load_high_intensity_paper_shadow_scope_plan_inputs(
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
            outcome_binding_dir=fixture["outcome_binding_dir"],
        )


def test_paper_shadow_loader_fails_closed_on_2359_broker_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_paper_shadow_scope_plan_fixture(tmp_path)
    summary_path = (
        fixture["outcome_binding_dir"]
        / "high_intensity_outcome_binding_contract_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["guardrail_summary"]["broker_action_enabled"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensityPaperShadowScopePlanError):
        load_high_intensity_paper_shadow_scope_plan_inputs(
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
            outcome_binding_dir=fixture["outcome_binding_dir"],
        )


def test_paper_shadow_scope_plan_blocks_broker_and_routes_to_2361(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    (
        source_review,
        scope_definition,
        no_broker_guardrail,
        daily_review_plan,
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
        "TRADING-2359",
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["paper_shadow_scope_plan_ready"] is True
    assert package["no_broker_guardrail_plan_ready"] is True
    assert package["paper_shadow_daily_review_plan_ready"] is True
    assert package["paper_shadow_owner_approval_requirement_ready"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["event_append_enabled"] is False
    assert package["event_append_attempted"] is False
    assert package["outcome_binding_enabled"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["outcome_store_mutated"] is False
    assert package["paper_shadow_enabled"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_enabled"] is False
    assert package["broker_action_enabled"] is False
    assert package["broker_action_attempted"] is False
    assert package["paper_shadow_scope_definition"] == PAPER_SHADOW_SCOPE_DEFINITION
    assert package["no_broker_guardrail"] == NO_BROKER_GUARDRAIL
    assert package["paper_shadow_daily_review_plan"] == PAPER_SHADOW_DAILY_REVIEW_PLAN
    assert package["paper_shadow_owner_approval_requirement"] == (
        PAPER_SHADOW_OWNER_APPROVAL_REQUIREMENT
    )
    assert scope_definition["paper_shadow_scope_plan_ready"] is True
    assert no_broker_guardrail["no_broker_guardrail_plan_ready"] is True
    assert daily_review_plan["paper_shadow_daily_review_plan_ready"] is True
    assert package["paper_shadow_scope_definition"]["scope_mode"] == (
        "DISABLED_SCOPE_PLAN_ONLY"
    )
    assert package["no_broker_guardrail"]["must_block_broker_api_import"] is True
    assert package["no_broker_guardrail"]["must_block_order_creation"] is True
    assert package["no_broker_guardrail"]["must_block_order_preview_to_broker"] is True
    assert package["no_broker_guardrail"]["must_block_any_capital_at_risk"] is True
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2361_ROUTE


def test_paper_shadow_scope_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert "high-intensity-risk-cap-observe-only-paper-shadow-scope-plan" in (
        command_names
    )


def test_paper_shadow_scope_plan_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_paper_shadow_scope_plan_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-paper-shadow-scope-plan",
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
            "--outcome-binding-dir",
            str(fixture["outcome_binding_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_paper_shadow_scope_plan",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_paper_shadow_scope_plan_summary.json",
        "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.json",
        "high_intensity_paper_shadow_scope_plan_source_artifact_review.json",
        "high_intensity_paper_shadow_scope_definition.json",
        "high_intensity_no_broker_guardrail_plan.json",
        "high_intensity_paper_shadow_daily_review_plan.json",
        "high_intensity_paper_shadow_owner_approval_requirement.json",
        "high_intensity_paper_shadow_blocked_promotion_rationale.json",
        "high_intensity_2361_production_broker_hard_blocker_route.json",
        "high_intensity_paper_shadow_scope_interpretation_boundary.json",
        "high_intensity_paper_shadow_scope_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(output_dir / "high_intensity_paper_shadow_scope_plan_summary.json")
    package = read_json(
        output_dir / "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.json"
    )
    route = read_json(
        output_dir / "high_intensity_2361_production_broker_hard_blocker_route.json"
    )
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["paper_shadow_scope_plan_ready"] is True
    assert summary["no_broker_guardrail_plan_ready"] is True
    assert summary["paper_shadow_daily_review_plan_ready"] is True
    assert summary["paper_shadow_owner_approval_requirement_ready"] is True
    assert package["promotion_allowed"] is False
    assert package["paper_shadow_enabled"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["broker_action_enabled"] is False
    assert package["broker_action_attempted"] is False
    assert package["no_broker_guardrail"]["must_block_any_capital_at_risk"] is True
    assert route["next_route"] == NEXT_2361_ROUTE
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.md"
    ).exists()
    assert (
        docs_root / "high_intensity_2361_production_broker_hard_blocker_route.md"
    ).exists()


def test_registry_catalog_system_flow_and_task_register_reference_paper_shadow() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan" in registry
    assert (
        "TRADING-2360 High-Intensity Risk-Cap Observe-Only Paper-Shadow Scope "
        "No-Broker Guardrail Plan"
        in catalog
    )
    assert "high-intensity-risk-cap-observe-only-paper-shadow-scope-plan" in (
        system_flow
    )
    assert (
        "TRADING-2360_OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN"
        in task_register
    )
