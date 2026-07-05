from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_production_broker_hard_blocker_plan_fixtures import (
    build_high_intensity_production_broker_hard_blocker_plan_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    NEXT_2362_ROUTE,
    OWNER_DECISION,
    STATUS,
    HighIntensityProductionBrokerHardBlockerPlanError,
    load_high_intensity_production_broker_hard_blocker_plan_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_production_broker_hard_blocker_plan_fixture(
        tmp_path
    )
    return load_high_intensity_production_broker_hard_blocker_plan_inputs(
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
        paper_shadow_scope_dir=fixture["paper_shadow_scope_dir"],
    )


def test_production_broker_loader_reads_2360_2359_and_2358_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    paper_shadow_summary = inputs["paper_shadow_scope_plan"]["summary"]
    outcome_binding_summary = inputs["outcome_binding_plan"]["summary"]
    event_append_summary = inputs["event_append_plan"]["summary"]

    assert paper_shadow_summary["status"] == (
        "OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN_READY_"
        "WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert paper_shadow_summary["owner_decision"] == OWNER_DECISION
    assert paper_shadow_summary["next_route"] == (
        "TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan"
    )
    assert outcome_binding_summary["outcome_binding_contract_ready"] is True
    assert event_append_summary["event_append_contract_ready"] is True


def test_production_broker_loader_fails_closed_on_bad_2360_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_production_broker_hard_blocker_plan_fixture(
        tmp_path
    )
    route_path = (
        fixture["paper_shadow_scope_dir"]
        / "high_intensity_2361_production_broker_hard_blocker_route.json"
    )
    route = read_json(route_path)
    route["next_route"] = "TRADING-2361_Enable_Broker"
    write_json(route_path, route)

    with pytest.raises(HighIntensityProductionBrokerHardBlockerPlanError):
        load_high_intensity_production_broker_hard_blocker_plan_inputs(
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
            paper_shadow_scope_dir=fixture["paper_shadow_scope_dir"],
        )


def test_production_broker_plan_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-production-broker-hard-blocker-plan"
        in command_names
    )


def test_production_broker_plan_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_production_broker_hard_blocker_plan_fixture(
        tmp_path
    )
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-production-broker-hard-blocker-plan",
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
            "--paper-shadow-scope-dir",
            str(fixture["paper_shadow_scope_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    summary = read_json(
        output_dir / "high_intensity_production_broker_hard_blocker_plan_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.json"
    )
    route = read_json(
        output_dir / "high_intensity_2362_promotion_blocker_matrix_route.json"
    )

    assert summary["status"] == STATUS
    assert summary["production_hard_blocker_plan_ready"] is True
    assert summary["broker_hard_blocker_plan_ready"] is True
    assert summary["capital_at_risk_blocker_ready"] is True
    assert summary["human_confirmation_requirement_ready"] is True
    assert summary["production_enabled"] is False
    assert summary["production_attempted"] is False
    assert summary["broker_action_enabled"] is False
    assert summary["broker_action_attempted"] is False
    assert summary["capital_at_risk_allowed"] is False
    assert summary["promotion_allowed"] is False
    assert package["broker_hard_blocker_plan"]["broker_api_import_allowed"] is False
    assert route["next_route"] == NEXT_2362_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.md"
    ).exists()
    assert (
        docs_root / "high_intensity_2362_promotion_blocker_matrix_route.md"
    ).exists()


def test_registry_catalog_system_flow_and_task_register_reference_production_broker() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan"
        in registry
    )
    assert (
        "TRADING-2361 High-Intensity Risk-Cap Observe-Only Production Broker "
        "Hard-Blocker Plan"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-production-broker-hard-blocker-plan"
        in system_flow
    )
    assert "TRADING-2361_OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN" in (
        task_register
    )
