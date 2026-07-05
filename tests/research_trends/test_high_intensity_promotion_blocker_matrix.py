from __future__ import annotations

from pathlib import Path

from high_intensity_promotion_blocker_matrix_fixtures import (
    build_high_intensity_promotion_blocker_matrix_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    NEXT_2363_ROUTE,
    STATUS,
    load_high_intensity_promotion_blocker_matrix_inputs,
)


def test_promotion_blocker_loader_reads_2361_2360_and_2359_artifacts(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_promotion_blocker_matrix_fixture(tmp_path)
    inputs = load_high_intensity_promotion_blocker_matrix_inputs(
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
        production_broker_dir=fixture["production_broker_dir"],
    )

    production_summary = inputs["production_broker_hard_blocker_plan"]["summary"]
    paper_shadow_summary = inputs["paper_shadow_scope_plan"]["summary"]
    outcome_binding_summary = inputs["outcome_binding_plan"]["summary"]

    assert production_summary["production_hard_blocker_plan_ready"] is True
    assert paper_shadow_summary["paper_shadow_scope_plan_ready"] is True
    assert outcome_binding_summary["outcome_binding_contract_ready"] is True


def test_promotion_blocker_matrix_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-promotion-blocker-matrix"
        in command_names
    )


def test_promotion_blocker_matrix_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_promotion_blocker_matrix_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-promotion-blocker-matrix",
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
            "--production-broker-dir",
            str(fixture["production_broker_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    summary = read_json(
        output_dir / "high_intensity_promotion_blocker_matrix_summary.json"
    )
    package = read_json(
        output_dir / "high_intensity_risk_cap_observe_only_promotion_blocker_matrix.json"
    )
    route = read_json(output_dir / "high_intensity_2363_owner_decision_pause_route.json")

    assert summary["status"] == STATUS
    assert summary["consolidated_blocker_matrix_ready"] is True
    assert summary["safety_evidence_matrix_ready"] is True
    assert summary["promotion_allowed"] is False
    assert set(package["blocker_matrix"]) == {
        "scheduler_enablement",
        "event_append",
        "outcome_binding",
        "paper_shadow",
        "production",
        "broker_action_blocker",
    }
    for value in package["side_effect_summary"].values():
        assert value is False
    assert route["next_route"] == NEXT_2363_ROUTE
    assert (
        docs_root / "high_intensity_risk_cap_observe_only_promotion_blocker_matrix.md"
    ).exists()
    assert (docs_root / "high_intensity_2363_owner_decision_pause_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_promotion_blocker() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_promotion_blocker_matrix" in registry
    assert (
        "TRADING-2362 High-Intensity Risk-Cap Observe-Only Promotion Blocker "
        "Safety Evidence Matrix"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-promotion-blocker-matrix"
        in system_flow
    )
    assert (
        "TRADING-2362_OBSERVE_ONLY_CONSOLIDATED_PROMOTION_BLOCKER_AND_SAFETY_"
        "EVIDENCE_MATRIX"
        in task_register
    )
