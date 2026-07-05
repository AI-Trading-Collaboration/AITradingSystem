from __future__ import annotations

from pathlib import Path

from high_intensity_owner_decision_pause_checkpoint_fixtures import (
    build_high_intensity_owner_decision_pause_checkpoint_fixture,
    read_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_owner_decision_pause_checkpoint import (
    NEXT_OWNER_REASSESSMENT_ROUTE,
    OWNER_DECISION,
    STATUS,
    load_high_intensity_owner_decision_pause_checkpoint_inputs,
)


def test_owner_pause_loader_reads_2362_promotion_blocker_matrix(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_owner_decision_pause_checkpoint_fixture(tmp_path)
    inputs = load_high_intensity_owner_decision_pause_checkpoint_inputs(
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
        promotion_blocker_dir=fixture["promotion_blocker_dir"],
    )

    promotion_summary = inputs["promotion_blocker_matrix"]["summary"]
    assert promotion_summary["consolidated_blocker_matrix_ready"] is True
    assert promotion_summary["next_route"] == (
        "TRADING-2363_Observe_Only_Owner_Decision_And_Pause_Checkpoint"
    )


def test_owner_decision_pause_checkpoint_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-owner-decision-pause-checkpoint"
        in command_names
    )


def test_owner_decision_pause_checkpoint_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_owner_decision_pause_checkpoint_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-owner-decision-pause-checkpoint",
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
            "--promotion-blocker-dir",
            str(fixture["promotion_blocker_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    summary = read_json(
        output_dir / "high_intensity_owner_decision_pause_checkpoint_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint.json"
    )

    assert summary["status"] == STATUS
    assert summary["evidence_chain_complete"] is True
    assert summary["owner_decision_recorded"] is True
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["promotion_allowed"] is False
    assert summary["pause_checkpoint_recorded"] is True
    assert summary["continue_linear_guardrail_tasks"] is False
    assert summary["next_route"] == NEXT_OWNER_REASSESSMENT_ROUTE
    assert package["guardrail_summary"]["scheduler_enabled"] is False
    assert package["guardrail_summary"]["event_append_enabled"] is False
    assert package["guardrail_summary"]["outcome_binding_enabled"] is False
    assert package["guardrail_summary"]["paper_shadow_enabled"] is False
    assert package["guardrail_summary"]["production_enabled"] is False
    assert package["guardrail_summary"]["broker_action_enabled"] is False
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint.md"
    ).exists()
    assert (docs_root / "high_intensity_post_2363_owner_reassessment.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_owner_pause() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert (
        "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint"
        in registry
    )
    assert (
        "TRADING-2363 High-Intensity Risk-Cap Observe-Only Owner Decision Pause "
        "Checkpoint"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-owner-decision-pause-checkpoint"
        in system_flow
    )
    assert "TRADING-2363_OBSERVE_ONLY_OWNER_DECISION_PAUSE_CHECKPOINT" in (
        task_register
    )
