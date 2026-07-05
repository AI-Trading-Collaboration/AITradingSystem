from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_hardening_backlog_fixtures import (
    build_high_intensity_scheduler_hardening_backlog_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    EVIDENCE_MATRIX,
    HARDENING_BACKLOG,
    NEXT_2356_ROUTE,
    OWNER_DECISION,
    READINESS_STATUS,
    STATUS,
    HighIntensitySchedulerHardeningBacklogError,
    build_blocked_promotion_rationale,
    build_evidence_matrix,
    build_hardening_backlog,
    build_hardening_backlog_package,
    build_hardening_backlog_source_artifact_review,
    build_high_intensity_2356_scheduler_kill_switch_route,
    load_high_intensity_scheduler_hardening_backlog_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_hardening_backlog_fixture(tmp_path)
    return load_high_intensity_scheduler_hardening_backlog_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
    )


def _build_package_stack(inputs: dict) -> tuple[dict, dict, dict, dict, dict, dict]:
    generated_at = datetime.now(tz=UTC)
    source_review = build_hardening_backlog_source_artifact_review(inputs=inputs)
    backlog = build_hardening_backlog(
        generated_at=generated_at,
        source_review=source_review,
    )
    matrix = build_evidence_matrix(
        generated_at=generated_at,
        hardening_backlog=backlog,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
        hardening_backlog=backlog,
        evidence_matrix=matrix,
    )
    package = build_hardening_backlog_package(
        generated_at=generated_at,
        source_review=source_review,
        hardening_backlog=backlog,
        evidence_matrix=matrix,
        blocked_promotion_rationale=rationale,
    )
    route = build_high_intensity_2356_scheduler_kill_switch_route(package=package)
    return source_review, backlog, matrix, rationale, package, route


def test_hardening_loader_reads_2354_and_2353_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    owner_summary = inputs["owner_decision"]["summary"]
    gap_summary = inputs["gap_closure"]["summary"]

    assert owner_summary["owner_decision"] == OWNER_DECISION
    assert gap_summary["status"] == (
        "OBSERVE_ONLY_SCHEDULER_GAP_CLOSURE_AND_READINESS_HARDENING_PLAN_READY_"
        "WITH_CAVEATS_PROMOTION_BLOCKED"
    )
    assert gap_summary["owner_decision"] == OWNER_DECISION
    assert gap_summary["evidence_chain_complete"] is True
    assert gap_summary["gap_closure_plan_ready"] is True
    assert gap_summary["readiness_hardening_plan_ready"] is True
    assert inputs["gap_closure"]["route"]["next_route"] == (
        "TRADING-2355_Observe_Only_Scheduler_Hardening_Backlog_And_Evidence_Matrix"
    )


def test_hardening_loader_fails_closed_on_bad_2354_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_hardening_backlog_fixture(tmp_path)
    route_path = fixture["gap_closure_dir"] / "high_intensity_2355_hardening_backlog_route.json"
    route = read_json(route_path)
    route["next_route"] = "TRADING-2355_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerHardeningBacklogError):
        load_high_intensity_scheduler_hardening_backlog_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
        )


def test_hardening_loader_fails_closed_on_2354_side_effect(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_hardening_backlog_fixture(tmp_path)
    summary_path = (
        fixture["gap_closure_dir"]
        / "high_intensity_scheduler_gap_closure_plan_summary.json"
    )
    summary = read_json(summary_path)
    summary["side_effect_summary"]["broker_action_attempted"] = True
    write_json(summary_path, summary)

    with pytest.raises(HighIntensitySchedulerHardeningBacklogError):
        load_high_intensity_scheduler_hardening_backlog_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
            manual_review_gate_dir=fixture["manual_review_gate_dir"],
            manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
            replay_validation_dir=fixture["replay_validation_dir"],
            audit_package_dir=fixture["audit_package_dir"],
            owner_decision_dir=fixture["owner_decision_dir"],
            gap_closure_dir=fixture["gap_closure_dir"],
        )


def test_hardening_backlog_blocks_promotion_and_routes_to_2356(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_review, backlog, matrix, rationale, package, route = (
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
    ]
    assert package["status"] == STATUS
    assert package["evidence_chain_complete"] is True
    assert package["owner_decision"] == OWNER_DECISION
    assert package["hardening_backlog_ready"] is True
    assert package["evidence_matrix_ready"] is True
    assert package["promotion_decision"] == "BLOCKED"
    assert package["promotion_allowed"] is False
    assert package["scheduler_enabled"] is False
    assert package["manual_run_only"] is True
    assert package["dry_run_only"] is True
    assert package["manual_run_executed"] is False
    assert package["event_append_attempted"] is False
    assert package["outcome_binding_attempted"] is False
    assert package["paper_shadow_attempted"] is False
    assert package["production_attempted"] is False
    assert package["broker_action_attempted"] is False
    assert {row["category"] for row in backlog["hardening_backlog"]} >= {
        "scheduler_enablement_guardrail",
        "scheduler_idempotency",
        "event_append_guardrail",
        "outcome_binding_guardrail",
        "paper_shadow_guardrail",
    }
    assert set(matrix["evidence_matrix"]) == set(EVIDENCE_MATRIX)
    assert len(backlog["hardening_backlog"]) == len(HARDENING_BACKLOG)
    for item in backlog["hardening_backlog"]:
        assert item["side_effect_allowed"] is False
        assert item["promotion_allowed_after_task"] is False
    for item in matrix["evidence_matrix"].values():
        assert item["side_effect_allowed"] is False
    assert rationale["promotion_allowed"] is False
    assert route["readiness"] == READINESS_STATUS
    assert route["next_route"] == NEXT_2356_ROUTE


def test_hardening_backlog_cli_is_registered() -> None:
    command_names = {command.name for command in trends_app.registered_commands}

    assert (
        "high-intensity-risk-cap-observe-only-scheduler-hardening-backlog"
        in command_names
    )


def test_hardening_backlog_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_hardening_backlog_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-hardening-backlog",
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
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_hardening_backlog",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_hardening_backlog_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.json",
        "high_intensity_scheduler_hardening_backlog_source_artifact_review.json",
        "high_intensity_scheduler_hardening_backlog_items.json",
        "high_intensity_scheduler_hardening_evidence_matrix.json",
        "high_intensity_scheduler_hardening_blocked_promotion_rationale.json",
        "high_intensity_2356_scheduler_kill_switch_route.json",
        "high_intensity_scheduler_hardening_backlog_interpretation_boundary.json",
        "high_intensity_scheduler_hardening_backlog_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_scheduler_hardening_backlog_summary.json"
    )
    package = read_json(
        output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.json"
    )
    route = read_json(output_dir / "high_intensity_2356_scheduler_kill_switch_route.json")
    assert summary["status"] == STATUS
    assert summary["owner_decision"] == OWNER_DECISION
    assert summary["hardening_backlog_ready"] is True
    assert summary["evidence_matrix_ready"] is True
    assert package["promotion_allowed"] is False
    assert route["next_route"] == NEXT_2356_ROUTE
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog.md"
    ).exists()
    assert (docs_root / "high_intensity_2356_scheduler_kill_switch_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_hardening() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_hardening_backlog" in registry
    assert (
        "TRADING-2355 High-Intensity Risk-Cap Observe-Only Scheduler "
        "Hardening Backlog"
        in catalog
    )
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-hardening-backlog"
        in system_flow
    )
    assert (
        "TRADING-2355_OBSERVE_ONLY_SCHEDULER_HARDENING_BACKLOG_AND_"
        "EVIDENCE_MATRIX"
        in task_register
    )
