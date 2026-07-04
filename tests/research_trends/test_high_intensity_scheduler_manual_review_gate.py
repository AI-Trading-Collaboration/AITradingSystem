from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from high_intensity_scheduler_manual_review_gate_fixtures import (
    build_high_intensity_scheduler_manual_review_gate_fixture,
    read_json,
    write_json,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    NEXT_2350_TASK,
    STATUS,
    HighIntensitySchedulerManualReviewGateError,
    build_high_intensity_2350_task_route,
    build_manual_review_gate_package,
    build_manual_review_promotion_decision,
    build_manual_review_source_artifact_review,
    load_high_intensity_scheduler_manual_review_gate_inputs,
)


def _load_fixture_inputs(tmp_path: Path) -> dict:
    fixture = build_high_intensity_scheduler_manual_review_gate_fixture(tmp_path)
    return load_high_intensity_scheduler_manual_review_gate_inputs(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
    )


def test_manual_review_gate_loader_reads_2347_and_2348_artifacts(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)

    assert inputs["disabled_wiring"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_"
        "PROMOTION_BLOCKED"
    )
    assert inputs["smoke_dry_run"]["summary"]["status"] == (
        "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_SMOKE_DRY_RUN_PASSED_WITH_"
        "CAVEATS_PROMOTION_BLOCKED"
    )
    assert inputs["smoke_dry_run"]["evidence"]["readiness"] == (
        "READY_FOR_2349_WITH_CAVEATS"
    )


def test_manual_review_gate_loader_fails_closed_on_bad_2348_route(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_review_gate_fixture(tmp_path)
    route_path = fixture["smoke_dry_run_dir"] / "high_intensity_2349_manual_review_route.json"
    route = read_json(route_path)
    route["next_route"] = "TRADING-2349_Enable_Scheduler"
    write_json(route_path, route)

    with pytest.raises(HighIntensitySchedulerManualReviewGateError):
        load_high_intensity_scheduler_manual_review_gate_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        )


def test_manual_review_gate_fails_closed_on_promotion_allowed(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_scheduler_manual_review_gate_fixture(tmp_path)
    evidence_path = (
        fixture["smoke_dry_run_dir"]
        / "high_intensity_scheduler_smoke_dry_run_evidence.json"
    )
    evidence = read_json(evidence_path)
    evidence["promotion_allowed"] = True
    write_json(evidence_path, evidence)

    with pytest.raises(HighIntensitySchedulerManualReviewGateError):
        load_high_intensity_scheduler_manual_review_gate_inputs(
            disabled_wiring_dir=fixture["disabled_wiring_dir"],
            smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        )


def test_manual_review_gate_package_blocks_promotion_and_routes_to_2350(
    tmp_path: Path,
) -> None:
    inputs = _load_fixture_inputs(tmp_path)
    source_review = build_manual_review_source_artifact_review(inputs=inputs)
    gate_package = build_manual_review_gate_package(
        generated_at=datetime.now(tz=UTC),
        inputs=inputs,
        source_review=source_review,
    )
    decision = build_manual_review_promotion_decision(
        generated_at=datetime.now(tz=UTC),
        gate_package=gate_package,
    )
    route = build_high_intensity_2350_task_route(
        gate_package=gate_package,
        promotion_decision=decision,
    )

    assert gate_package["status"] == STATUS
    assert gate_package["source_tasks"] == ["TRADING-2347", "TRADING-2348"]
    assert gate_package["promotion_decision"] == "BLOCKED"
    assert gate_package["promotion_allowed"] is False
    assert gate_package["scheduler_enabled"] is False
    assert gate_package["manual_run_only"] is True
    assert gate_package["dry_run_only"] is True
    assert gate_package["manual_review_required"] is True
    assert gate_package["paper_shadow_enabled"] is False
    assert gate_package["production_enabled"] is False
    assert gate_package["broker_action_enabled"] is False
    assert gate_package["review_findings"]["disabled_wiring_present"] is True
    assert gate_package["review_findings"]["smoke_dry_run_passed"] is True
    assert gate_package["review_findings"]["guardrail_evidence_present"] is True
    assert gate_package["review_findings"]["side_effect_assertions_present"] is True
    assert (
        gate_package["review_findings"]["promotion_evidence_sufficient_for_enablement"]
        is False
    )
    assert decision["promotion_decision"] == "BLOCKED"
    assert decision["owner_review_required"] is True
    assert decision["owner_review_completed"] is False
    assert route["readiness"] == "READY_FOR_2350_WITH_CAVEATS"
    assert route["next_route"] == NEXT_2350_TASK


def test_manual_review_gate_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert (
        "high-intensity-risk-cap-observe-only-scheduler-manual-review-gate"
        in result.output
    )


def test_manual_review_gate_cli_writes_outputs(tmp_path: Path) -> None:
    fixture = build_high_intensity_scheduler_manual_review_gate_fixture(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "high-intensity-risk-cap-observe-only-scheduler-manual-review-gate",
            "--disabled-wiring-dir",
            str(fixture["disabled_wiring_dir"]),
            "--smoke-dry-run-dir",
            str(fixture["smoke_dry_run_dir"]),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
            "--mode",
            "observe_only_scheduler_manual_review_gate",
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "high_intensity_scheduler_manual_review_gate_summary.json",
        "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.json",
        "high_intensity_scheduler_manual_review_gate_source_artifact_review.json",
        "high_intensity_scheduler_manual_review_gate_promotion_decision.json",
        "high_intensity_2350_manual_run_interface_route.json",
        "high_intensity_scheduler_manual_review_gate_interpretation_boundary.json",
        "high_intensity_scheduler_manual_review_gate_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    summary = read_json(
        output_dir / "high_intensity_scheduler_manual_review_gate_summary.json"
    )
    package = read_json(
        output_dir / "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.json"
    )
    decision = read_json(
        output_dir / "high_intensity_scheduler_manual_review_gate_promotion_decision.json"
    )
    route = read_json(output_dir / "high_intensity_2350_manual_run_interface_route.json")
    assert summary["status"] == STATUS
    assert package["promotion_decision"] == "BLOCKED"
    assert decision["promotion_allowed"] is False
    assert summary["scheduler_enabled"] is False
    assert summary["manual_run_only"] is True
    assert summary["dry_run_only"] is True
    assert summary["manual_review_required"] is True
    assert summary["paper_shadow_enabled"] is False
    assert summary["production_enabled"] is False
    assert summary["broker_action_enabled"] is False
    assert route["next_route"] == NEXT_2350_TASK
    assert (
        docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.md"
    ).exists()
    assert (docs_root / "high_intensity_2350_manual_run_interface_route.md").exists()


def test_registry_catalog_system_flow_and_task_register_reference_manual_review_gate() -> None:
    registry = Path("config/report_registry.yaml").read_text(encoding="utf-8")
    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")

    assert "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate" in registry
    assert (
        "TRADING-2349 High-Intensity Risk-Cap Observe-Only Scheduler Manual Review Gate"
        in catalog
    )
    assert "high-intensity-risk-cap-observe-only-scheduler-manual-review-gate" in system_flow
    assert (
        "TRADING-2349_MANUAL_REVIEW_PROMOTION_GATE_FOR_OBSERVE_ONLY_SCHEDULER"
        in task_register
    )
