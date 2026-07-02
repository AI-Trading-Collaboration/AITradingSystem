from __future__ import annotations

from pathlib import Path

from dynamic_target_baseline_preparation_fixtures import (
    build_dynamic_target_baseline_preparation_fixture,
)

from ai_trading_system.dynamic_target_baseline_preparation import (
    READINESS_DYNAMIC_ROUTE,
    READINESS_REMEDIATION_ROUTE,
    READINESS_SCHEMA_ADAPTER_ROUTE,
    READINESS_STATIC_ONLY_ROUTE,
    build_dynamic_target_baseline_2329_task_route,
    run_dynamic_target_baseline_preparation,
)


def test_ready_source_allows_2329_dynamic_dry_run(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(tmp_path, source_kind="ready")

    payload = run_dynamic_target_baseline_preparation(
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        candidate_artifact_roots=[fixture["candidate_root"]],
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["next_task"] == READINESS_DYNAMIC_ROUTE
    assert payload["recommended_candidate_count"] == 1
    assert payload["promotion_allowed"] is False


def test_schema_adapter_source_routes_to_adapter(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(
        tmp_path,
        source_kind="schema_adapter",
    )

    payload = run_dynamic_target_baseline_preparation(
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        candidate_artifact_roots=[fixture["candidate_root"]],
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["next_task"] == READINESS_SCHEMA_ADAPTER_ROUTE
    assert payload["simulation_executed"] is False


def test_missing_source_routes_to_remediation(tmp_path: Path) -> None:
    fixture = build_dynamic_target_baseline_preparation_fixture(
        tmp_path,
        source_kind="missing",
    )

    payload = run_dynamic_target_baseline_preparation(
        diagnostics_dir=fixture["diagnostics_dir"],
        static_dry_run_dir=fixture["static_dry_run_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        candidate_artifact_roots=[fixture["candidate_root"]],
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["next_task"] == READINESS_REMEDIATION_ROUTE
    assert payload["dynamic_target_baseline_readiness_status"] == (
        "BLOCKED_BY_MISSING_DYNAMIC_TARGET_SOURCE"
    )


def test_empty_candidate_matrix_can_continue_static_only() -> None:
    route = build_dynamic_target_baseline_2329_task_route(
        readiness={"readiness_status": "DYNAMIC_BASELINE_BLOCKED"},
        candidate_rows=[],
        gap_rows=[],
        inventory_rows=[],
    )

    assert route["next_task"] == READINESS_STATIC_ONLY_ROUTE
