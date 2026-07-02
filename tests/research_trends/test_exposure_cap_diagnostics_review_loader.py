from __future__ import annotations

from pathlib import Path

import pytest
from exposure_cap_diagnostics_review_fixtures import (
    build_exposure_cap_diagnostics_review_fixture,
    read_json,
    write_json,
)

from ai_trading_system.exposure_cap_diagnostics_review import (
    ExposureCapDiagnosticsReviewError,
    load_exposure_cap_diagnostics_review_inputs,
)


def test_exposure_cap_diagnostics_loader_reads_required_inputs(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)

    payload = load_exposure_cap_diagnostics_review_inputs(
        dry_run_dir=fixture["dry_run_dir"],
        source_binding_dir=fixture["source_binding_dir"],
        baseline_decision_dir=fixture["baseline_decision_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
    )

    assert payload["dry_run"]["summary"]["selected_baseline"] == (
        "static_etf_allocation_baseline"
    )
    assert payload["baseline_decision"]["recommended_baseline"]["selected_for_2326"] == (
        "static_etf_allocation_baseline"
    )
    assert payload["source_binding"]["summary"]["task_id"] == (
        "TRADING-2324_EXPOSURE_CAP_SIMULATION_SOURCE_BINDING"
    )
    assert payload["simulation_policy"]["summary"]["task_id"] == (
        "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION"
    )


def test_exposure_cap_diagnostics_loader_fails_when_comparison_missing(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    (fixture["dry_run_dir"] / "exposure_cap_vs_no_cap_static_etf_comparison.json").unlink()

    with pytest.raises(ExposureCapDiagnosticsReviewError, match="required artifacts"):
        load_exposure_cap_diagnostics_review_inputs(
            dry_run_dir=fixture["dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )

def test_exposure_cap_diagnostics_loader_fails_when_boundary_missing(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    (fixture["dry_run_dir"] / "exposure_cap_simulation_interpretation_boundary.json").unlink()

    with pytest.raises(ExposureCapDiagnosticsReviewError, match="required artifacts"):
        load_exposure_cap_diagnostics_review_inputs(
            dry_run_dir=fixture["dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_exposure_cap_diagnostics_loader_fails_on_unsafe_promotion(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    summary_path = fixture["dry_run_dir"] / "source_bound_static_etf_dry_run_summary.json"
    payload = read_json(summary_path)
    payload["promotion_allowed"] = True
    write_json(summary_path, payload)

    with pytest.raises(ExposureCapDiagnosticsReviewError, match="promotion"):
        load_exposure_cap_diagnostics_review_inputs(
            dry_run_dir=fixture["dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )


def test_exposure_cap_diagnostics_loader_fails_on_broker_action(
    tmp_path: Path,
) -> None:
    fixture = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    safety_path = (
        fixture["baseline_decision_dir"] / "portfolio_baseline_source_safety_boundary.json"
    )
    payload = read_json(safety_path)
    payload["broker_action"] = "buy"
    write_json(safety_path, payload)

    with pytest.raises(ExposureCapDiagnosticsReviewError, match="broker_action"):
        load_exposure_cap_diagnostics_review_inputs(
            dry_run_dir=fixture["dry_run_dir"],
            source_binding_dir=fixture["source_binding_dir"],
            baseline_decision_dir=fixture["baseline_decision_dir"],
            simulation_policy_dir=fixture["simulation_policy_dir"],
        )
