from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.current_subscription_qualification import (
    CONTROLLED_REPRESENTATIVE_UNIVERSE,
    build_strategy_research_readiness_board,
    capture_forward_evidence_daily_dry_run,
    capture_forward_evidence_dry_run_archive,
    classify_forward_evidence_requirement,
    run_asset_master_qualification,
    run_benchmark_controls_real_data_batch,
    run_controlled_benchmark_batch,
    run_controlled_benchmark_execution_expansion,
    run_controlled_research_batch_review,
    run_cost_liquidity_model_qualification,
    run_data_foundation_acceptance_v2,
    run_data_source_usage_guardrails,
    run_data_vendor_decision_gate,
    run_first_current_subscription_source_qualification_batch,
    run_fmp_pit_owner_review,
    run_fmp_price_corporate_action_qualification,
    run_fmp_watchlist_owner_review_closure,
    run_gbdt_action_utility_baseline,
    run_horizon_conditioned_value_surface_prototype,
    run_label_boundary_qualification,
    run_macro_risk_source_qualification,
    run_marketstack_coverage_expansion,
    run_marketstack_data_required_closure,
    run_marketstack_reconciliation_qualification,
    run_pilot_batch_review,
    run_regret_casebook_controlled_pilot,
    run_regret_casebook_failure_taxonomy_pilot,
    run_regret_driven_state_machine_prototype,
    run_reverse_diagnostics_activation_gate,
    run_reverse_diagnostics_controlled_pilot,
    run_sec_fundamental_pit_qualification,
    run_simple_strategy_ensemble_selector_prototype,
    run_strategy_pair_reverse_diagnostics_pilot,
    validate_forward_capture_contract,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS


def test_current_subscription_source_qualification_contracts(tmp_path: Path) -> None:
    coverage_path = _write_coverage(tmp_path)
    requirements_path = _write_requirements(tmp_path)
    updated_matrix_path = _write_json(tmp_path / "updated_matrix.json", {"status": "BASELINE"})
    source_root = tmp_path / "source"

    usage = run_data_source_usage_guardrails(
        subscription_coverage_path=coverage_path,
        source_requirement_matrix_path=requirements_path,
        qualification_matrix_updated_path=updated_matrix_path,
        output_root=source_root,
    )
    _assert_safety(usage)
    assert usage["summary"]["current_view_only_strategy_input_violation_count"] == 0
    assert usage["summary"]["research_label_only_promotion_violation_count"] == 0
    assert usage["summary"]["blocked_until_qualified_promotion_violation_count"] == 0
    assert (source_root / "blocked_source_usage_report.md").exists()

    fmp = run_fmp_price_corporate_action_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    marketstack = run_marketstack_reconciliation_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    asset_master = run_asset_master_qualification(output_root=source_root)
    forward = classify_forward_evidence_requirement(
        source_requirement_matrix_path=requirements_path,
        output_root=source_root,
    )
    forward_validation = validate_forward_capture_contract(
        capture_contract_path=source_root / "forward_evidence_capture_contract.json",
        output_root=source_root,
    )
    sec = run_sec_fundamental_pit_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    macro = run_macro_risk_source_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    labels = run_label_boundary_qualification(output_root=source_root)
    costs = run_cost_liquidity_model_qualification(output_root=source_root)

    assert fmp["summary"]["source_manifest_generated"] is True
    assert fmp["summary"]["available_time_contract_present"] is True
    assert fmp["qualification_status"] == "promotion_candidate_after_qualification"
    assert marketstack["summary"]["second_source_reconciliation_available"] is True
    assert marketstack["summary"]["marketstack_primary_source_allowed"] is False
    assert asset_master["summary"]["asset_id_stable"] is True
    assert asset_master["summary"]["cash_asset_defined"] is True
    assert forward["summary"]["requires_new_paid_source_for_forward_archive"] is False
    assert forward_validation["summary"]["internal_capture_contract_present"] is True
    assert sec["summary"]["accepted_time_contract_present"] is True
    assert sec["summary"]["current_view_fundamental_strategy_input_allowed"] is False
    assert macro["summary"]["cash_yield_proxy_defined"] is True
    assert labels["summary"]["post_hoc_label_strategy_input_violation_count"] == 0
    assert costs["summary"]["turnover_cost_monotonicity_test_pass"] is True

    missing_contract_acceptance = run_data_foundation_acceptance_v2(
        output_root=tmp_path / "acceptance_missing_contract",
        usage_guardrails_path=source_root / "data_source_usage_policy_audit.json",
        fmp_qualification_path=(
            source_root / "fmp_price_corporate_action_qualification_report.json"
        ),
        asset_master_qualification_path=source_root / "asset_master_qualification_report.json",
        cost_qualification_path=source_root / "cost_liquidity_qualification_report.json",
        forward_capture_contract_path=tmp_path / "missing_forward_contract.json",
    )
    assert missing_contract_acceptance["minimum_research_readiness_level"] == (
        "DIAGNOSTIC_ONLY_READY"
    )
    assert missing_contract_acceptance["summary"]["forward_capture_contract_ready"] is False

    acceptance = run_data_foundation_acceptance_v2(
        output_root=tmp_path / "acceptance",
        usage_guardrails_path=source_root / "data_source_usage_policy_audit.json",
        fmp_qualification_path=(
            source_root / "fmp_price_corporate_action_qualification_report.json"
        ),
        asset_master_qualification_path=source_root / "asset_master_qualification_report.json",
        cost_qualification_path=source_root / "cost_liquidity_qualification_report.json",
        forward_capture_contract_path=source_root / "forward_evidence_capture_contract.json",
    )
    assert acceptance["minimum_research_readiness_level"] == "CONTROLLED_RESEARCH_READY"
    assert (tmp_path / "acceptance" / "minimum_research_readiness_report.json").exists()

    research_root = tmp_path / "research"
    board = build_strategy_research_readiness_board(
        acceptance_v2_path=tmp_path / "acceptance" / "data_foundation_acceptance_report_v2.json",
        output_root=research_root,
    )
    benchmark = run_benchmark_controls_real_data_batch(output_root=research_root)
    reverse = run_strategy_pair_reverse_diagnostics_pilot(output_root=research_root)
    regret = run_regret_casebook_failure_taxonomy_pilot(output_root=research_root)
    value_surface = run_horizon_conditioned_value_surface_prototype(output_root=research_root)
    state_machine = run_regret_driven_state_machine_prototype(output_root=research_root)
    ensemble = run_simple_strategy_ensemble_selector_prototype(output_root=research_root)
    gbdt = run_gbdt_action_utility_baseline(output_root=research_root)
    review = run_pilot_batch_review(output_root=research_root)
    vendor = run_data_vendor_decision_gate(
        acceptance_v2_path=tmp_path / "acceptance" / "data_foundation_acceptance_report_v2.json",
        output_root=research_root,
    )

    assert board["summary"]["research_start_decision_explicit"] is True
    assert benchmark["summary"]["future_leakage_trap_blocked"] is True
    assert reverse["summary"]["oracle_promotion_violation_count"] == 0
    assert regret["summary"]["unclassified_regret_case_count"] == 0
    assert value_surface["summary"]["horizon_leakage_check_pass"] is True
    assert state_machine["summary"]["state_transition_explainable"] is True
    assert ensemble["summary"]["selector_overfit_warning_present"] is True
    assert gbdt["summary"]["negative_control_pass"] is True
    assert review["summary"]["all_candidates_have_decision"] is True
    assert vendor["status"] == "DO_NOT_BUY_NEW_SOURCE_YET"
    assert vendor["summary"]["vendor_purchase_not_recommended_without_blocker_mapping"] is True
    for payload in (
        board,
        benchmark,
        reverse,
        regret,
        value_surface,
        state_machine,
        ensemble,
        gbdt,
        review,
        vendor,
    ):
        _assert_safety(payload)

    batch = run_first_current_subscription_source_qualification_batch(
        subscription_coverage_path=coverage_path,
        source_requirement_matrix_path=requirements_path,
        qualification_matrix_updated_path=updated_matrix_path,
        source_output_root=tmp_path / "batch_source",
        acceptance_output_root=tmp_path / "batch_acceptance",
        controlled_output_root=tmp_path / "batch_controlled",
        output_root=tmp_path / "batch_review",
    )
    _assert_safety(batch)
    assert batch["report_type"] == "current_subscription_source_qualification_batch_review"
    assert batch["summary"]["current_view_only_strategy_input_violation_count"] == 0
    assert batch["summary"]["research_label_only_promotion_violation_count"] == 0
    assert batch["summary"]["blocked_until_qualified_promotion_violation_count"] == 0
    assert batch["fmp_price_corporate_action"]["source_manifest_generated"] is True
    assert "delisted_companies" in batch["fmp_price_corporate_action"]["covered_endpoints"]
    assert batch["forward_evidence_reclassification"]["reclassification"] == (
        "internal_capture_requirement"
    )
    assert batch["acceptance_v2"]["promotion_candidate_after_qualification_count"] == 1
    assert batch["marketstack_reconciliation"]["marketstack_second_source_only"] is True
    assert batch["marketstack_reconciliation"]["price_discrepancy_summary"]["status"] == (
        "DATA_REQUIRED"
    )
    decisions = {item["candidate_id"]: item for item in batch["candidate_decisions"]}
    assert decisions["marketstack_reconciliation"]["decision"] == "DATA_REQUIRED"
    assert decisions["benchmark_controls"]["decision"] == "CONTINUE"
    assert decisions["regret_casebook_pilot"]["decision"] == "WATCHLIST"
    assert (
        tmp_path / "batch_review" / "current_subscription_source_qualification_batch_review.json"
    ).exists()


def test_controlled_research_batch_760_to_764_contract(tmp_path: Path) -> None:
    coverage_path = _write_coverage(tmp_path)
    prices_path, marketstack_prices_path, rates_path = _write_controlled_price_caches(tmp_path)
    source_root = tmp_path / "source"
    benchmark_root = tmp_path / "benchmark"
    marketstack_root = tmp_path / "marketstack"
    fmp_root = tmp_path / "fmp"
    reverse_root = tmp_path / "reverse"
    regret_root = tmp_path / "regret"
    review_root = tmp_path / "review"

    run_fmp_price_corporate_action_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    benchmark = run_controlled_benchmark_batch(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=benchmark_root,
        as_of_date=date(2022, 12, 5),
        expected_price_tickers=list(CONTROLLED_REPRESENTATIVE_UNIVERSE),
        expected_rate_series=["DGS10"],
    )
    _assert_safety(benchmark)
    assert benchmark["report_type"] == "controlled_benchmark_batch_report"
    assert benchmark["summary"]["benchmark_run_count"] >= benchmark["summary"]["configured_minimum"]
    assert benchmark["summary"]["negative_control_promotion_count"] == 0
    assert benchmark["summary"]["future_leakage_trap_blocked"] is True
    assert (benchmark_root / "control_audit_report.json").exists()

    forward = capture_forward_evidence_dry_run_archive(
        benchmark_report_path=benchmark_root / "controlled_benchmark_batch_report.json",
        control_audit_path=benchmark_root / "control_audit_report.json",
        output_root=tmp_path / "forward",
    )
    _assert_safety(forward)
    assert forward["summary"]["forward_archive_created"] is True
    assert forward["outcome_status"] == "pending"
    assert forward["outcome_append_only"] is True

    marketstack = run_marketstack_coverage_expansion(
        subscription_coverage_path=coverage_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=marketstack_root,
        as_of_date=date(2022, 12, 5),
        expected_rate_series=["DGS10"],
    )
    _assert_safety(marketstack)
    assert marketstack["summary"]["representative_universe_probe_complete"] is True
    assert marketstack["summary"]["coverage_ratio_explained"] is True
    assert marketstack["summary"]["symbol_mapping_issue_count"] == 1
    assert marketstack["summary"]["marketstack_primary_source_allowed"] is False
    discrepancy = json.loads(
        (marketstack_root / "fmp_marketstack_discrepancy_report.json").read_text(encoding="utf-8")
    )
    assert "SYMBOL_MAPPING_ISSUE" in discrepancy["discrepancy_reason_enum"]

    fmp_review = run_fmp_pit_owner_review(
        fmp_qualification_path=source_root / "fmp_price_corporate_action_qualification_report.json",
        fmp_manifest_path=source_root / "fmp_source_manifest_sample.json",
        output_root=fmp_root,
    )
    _assert_safety(fmp_review)
    assert fmp_review["summary"]["owner_review_package_generated"] is True
    assert fmp_review["summary"]["provider_timestamp_gap_explicit"] is True
    delisted = json.loads(
        (fmp_root / "fmp_delisted_validation_report.json").read_text(encoding="utf-8")
    )
    assert delisted["summary"]["promotion_blocker_remaining"] is True

    reverse = run_reverse_diagnostics_controlled_pilot(
        benchmark_report_path=benchmark_root / "controlled_benchmark_batch_report.json",
        control_audit_path=benchmark_root / "control_audit_report.json",
        output_root=reverse_root,
    )
    _assert_safety(reverse)
    assert reverse["summary"]["oracle_promotion_violation_count"] == 0
    assert reverse["summary"]["decision_delta_trace_complete"] is True

    regret = run_regret_casebook_controlled_pilot(
        reverse_diagnostics_path=reverse_root / "reverse_diagnostics_controlled_pilot.json",
        output_root=regret_root,
    )
    _assert_safety(regret)
    assert regret["summary"]["regret_case_count"] > 0
    assert regret["summary"]["unclassified_regret_case_count"] == 0
    assert regret["summary"]["hypothesis_candidate_count"] > 0

    review = run_controlled_research_batch_review(
        benchmark_report_path=benchmark_root / "controlled_benchmark_batch_report.json",
        control_audit_path=benchmark_root / "control_audit_report.json",
        forward_archive_path=tmp_path / "forward" / "forward_evidence_dry_run_archive.json",
        marketstack_report_path=marketstack_root / "marketstack_coverage_expansion_report.json",
        fmp_owner_review_path=fmp_root / "fmp_pit_owner_review_package.json",
        fmp_delisted_report_path=fmp_root / "fmp_delisted_validation_report.json",
        reverse_diagnostics_path=reverse_root / "reverse_diagnostics_controlled_pilot.json",
        regret_casebook_path=regret_root / "regret_casebook_controlled_pilot.json",
        output_root=review_root,
    )
    _assert_safety(review)
    assert review["summary"]["all_modules_have_decision"] is True
    assert review["summary"]["next_batch_recommendation_present"] is True
    assert review["summary"]["promotion_gate_allowed"] is False
    decisions = {item["module_id"]: item["decision"] for item in review["module_decisions"]}
    assert decisions["benchmark_controls"] == "CONTINUE"
    assert decisions["marketstack_reconciliation"] in {"WATCHLIST", "DATA_REQUIRED"}
    assert decisions["regret_casebook"] == "WATCHLIST"


def test_controlled_research_expansion_765_to_769_contract(tmp_path: Path) -> None:
    coverage_path = _write_coverage(tmp_path)
    prices_path, marketstack_prices_path, rates_path = _write_controlled_price_caches(tmp_path)
    source_root = tmp_path / "source"
    benchmark_root = tmp_path / "benchmark"
    marketstack_root = tmp_path / "marketstack"
    fmp_root = tmp_path / "fmp"
    forward_root = tmp_path / "forward"
    reverse_root = tmp_path / "reverse"
    review_root = tmp_path / "review"

    run_fmp_price_corporate_action_qualification(
        subscription_coverage_path=coverage_path,
        output_root=source_root,
    )
    run_controlled_benchmark_batch(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=benchmark_root,
        as_of_date=date(2022, 12, 5),
        expected_price_tickers=list(CONTROLLED_REPRESENTATIVE_UNIVERSE),
        expected_rate_series=["DGS10"],
    )
    expansion = run_controlled_benchmark_execution_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=benchmark_root,
        as_of_date=date(2022, 12, 5),
        expected_price_tickers=list(CONTROLLED_REPRESENTATIVE_UNIVERSE),
        expected_rate_series=["DGS10"],
    )
    _assert_safety(expansion)
    assert expansion["report_type"] == "controlled_benchmark_execution_expansion_report"
    assert expansion["summary"]["benchmark_run_count"] > 0
    assert expansion["summary"]["control_run_count"] > 0
    assert expansion["summary"]["negative_control_promotion_count"] == 0
    assert expansion["summary"]["future_leakage_trap_blocked"] is True
    assert expansion["summary"]["random_signal_not_promoted"] is True
    assert expansion["summary"]["gross_net_return_present"] is True
    assert expansion["summary"]["turnover_present"] is True
    assert expansion["summary"]["drawdown_present"] is True
    assert expansion["summary"]["cost_aware_metrics_present"] is True
    assert expansion["summary"]["by_asset_breakdown_present"] is True
    assert expansion["summary"]["by_horizon_breakdown_present"] is True
    assert expansion["summary"]["by_regime_breakdown_present"] is True
    assert expansion["benchmark_results"][0]["cost_aware_metrics"]["cost_bps"] >= 0

    marketstack = run_marketstack_coverage_expansion(
        subscription_coverage_path=coverage_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=marketstack_root,
        as_of_date=date(2022, 12, 5),
        expected_rate_series=["DGS10"],
    )
    marketstack_closure = run_marketstack_data_required_closure(
        marketstack_report_path=marketstack_root / "marketstack_coverage_expansion_report.json",
        discrepancy_report_path=marketstack_root / "fmp_marketstack_discrepancy_report.json",
        output_root=marketstack_root,
    )
    _assert_safety(marketstack_closure)
    assert marketstack["summary"]["previous_row_snapshot_coverage_ratio"] == 0.125
    assert marketstack_closure["summary"]["row_snapshot_coverage_0_125_explained"] is True
    assert marketstack_closure["summary"]["marketstack_final_role"] == (
        "LIMITED_SECOND_SOURCE_ONLY"
    )
    assert (
        marketstack_closure["summary"]["main_controlled_research_blocked_by_marketstack"] is False
    )
    assert marketstack_closure["summary"]["marketstack_primary_source_allowed"] is False

    fmp_review = run_fmp_pit_owner_review(
        fmp_qualification_path=source_root / "fmp_price_corporate_action_qualification_report.json",
        fmp_manifest_path=source_root / "fmp_source_manifest_sample.json",
        output_root=fmp_root,
    )
    fmp_closure = run_fmp_watchlist_owner_review_closure(
        fmp_owner_review_path=fmp_root / "fmp_pit_owner_review_package.json",
        fmp_delisted_report_path=fmp_root / "fmp_delisted_validation_report.json",
        fmp_allowed_uses_path=fmp_root / "fmp_allowed_uses_update.json",
        output_root=fmp_root,
    )
    _assert_safety(fmp_review)
    _assert_safety(fmp_closure)
    assert fmp_closure["summary"]["fmp_watchlist_closed_for_controlled_research"] is True
    assert fmp_closure["summary"]["fmp_controlled_research_primary_price_source_allowed"] is True
    assert fmp_closure["summary"]["promotion_blocking_gap_count"] > 0
    assert (
        fmp_closure["summary"]["delisted_companies_supports_tradable_universe_promotion"] is False
    )

    forward = capture_forward_evidence_daily_dry_run(
        as_of_date=date(2022, 12, 5),
        benchmark_report_path=(
            benchmark_root / "controlled_benchmark_execution_expansion_report.json"
        ),
        control_audit_path=benchmark_root / "control_audit_report.json",
        output_root=forward_root,
        ledger_path=forward_root / "forward_evidence_dry_run_ledger.jsonl",
    )
    _assert_safety(forward)
    assert forward["summary"]["daily_archive_created"] is True
    assert forward["summary"]["candidate_placeholder_present"] is True
    assert forward["outcome_status"] == "pending"
    assert forward["outcome_append_only"] is True
    assert (forward_root / "forward_evidence_dry_run_2022-12-05.json").exists()
    assert (forward_root / "forward_evidence_dry_run_ledger.jsonl").exists()

    static_forward = capture_forward_evidence_dry_run_archive(
        benchmark_report_path=benchmark_root / "controlled_benchmark_batch_report.json",
        control_audit_path=benchmark_root / "control_audit_report.json",
        output_root=forward_root,
    )
    assert static_forward["summary"]["forward_archive_created"] is True
    review = run_controlled_research_batch_review(
        benchmark_report_path=benchmark_root / "controlled_benchmark_batch_report.json",
        control_audit_path=benchmark_root / "control_audit_report.json",
        forward_archive_path=forward_root / "forward_evidence_dry_run_archive.json",
        marketstack_report_path=marketstack_root / "marketstack_coverage_expansion_report.json",
        fmp_owner_review_path=fmp_root / "fmp_pit_owner_review_package.json",
        fmp_delisted_report_path=fmp_root / "fmp_delisted_validation_report.json",
        reverse_diagnostics_path=tmp_path / "missing_reverse.json",
        regret_casebook_path=tmp_path / "missing_regret.json",
        output_root=review_root,
    )
    assert review["summary"]["all_modules_have_decision"] is True
    activation = run_reverse_diagnostics_activation_gate(
        benchmark_expansion_path=benchmark_root
        / "controlled_benchmark_execution_expansion_report.json",
        fmp_closure_path=fmp_root / "fmp_watchlist_closure_report.json",
        controlled_review_path=review_root / "controlled_research_batch_review.json",
        output_root=reverse_root,
    )
    _assert_safety(activation)
    assert activation["summary"]["benchmark_control_batch_passed"] is True
    assert activation["summary"]["fmp_controlled_research_source_confirmed"] is True
    assert activation["summary"]["baseline_vs_simple_interpretable_difference_count"] > 0
    assert activation["summary"]["reverse_diagnostics_controlled_activation_allowed"] is True
    assert activation["summary"]["large_scale_reverse_diagnostics_allowed"] is False


def test_current_subscription_source_qualification_cli_smoke(tmp_path: Path) -> None:
    coverage_path = _write_coverage(tmp_path)
    requirements_path = _write_requirements(tmp_path)
    updated_matrix_path = _write_json(tmp_path / "updated_matrix.json", {"status": "BASELINE"})
    source_root = tmp_path / "source_cli"
    research_root = tmp_path / "research_cli"
    runner = CliRunner()

    commands = [
        [
            "data",
            "source-qualification",
            "usage-guardrails",
            "--subscription-coverage",
            str(coverage_path),
            "--source-requirement-matrix",
            str(requirements_path),
            "--qualification-matrix-updated",
            str(updated_matrix_path),
            "--output-root",
            str(source_root),
        ],
        [
            "data",
            "source-qualification",
            "fmp-price-corporate-action",
            "--subscription-coverage",
            str(coverage_path),
            "--output-root",
            str(source_root),
        ],
        [
            "forward-evidence",
            "classify-requirement",
            "--source-requirement-matrix",
            str(requirements_path),
            "--output-root",
            str(source_root),
        ],
        ["research", "labels", "boundary-qualification", "--output-root", str(source_root)],
        ["trading-costs", "qualify-model", "--output-root", str(source_root)],
        ["research", "strategy-pilot", "readiness-board", "--output-root", str(research_root)],
        ["research", "strategy-pilot", "benchmark-controls", "--output-root", str(research_root)],
        [
            "data",
            "source-qualification",
            "vendor-decision-gate",
            "--output-root",
            str(research_root),
        ],
        [
            "data",
            "source-qualification",
            "first-batch",
            "--subscription-coverage",
            str(coverage_path),
            "--source-requirement-matrix",
            str(requirements_path),
            "--qualification-matrix-updated",
            str(updated_matrix_path),
            "--source-output-root",
            str(source_root / "batch_source"),
            "--acceptance-output-root",
            str(tmp_path / "batch_acceptance"),
            "--controlled-output-root",
            str(research_root / "batch_controlled"),
            "--output-root",
            str(tmp_path / "batch_review"),
        ],
    ]

    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
        assert "production_effect=none" in result.output

    assert (source_root / "data_source_usage_policy_audit.json").exists()
    assert (source_root / "forward_evidence_requirement_reclassification.json").exists()
    assert (research_root / "strategy_research_readiness_board.json").exists()
    assert (research_root / "data_vendor_decision_gate.json").exists()
    assert (
        tmp_path / "batch_review" / "current_subscription_source_qualification_batch_review.json"
    ).exists()


def test_current_subscription_source_qualification_registry_catalog_schema_and_tiers() -> None:
    test_path = "tests/test_current_subscription_source_qualification.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    for report_id in {
        "data_source_usage_policy_audit",
        "fmp_price_corporate_action_qualification_report",
        "data_foundation_acceptance_report_v2",
        "strategy_research_readiness_board",
        "benchmark_controls_real_data_batch",
        "data_vendor_decision_gate",
        "current_subscription_source_qualification_batch_review",
        "controlled_benchmark_batch_report",
        "forward_evidence_dry_run_archive",
        "controlled_benchmark_execution_expansion_report",
        "forward_evidence_daily_dry_run_archive",
        "marketstack_coverage_expansion_report",
        "marketstack_data_required_closure_report",
        "fmp_pit_owner_review_package",
        "fmp_watchlist_closure_report",
        "reverse_diagnostics_controlled_pilot",
        "reverse_diagnostics_activation_gate",
        "regret_casebook_controlled_pilot",
        "controlled_research_batch_review",
    }:
        assert report_id in report_ids
        assert report_ids[report_id]["artifact_selection_policy"] == "latest_available"
        assert report_ids[report_id]["required_for_daily_reading"] is False

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "data_source_usage_policy_audit.json/md" in catalog
    assert "strategy_research_readiness_board.json/md" in catalog
    assert "current_subscription_source_qualification_batch_review.json/md" in catalog
    assert "controlled_benchmark_batch_report.json/md" in catalog
    assert "controlled_research_batch_review.json/md" in catalog
    assert "controlled_benchmark_execution_expansion_report.json/md" in catalog
    assert "forward_evidence_dry_run_YYYY-MM-DD.json/md" in catalog
    assert "LIMITED_SECOND_SOURCE_ONLY" in catalog
    assert "DO_NOT_BUY_NEW_SOURCE_YET" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-739～748" in system_flow
    assert "TRADING-759" in system_flow
    assert "TRADING-760～764" in system_flow
    assert "TRADING-765～769" in system_flow
    assert "aits research strategy-pilot readiness-board" in system_flow
    assert "aits research controlled-pilot benchmark-batch" in system_flow
    assert "aits research controlled-pilot benchmark-expansion" in system_flow

    assert (
        PROJECT_ROOT / "docs" / "schema" / "current_subscription_source_qualification.schema.json"
    ).exists()
    assert (
        PROJECT_ROOT / "docs" / "schema" / "controlled_strategy_research_pilot.schema.json"
    ).exists()


def _write_coverage(tmp_path: Path) -> Path:
    rows = [
        _endpoint("Financial Modeling Prep", "historical_eod_price_full"),
        _endpoint("Financial Modeling Prep", "historical_eod_price_light"),
        _endpoint("Financial Modeling Prep", "historical_eod_price_non_split_adjusted"),
        _endpoint("Financial Modeling Prep", "dividends"),
        _endpoint("Financial Modeling Prep", "splits"),
        _endpoint("Financial Modeling Prep", "delisted_companies"),
        _endpoint("Marketstack", "eod_historical_price"),
        _endpoint("Marketstack", "splits"),
        _endpoint("Marketstack", "dividends"),
        _endpoint("SEC EDGAR", "company_submissions", available_time_supported=True),
        _endpoint("SEC EDGAR", "companyfacts"),
        _endpoint("FRED", "series_observations", available_time_supported=True),
        _endpoint("Cboe", "vix_daily_history"),
        _endpoint("Congress.gov", "bill_search", likely_allowed_use="research_label_only"),
    ]
    return _write_json(
        tmp_path / "coverage.json",
        {
            "report_type": "current_subscription_data_coverage_matrix",
            "status": "COVERAGE_AUDIT_RECORDED_NO_SOURCE_UPGRADE",
            "endpoint_coverage_matrix": rows,
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
        },
    )


def _write_requirements(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "requirements.json",
        {
            "report_type": "data_source_requirement_matrix",
            "status": "REQUIREMENTS_READY_WITH_SOURCE_BLOCKERS",
            "source_requirements": [
                {
                    "component": "forward_evidence_archive",
                    "current_status": "BLOCKED_UNTIL_QUALIFIED",
                    "missing_proof": ["forward archive capture contract"],
                    "required_raw_source": [
                        "daily archive",
                        "feature snapshot",
                        "strategy output",
                        "outcome append",
                    ],
                },
                {
                    "component": "regime_event_cluster_labels",
                    "current_status": "RESEARCH_LABEL_ONLY",
                },
                {
                    "component": "fundamentals",
                    "current_status": "CURRENT_VIEW_ONLY",
                },
            ],
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
        },
    )


def _endpoint(
    provider: str,
    endpoint_name: str,
    *,
    likely_allowed_use: str = "promotion_candidate_after_qualification",
    available_time_supported: bool = False,
) -> dict[str, Any]:
    coverage = {"coverage_ratio_observed": 1.0}
    if provider == "Marketstack" and endpoint_name == "eod_historical_price":
        coverage = {
            "coverage_ratio_observed": 0.125,
            "probed": ["SPY"],
            "covered": ["SPY"],
        }
    return {
        "provider": provider,
        "endpoint_name": endpoint_name,
        "accessible": True,
        "endpoint_accessible": True,
        "likely_allowed_use": likely_allowed_use,
        "current_view_only_risk": False,
        "available_time_supported": available_time_supported,
        "coverage_for_representative_universe": coverage,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
    }


def _write_controlled_price_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    dates = ["2022-12-01", "2022-12-02", "2022-12-05"]
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    header = "date,ticker,open,high,low,close,adj_close,volume\n"
    primary_rows = [header]
    secondary_rows = [header]
    for ticker_index, ticker in enumerate(CONTROLLED_REPRESENTATIVE_UNIVERSE):
        base = 100 + ticker_index
        for day_index, row_date in enumerate(dates):
            close = base + day_index
            primary_rows.append(
                f"{row_date},{ticker},{close - 1},{close + 1},{close - 2},"
                f"{close},{close},{1000000 + ticker_index}\n"
            )
            secondary_ticker = "GOOG" if ticker == "GOOGL" else ticker
            if ticker == "GOOGL":
                close = close + 1
            secondary_rows.append(
                f"{row_date},{secondary_ticker},{close - 1},{close + 1},{close - 2},"
                f"{close},{close},{1000000 + ticker_index}\n"
            )
    prices_path.write_text("".join(primary_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text(
        "date,series,value\n"
        "2022-12-01,DGS10,3.50\n"
        "2022-12-02,DGS10,3.51\n"
        "2022-12-05,DGS10,3.52\n",
        encoding="utf-8",
    )
    return prices_path, marketstack_path, rates_path


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _assert_safety(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0
