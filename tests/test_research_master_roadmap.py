from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.portfolio_decision import (
    build_action_outcome_dataset,
    build_advanced_policy_register,
    build_cohort_prepare,
    build_strategy_evaluation,
    build_value_surface_report,
    validate_portfolio_decision_contract,
)
from ai_trading_system.research_acceleration import (
    build_control_audit,
    build_dashboard,
    build_hypothesis_compile,
    build_queue,
    build_strategy_pair_diagnosis,
)
from ai_trading_system.research_governance import (
    build_evidence_audit,
    build_promotion_readiness,
    build_protocol_validation,
    build_sample_quality_audit,
    build_state_evaluation,
    build_threshold_dependency_audit,
    ingest_evidence_ledger,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS


def test_research_protocol_registry_and_evidence_policy(tmp_path: Path) -> None:
    protocol = build_protocol_validation()

    assert protocol["status"] == "PASS"
    assert protocol["summary"]["pilot_protocol_count"] >= 3
    assert protocol["summary"]["schema_validation_errors"] == 0
    assert protocol["summary"]["protocol_validation_pass"] is True

    ingest = ingest_evidence_ledger(
        "dynamic_trend_thresholds",
        output_root=tmp_path,
    )
    audit = build_evidence_audit("dynamic_trend_thresholds", output_root=tmp_path)

    assert ingest["summary"]["evidence_record_count"] >= 1
    assert audit["status"] == "PASS"
    assert audit["summary"]["unclassified_evidence_count"] == 0
    assert audit["summary"]["promotion_ineligible_source_violation_count"] == 0
    assert audit["summary"]["missing_provenance_count"] == 0
    assert audit["summary"]["lookahead_violation_count"] == 0


def test_research_state_sample_threshold_and_promotion_are_fail_closed(tmp_path: Path) -> None:
    ingest_evidence_ledger("dynamic_trend_thresholds", output_root=tmp_path)

    state = build_state_evaluation("dynamic_trend_thresholds", output_root=tmp_path)
    sample = build_sample_quality_audit("dynamic_trend_thresholds", output_root=tmp_path)
    threshold = build_threshold_dependency_audit("dynamic_trend_thresholds")
    promotion = build_promotion_readiness("dynamic_trend_thresholds", output_root=tmp_path)

    assert state["summary"]["engineering_pass_implies_research_pass"] is False
    assert state["summary"]["sensitivity_tested_implies_validated_boundary"] is False
    assert set(state["state_vector"]) >= {
        "engineering_readiness",
        "data_readiness",
        "evidence_maturity",
        "promotion_readiness",
    }
    assert sample["summary"]["row_count_only_decision_count"] == 0
    assert sample["summary"]["sample_source_breakdown_complete"] is True
    assert sample["summary"]["horizon_maturity_breakdown_complete"] is True
    assert threshold["summary"]["unregistered_high_impact_threshold_reference_count"] == 0
    assert threshold["summary"]["inline_promotion_threshold_count"] == 0
    assert promotion["summary"]["promotion_boolean_single_source_of_truth"] is True
    assert promotion["summary"]["human_review_required"] is True
    assert promotion["summary"]["automatic_weight_change_count"] == 0


def test_research_acceleration_outputs_keep_oracle_and_controls_non_promotional(
    tmp_path: Path,
) -> None:
    diagnosis = build_strategy_pair_diagnosis(
        "dynamic_trend_thresholds",
        baseline="baseline",
        teacher="hindsight_oracle",
        output_root=tmp_path,
    )
    controls = build_control_audit("dynamic_trend_thresholds", output_root=tmp_path)
    directions = build_hypothesis_compile("dynamic_trend_thresholds")
    queue = build_queue(output_root=tmp_path)
    dashboard = build_dashboard(output_root=tmp_path)

    assert diagnosis["oracle_diagnostic_only"] is True
    assert diagnosis["promotion_gate_allowed"] is False
    assert diagnosis["summary"]["oracle_promotion_violation_count"] == 0
    assert controls["summary"]["future_leakage_control_blocked"] is True
    assert controls["summary"]["random_control_promotion_count"] == 0
    assert controls["summary"]["negative_control_false_positive_count"] == 0
    assert directions["summary"]["direction_review_local_only_count"] == 0
    assert directions["summary"]["null_or_reversal_candidate_present"] is True
    assert directions["summary"]["orthogonal_candidate_present"] is True
    assert directions["summary"]["all_candidates_have_mve"] is True
    assert queue["summary"]["blocked_item_does_not_block_queue"] is True
    assert queue["summary"]["wip_limit_enforced"] is True
    assert dashboard["summary"]["wip_by_lane_visible"] is True
    assert dashboard["summary"]["research_idle_or_stalled_items_visible"] is True


def test_portfolio_decision_contract_strategy_and_sandbox(tmp_path: Path) -> None:
    contract = validate_portfolio_decision_contract()
    dataset = build_action_outcome_dataset(
        "portfolio_decision_problem_v1",
        output_root=tmp_path,
    )
    strategy = build_strategy_evaluation(
        strategy_id="value_surface_baseline",
        stage="stage_1_simple_benchmark",
        output_root=tmp_path,
    )
    value_surface = build_value_surface_report(output_root=tmp_path)
    advanced = build_advanced_policy_register(
        policy_id="tree_candidate",
        method="tree",
        output_root=tmp_path,
    )
    cohort = build_cohort_prepare(
        candidate_id="candidate_requires_review",
        strategy_id="value_surface_baseline",
        output_root=tmp_path,
    )

    assert contract["summary"]["portfolio_decision_contract_valid"] is True
    assert contract["summary"]["action_space_defined"] is True
    assert contract["summary"]["utility_profile_versioned"] is True
    assert dataset["summary"]["pit_valid_rows_only"] is True
    assert dataset["summary"]["future_outcome_marked_evaluation_only"] is True
    assert dataset["summary"]["horizon_maturity_recorded"] is True
    assert dataset["summary"]["overlapping_horizon_warning_present"] is True
    assert strategy["summary"]["all_strategies_use_same_interface"] is True
    assert strategy["summary"]["complex_strategy_cannot_skip_stage"] is True
    assert strategy["summary"]["stage_outputs_registered"] is True
    assert value_surface["summary"]["fixed_window_baseline_comparison_present"] is True
    assert value_surface["summary"]["horizon_leakage_check_pass"] is True
    assert advanced["summary"]["advanced_policy_skip_stage_count"] == 0
    assert advanced["summary"]["promotion_gate_allowed"] is False
    assert cohort["summary"]["paper_shadow_change_allowed"] is False
    assert cohort["summary"]["broker_action"] == "none"
    assert cohort["summary"]["rollback_criteria_present"] is True


def test_research_master_roadmap_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    portfolio_root = tmp_path / "portfolio"
    ops_root = tmp_path / "ops"

    protocol = runner.invoke(
        app,
        [
            "research",
            "governance",
            "protocol-validate",
            "--output-root",
            str(tmp_path / "governance"),
        ],
    )
    evidence = runner.invoke(
        app,
        [
            "research",
            "governance",
            "evidence-audit",
            "--research-id",
            "dynamic_trend_thresholds",
            "--output-root",
            str(tmp_path / "governance"),
        ],
    )
    portfolio = runner.invoke(
        app,
        [
            "research",
            "portfolio-decision",
            "validate-contract",
            "--output-root",
            str(portfolio_root),
        ],
    )
    ops = runner.invoke(
        app,
        ["research", "ops", "queue-build", "--output-root", str(ops_root)],
    )
    dashboard = runner.invoke(
        app,
        ["research", "ops", "dashboard", "--output-root", str(ops_root)],
    )

    assert protocol.exit_code == 0, protocol.output
    assert evidence.exit_code == 0, evidence.output
    assert portfolio.exit_code == 0, portfolio.output
    assert ops.exit_code == 0, ops.output
    assert dashboard.exit_code == 0, dashboard.output
    assert (tmp_path / "governance" / "governance" / "research_protocol_validation.json").exists()
    assert portfolio_root.joinpath("portfolio_decision_contract_validation.json").exists()
    assert ops_root.joinpath("queue_build.json").exists()
    assert ops_root.joinpath("research_ops_dashboard.json").exists()


def test_research_master_roadmap_registry_catalog_and_validation_tiers() -> None:
    test_path = "tests/test_research_master_roadmap.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    required_report_ids = {
        "research_protocol_validation",
        "research_evidence_audit",
        "research_promotion_readiness",
        "research_governance_rollup",
        "research_watchlist",
        "portfolio_decision_contract_validation",
        "pit_action_outcome_dataset",
        "portfolio_value_surface_report",
        "research_strategy_evaluation",
        "research_ops_dashboard",
        "paper_shadow_cohort_prepare",
    }

    assert required_report_ids <= set(report_ids)
    for report_id in required_report_ids:
        entry = report_ids[report_id]
        assert entry["group"] == "research"
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "TRADING-703～725" in catalog
    assert "outputs/research_governance/governance/research_protocol_validation.json/md" in catalog
    assert "outputs/portfolio_decision/portfolio_decision_contract_validation.json/md" in catalog
    assert "outputs/research_ops/research_ops_dashboard.json/md" in catalog
