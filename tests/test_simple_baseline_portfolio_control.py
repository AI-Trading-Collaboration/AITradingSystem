from __future__ import annotations

import csv
import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import render_reader_brief_html
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.simple_baseline_candidate_validation import (
    run_dynamic_vs_static_edge_significance_review,
    run_equal_risk_qqq_sgov_deep_dive,
    run_simple_baseline_drawdown_episode_review,
    run_simple_baseline_period_split_validation,
    run_simple_baseline_watchlist_owner_decision,
    run_tqqq_heavy_pause_rationale_report,
)
from ai_trading_system.simple_baseline_data_repair import (
    run_data_repair_owner_decision_pack,
    run_data_repair_reproducibility_proof,
    run_equal_risk_result_recompute_after_data_repair,
    run_first_forward_aging_observation_dry_run,
    run_forward_aging_unblock_readiness_review,
    run_market_data_repair_manifest_audit,
    run_marketstack_ssl_failure_triage,
    run_reader_brief_forward_aging_safe_preview,
    run_sgov_total_return_data_contract,
    run_sgov_total_return_proxy_quality_review,
    run_simple_baseline_data_source_inventory,
    run_simple_baseline_post_data_repair_real_run,
    run_simple_baseline_validate_data_hardening,
    run_tqqq_cache_rebuild_validation,
    run_tqqq_challenger_revalidation_after_cache_fix,
)
from ai_trading_system.simple_baseline_forward_aging import (
    run_daily_reader_forward_aging_summary,
    run_equal_risk_qqq_sgov_policy_definition_lock,
    run_first_forward_aging_observation_write,
    run_forward_aging_idempotency_and_duplicate_guard,
    run_forward_aging_owner_launch_pack,
    run_forward_aging_scheduler_dry_run,
    run_paper_shadow_blocker_status_report,
    run_simple_baseline_absolute_return_gap_review,
    run_simple_baseline_candidate_role_assignment,
    run_simple_baseline_comparator_definition_lock,
    run_simple_baseline_forward_aging_automation_readiness,
    run_simple_baseline_forward_aging_candidate_freeze,
    run_simple_baseline_forward_aging_contract,
    run_simple_baseline_forward_aging_data_quality_gate,
    run_simple_baseline_forward_aging_master_review,
    run_simple_baseline_forward_aging_owner_review_pack,
    run_simple_baseline_forward_aging_scoreboard,
    run_simple_baseline_forward_aging_update_maturity,
    run_simple_baseline_forward_aging_write_observation,
    run_simple_baseline_paper_shadow_threshold_contract,
    run_simple_baseline_real_result_reconciliation,
    run_simple_baseline_risk_budget_review,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    run_options_next_stage_gate,
    run_qqq_sgov_baseline_backtest,
    run_simple_baseline_cost_sensitivity,
    run_simple_baseline_daily_reader_safety_summary,
    run_simple_baseline_dominance_ranking,
    run_simple_baseline_forward_aging_tracker,
    run_simple_baseline_master_review,
    run_simple_baseline_paper_shadow_readiness,
    run_simple_baseline_pit_boundary_audit,
    run_simple_baseline_portfolio_dry_run_mapper,
    run_simple_baseline_regime_review,
    run_simple_baseline_registry_review,
    run_tqqq_sgov_risk_controlled_baseline,
    run_trend_vol_allocation_policy_search,
)

TEST_AS_OF = date(2024, 7, 10)
SIMPLE_BASELINE_REPORT_IDS = {
    "simple_baseline_strategy_registry_review",
    "qqq_sgov_baseline_backtest",
    "tqqq_sgov_risk_controlled_baseline",
    "trend_vol_allocation_policy_search",
    "simple_baseline_dominance_ranking",
    "simple_baseline_pit_boundary_audit",
    "simple_baseline_cost_sensitivity",
    "simple_baseline_regime_review",
    "simple_baseline_forward_aging_tracker",
    "simple_baseline_paper_shadow_readiness",
    "daily_reader_portfolio_control_safety_summary",
    "simple_baseline_portfolio_dry_run_mapper",
    "simple_baseline_master_review",
    "options_next_stage_gate",
    "equal_risk_qqq_sgov_deep_dive",
    "simple_baseline_period_split_validation",
    "simple_baseline_drawdown_episode_review",
    "dynamic_vs_static_edge_significance_review",
    "tqqq_heavy_pause_rationale_report",
    "simple_baseline_watchlist_owner_decision",
}
FORWARD_AGING_REPORT_IDS = {
    "simple_baseline_real_result_reconciliation",
    "simple_baseline_forward_aging_candidate_freeze",
    "simple_baseline_forward_aging_contract",
    "simple_baseline_forward_aging_write_observation",
    "simple_baseline_forward_aging_update_maturity",
    "simple_baseline_forward_aging_scoreboard",
    "equal_risk_qqq_sgov_policy_definition_lock",
    "simple_baseline_comparator_definition_lock",
    "simple_baseline_forward_aging_data_quality_gate",
    "simple_baseline_paper_shadow_threshold_contract",
    "daily_reader_forward_aging_summary",
    "simple_baseline_risk_budget_review",
    "simple_baseline_absolute_return_gap_review",
    "simple_baseline_candidate_role_assignment",
    "simple_baseline_forward_aging_owner_review_pack",
    "simple_baseline_forward_aging_automation_readiness",
    "simple_baseline_forward_aging_master_review",
    "first_forward_aging_observation_write",
    "forward_aging_idempotency_and_duplicate_guard",
    "forward_aging_scheduler_dry_run",
    "paper_shadow_blocker_status_report",
    "forward_aging_owner_launch_pack",
}
DATA_REPAIR_REPORT_IDS = {
    "simple_baseline_data_source_inventory",
    "tqqq_cache_rebuild_validation",
    "sgov_total_return_data_contract",
    "market_data_repair_manifest_audit",
    "simple_baseline_validate_data_hardening",
    "simple_baseline_post_data_repair_real_run",
    "equal_risk_result_recompute_after_data_repair",
    "tqqq_challenger_revalidation_after_cache_fix",
    "forward_aging_unblock_readiness_review",
    "first_forward_aging_observation_dry_run",
    "reader_brief_forward_aging_safe_preview",
    "data_repair_owner_decision_pack",
    "data_repair_reproducibility_proof",
    "marketstack_ssl_failure_triage",
    "sgov_total_return_proxy_quality_review",
}


def test_simple_baseline_research_functions_write_auditable_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    docs_path = tmp_path / "docs" / "research" / "simple_baseline_master_review.md"

    registry = run_simple_baseline_registry_review(output_root=output_root)
    qqq = run_qqq_sgov_baseline_backtest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    tqqq = run_tqqq_sgov_risk_controlled_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    policy = run_trend_vol_allocation_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    ranking = run_simple_baseline_dominance_ranking(output_root=output_root)
    pit = run_simple_baseline_pit_boundary_audit(output_root=output_root)
    cost = run_simple_baseline_cost_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    regime = run_simple_baseline_regime_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    forward = run_simple_baseline_forward_aging_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    readiness = run_simple_baseline_paper_shadow_readiness(output_root=output_root)
    daily = run_simple_baseline_daily_reader_safety_summary(output_root=output_root)
    mapper = run_simple_baseline_portfolio_dry_run_mapper(output_root=output_root)
    master = run_simple_baseline_master_review(output_root=output_root, master_doc_path=docs_path)
    options = run_options_next_stage_gate(output_root=output_root)
    _write_minimal_real_run_support(output_root)
    deep_dive = run_equal_risk_qqq_sgov_deep_dive(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    period = run_simple_baseline_period_split_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    episode = run_simple_baseline_drawdown_episode_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    edge = run_dynamic_vs_static_edge_significance_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    pause = run_tqqq_heavy_pause_rationale_report(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    owner_decision_path = (
        tmp_path / "docs" / "research" / "simple_baseline_watchlist_owner_decision.md"
    )
    owner_decision = run_simple_baseline_watchlist_owner_decision(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_path=owner_decision_path,
        as_of_date=TEST_AS_OF,
    )

    payloads = [
        registry,
        qqq,
        tqqq,
        policy,
        ranking,
        pit,
        cost,
        regime,
        forward,
        readiness,
        daily,
        mapper,
        master,
        options,
        deep_dive,
        period,
        episode,
        edge,
        pause,
        owner_decision,
    ]
    assert {payload["report_type"] for payload in payloads} == SIMPLE_BASELINE_REPORT_IDS
    assert registry["status"] == "BASELINE_REGISTRY_READY"
    assert qqq["data_quality"]["passed"] is True
    assert qqq["data_quality"]["price_row_count"] > 0
    assert qqq["data_quality"]["price_checksum"]
    assert qqq["requested_date_range"].startswith("2022-12-01")
    assert tqqq["status"] in {
        "TQQQ_BASELINE_RESEARCH_READY",
        "TQQQ_BASELINE_TOO_RISKY",
    }
    assert policy["allowed_inputs"]
    assert ranking["recommended_research_candidates"]
    assert pit["status"] == "PIT_BOUNDARY_PASS"
    assert cost["data_quality"]["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert regime["return_by_regime"]
    assert forward["summary"]["matured_20d_count"] >= 20
    assert readiness["promotion_allowed"] is False
    assert readiness["paper_shadow_allowed"] is False
    assert daily["portfolio_control_research_status"]["broker_action"] == "none"
    assert mapper["broker_read_performed"] is False
    assert master["master_review_doc_path"] == str(docs_path)
    assert docs_path.exists()
    assert options["options_research_allowed"] is False
    assert deep_dive["status"] == "EQUAL_RISK_DEEP_DIVE_READY"
    assert deep_dive["output_metrics"]["strategy_id"] == "equal_risk_qqq_sgov"
    assert period["period_results"]
    assert any(
        row["coverage_status"] == "INSUFFICIENT_PRICE_COVERAGE" for row in period["period_results"]
    )
    assert episode["episode_results"]
    assert edge["status"] in {
        "DYNAMIC_EDGE_REVIEWABLE_LATER",
        "DYNAMIC_EDGE_NOT_MATERIAL",
        "DYNAMIC_EDGE_REGIME_CONCENTRATED",
    }
    assert pause["status"] == "TQQQ_HEAVY_PAUSE_CONFIRMED"
    assert owner_decision["status"] == "OWNER_DECISION_READY"
    assert owner_decision_path.exists()
    assert (
        owner_decision["final_required_answers"]["1_equal_risk_primary_forward_aging_candidate"]
        is True
    )

    for payload in payloads:
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_simple_baseline_cli_smoke_and_report_registry(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    manifest_path = _write_simple_baseline_manifest(tmp_path, prices_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    docs_path = tmp_path / "docs" / "research" / "simple_baseline_master_review.md"
    runner = CliRunner()

    data_args = [
        "--prices-path",
        str(prices_path),
        "--marketstack-prices-path",
        str(marketstack_path),
        "--rates-path",
        str(rates_path),
        "--as-of",
        TEST_AS_OF.isoformat(),
        "--output-root",
        str(output_root),
    ]
    manifest_args = ["--manifest-path", str(manifest_path)]
    commands = [
        [
            "research",
            "strategies",
            "simple-baseline-registry-review",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "qqq-sgov-baseline-backtest", *data_args],
        ["research", "strategies", "tqqq-sgov-risk-controlled-baseline", *data_args],
        ["research", "strategies", "trend-vol-allocation-policy-search", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-dominance-ranking",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-pit-boundary-audit",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "simple-baseline-cost-sensitivity", *data_args],
        ["research", "strategies", "simple-baseline-regime-review", *data_args],
        ["research", "strategies", "simple-baseline-forward-aging-tracker", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-paper-shadow-readiness",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "daily-reader-portfolio-control-safety-summary",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-portfolio-dry-run-mapper",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-master-review",
            "--output-root",
            str(output_root),
            "--docs-path",
            str(docs_path),
        ],
        ["research", "strategies", "options-next-stage-gate", "--output-root", str(output_root)],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
    _write_minimal_real_run_support(output_root)
    new_commands = [
        ["research", "strategies", "equal-risk-qqq-sgov-deep-dive", *data_args],
        ["research", "strategies", "simple-baseline-period-split-validation", *data_args],
        ["research", "strategies", "simple-baseline-drawdown-episode-review", *data_args],
        ["research", "strategies", "dynamic-vs-static-edge-significance-review", *data_args],
        ["research", "strategies", "tqqq-heavy-pause-rationale-report", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-watchlist-owner-decision",
            *data_args,
            "--docs-path",
            str(tmp_path / "docs" / "research" / "simple_baseline_watchlist_owner_decision.md"),
        ],
    ]
    for command in new_commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
    forward_commands = [
        [
            "research",
            "strategies",
            "simple-baseline-real-result-reconciliation",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-candidate-freeze",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-contract",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "equal-risk-qqq-sgov-policy-definition-lock",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-comparator-definition-lock",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-candidate-role-assignment",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "simple-baseline-forward-aging-data-quality-gate", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-write-observation",
            *data_args,
            "--decision-date",
            "2023-10-10",
        ],
        ["research", "strategies", "simple-baseline-forward-aging-update-maturity", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-scoreboard",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-paper-shadow-threshold-contract",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "daily-reader-forward-aging-summary",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "simple-baseline-risk-budget-review", *data_args],
        ["research", "strategies", "simple-baseline-absolute-return-gap-review", *data_args],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-owner-review-pack",
            "--output-root",
            str(output_root),
            "--docs-path",
            str(tmp_path / "docs" / "research" / "simple_baseline_forward_owner_pack.md"),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-automation-readiness",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-forward-aging-master-review",
            "--output-root",
            str(output_root),
            "--docs-path",
            str(tmp_path / "docs" / "research" / "simple_baseline_forward_master.md"),
        ],
    ]
    for command in forward_commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
    _write_json(
        output_root / "data_repair_owner_decision_pack.json",
        {"status": "OWNER_APPROVE_FORWARD_AGING"},
    )
    forward_launch_commands = [
        [
            "research",
            "strategies",
            "first-forward-aging-observation-write",
            *data_args,
            "--decision-date",
            "2023-10-10",
        ],
        [
            "research",
            "strategies",
            "forward-aging-idempotency-and-duplicate-guard",
            *data_args,
            "--decision-date",
            "2023-10-10",
        ],
        [
            "research",
            "strategies",
            "forward-aging-scheduler-dry-run",
            *data_args,
            "--decision-date",
            "2023-10-10",
        ],
        [
            "research",
            "strategies",
            "paper-shadow-blocker-status-report",
            "--output-root",
            str(output_root),
        ],
    ]
    for command in forward_launch_commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
    repair_commands = [
        [
            "research",
            "strategies",
            "simple-baseline-data-source-inventory",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            *manifest_args,
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "tqqq-cache-rebuild-and-validation",
            *data_args,
            *manifest_args,
            "--no-execute-repair",
        ],
        [
            "research",
            "strategies",
            "sgov-total-return-data-contract",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            *manifest_args,
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "market-data-repair-manifest-audit",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            *manifest_args,
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "simple-baseline-validate-data-hardening",
            *data_args,
            *manifest_args,
        ],
        [
            "research",
            "strategies",
            "data-repair-reproducibility-proof",
            *data_args,
            *manifest_args,
            "--expected-tqqq-rows",
            "420",
        ],
        [
            "research",
            "strategies",
            "marketstack-ssl-failure-triage",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            *manifest_args,
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "sgov-total-return-proxy-quality-review",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            *manifest_args,
            "--output-root",
            str(output_root),
        ],
    ]
    for command in repair_commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output
    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "forward-aging-owner-launch-pack",
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    registered = {item["report_id"] for item in registry["reports"]}
    expected_registry_ids = (
        SIMPLE_BASELINE_REPORT_IDS | FORWARD_AGING_REPORT_IDS | DATA_REPAIR_REPORT_IDS
    )
    assert expected_registry_ids <= registered
    daily_entry = next(
        item
        for item in registry["reports"]
        if item["report_id"] == "daily_reader_portfolio_control_safety_summary"
    )
    assert daily_entry["include_in_reader_brief"] is True
    assert daily_entry["required_for_daily_reading"] is False


def test_forward_aging_writer_replaces_failed_placeholder_after_data_recovery(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    observation_root = output_root / "forward_aging_observations"
    failed_path = observation_root / "simple_baseline_forward_aging_observation_2023-10-10.json"
    _write_json(
        failed_path,
        {
            "report_type": "simple_baseline_forward_aging_write_observation",
            "status": "MARKET_DATA_MISSING",
            "summary": {
                "decision_date": "2023-10-10",
                "data_quality_status": "FAIL",
                "observation_count": 0,
            },
            "data_quality": {"status": "FAIL"},
            "observations": [],
        },
    )
    _write_json(
        output_root / "data_repair_owner_decision_pack.json",
        {"status": "OWNER_APPROVE_FORWARD_AGING"},
    )

    first_write = run_first_forward_aging_observation_write(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    written = json.loads(failed_path.read_text(encoding="utf-8"))

    assert first_write["status"] == "FIRST_OBSERVATION_WRITTEN"
    assert first_write["summary"]["observation_written"] is True
    assert written["status"] == "OBSERVATION_WRITTEN"
    assert written["summary"]["replaced_invalid_existing_artifact"] is True
    assert written["previous_invalid_artifact"]["previous_status"] == "MARKET_DATA_MISSING"
    assert written["observations"]


def test_simple_baseline_forward_aging_convergence_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    manifest_path = _write_simple_baseline_manifest(tmp_path, prices_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    _write_simple_baseline_prerequisites(
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_root=tmp_path / "docs" / "research",
    )

    reconciliation = run_simple_baseline_real_result_reconciliation(output_root=output_root)
    freeze = run_simple_baseline_forward_aging_candidate_freeze(output_root=output_root)
    contract = run_simple_baseline_forward_aging_contract(output_root=output_root)
    policy_lock = run_equal_risk_qqq_sgov_policy_definition_lock(output_root=output_root)
    comparator_lock = run_simple_baseline_comparator_definition_lock(output_root=output_root)
    role_assignment = run_simple_baseline_candidate_role_assignment(output_root=output_root)
    data_quality = run_simple_baseline_forward_aging_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    _write_json(
        output_root / "data_repair_owner_decision_pack.json",
        {"status": "OWNER_APPROVE_FORWARD_AGING"},
    )
    first_write = run_first_forward_aging_observation_write(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    duplicate_observation = run_simple_baseline_forward_aging_write_observation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    idempotency = run_forward_aging_idempotency_and_duplicate_guard(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    maturity = run_simple_baseline_forward_aging_update_maturity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    scoreboard = run_simple_baseline_forward_aging_scoreboard(output_root=output_root)
    threshold = run_simple_baseline_paper_shadow_threshold_contract(output_root=output_root)
    daily_forward = run_daily_reader_forward_aging_summary(output_root=output_root)
    scheduler = run_forward_aging_scheduler_dry_run(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    risk_budget = run_simple_baseline_risk_budget_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    return_gap = run_simple_baseline_absolute_return_gap_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    owner_pack_path = tmp_path / "docs" / "research" / "forward_owner.md"
    owner_pack = run_simple_baseline_forward_aging_owner_review_pack(
        output_root=output_root,
        docs_path=owner_pack_path,
    )
    automation = run_simple_baseline_forward_aging_automation_readiness(output_root=output_root)
    master_path = tmp_path / "docs" / "research" / "forward_master.md"
    master = run_simple_baseline_forward_aging_master_review(
        output_root=output_root,
        docs_path=master_path,
    )
    marketstack = run_marketstack_ssl_failure_triage(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    sgov_proxy = run_sgov_total_return_proxy_quality_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    blocker_report = run_paper_shadow_blocker_status_report(output_root=output_root)
    launch_pack = run_forward_aging_owner_launch_pack(output_root=output_root)

    assert reconciliation["status"] == "RECONCILED"
    assert freeze["status"] == "CANDIDATES_FROZEN"
    assert contract["status"] == "FORWARD_AGING_CONTRACT_READY"
    assert policy_lock["status"] == "POLICY_DEFINITION_LOCKED"
    assert policy_lock["policy_definition_hash"]
    assert comparator_lock["status"] == "COMPARATOR_DEFINITIONS_LOCKED"
    assert role_assignment["status"] == "ROLE_ASSIGNMENT_READY"
    assert data_quality["status"] in {"DATA_QUALITY_PASS", "DATA_QUALITY_PASS_WITH_WARNINGS"}
    assert first_write["status"] == "FIRST_OBSERVATION_WRITTEN"
    assert first_write["summary"]["observation_written"] is True
    assert duplicate_observation["status"] == "OBSERVATION_ALREADY_EXISTS"
    assert idempotency["status"] == "FORWARD_IDEMPOTENCY_GUARD_PASS"
    assert first_write["observations"][0]["strategy_id"] == "equal_risk_qqq_sgov"
    assert maturity["status"] in {"MATURITY_UPDATED", "MATURITY_PARTIAL"}
    assert scoreboard["status"] == "FORWARD_SCOREBOARD_INSUFFICIENT"
    primary_score = next(
        row for row in scoreboard["scoreboard"] if row["strategy_id"] == "equal_risk_qqq_sgov"
    )
    assert primary_score["matured_120d_count"] == 1
    assert threshold["status"] == "THRESHOLD_CONTRACT_READY"
    assert threshold["paper_shadow_allowed"] is False
    assert daily_forward["status"] == "DAILY_FORWARD_SUMMARY_SAFE"
    assert daily_forward["portfolio_control_forward_aging"]["data_quality_status"] == "PASS"
    assert scheduler["status"] == "FORWARD_AGING_SCHEDULER_OBSERVATION_ALREADY_EXISTS"
    assert risk_budget["status"] in {"RISK_BUDGET_REVIEW_READY", "RISK_BUDGET_REVIEW_MIXED"}
    assert return_gap["role_recommendation"] in {
        "DEFENSIVE_CORE",
        "BALANCED_CORE",
        "GROWTH_INSUFFICIENT",
    }
    assert owner_pack["status"] == "OWNER_REVIEW_READY"
    assert owner_pack_path.exists()
    assert automation["status"] == "AUTOMATION_READY_FOR_OBSERVATION_ONLY"
    assert master["status"] == "START_FORWARD_AGING"
    assert master_path.exists()
    assert marketstack["status"] == "MARKETSTACK_FAIL_CLOSED_ACCEPTED"
    assert sgov_proxy["status"] == "SGOV_PROXY_ACCEPTABLE"
    assert blocker_report["summary"]["paper_shadow_allowed"] is False
    assert blocker_report["summary"]["minimum_120d_matured_observations_remaining"] >= 19
    assert launch_pack["status"] == "FORWARD_AGING_OWNER_LAUNCH_PACK_READY"
    assert launch_pack["required_answers"]["7_paper_shadow_still_blocked"] is True

    payloads = [
        reconciliation,
        freeze,
        contract,
        policy_lock,
        comparator_lock,
        role_assignment,
        data_quality,
        first_write,
        duplicate_observation,
        idempotency,
        maturity,
        scoreboard,
        threshold,
        daily_forward,
        scheduler,
        risk_budget,
        return_gap,
        owner_pack,
        automation,
        master,
        blocker_report,
        launch_pack,
    ]
    assert {payload["report_type"] for payload in payloads} == FORWARD_AGING_REPORT_IDS
    for payload in payloads:
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_simple_baseline_data_repair_forward_aging_unblock_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    manifest_path = _write_simple_baseline_manifest(tmp_path, prices_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    _write_simple_baseline_prerequisites(
        prices_path=prices_path,
        marketstack_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_root=tmp_path / "docs" / "research",
    )

    inventory = run_simple_baseline_data_source_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    tqqq_rebuild = run_tqqq_cache_rebuild_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        execute_repair=False,
    )
    sgov_contract = run_sgov_total_return_data_contract(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    manifest_audit = run_market_data_repair_manifest_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    hardening = run_simple_baseline_validate_data_hardening(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    post_run = run_simple_baseline_post_data_repair_real_run(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        docs_root=tmp_path / "docs" / "research",
        as_of_date=TEST_AS_OF,
    )
    equal_recompute = run_equal_risk_result_recompute_after_data_repair(
        prices_path=prices_path,
        output_root=output_root,
    )
    challenger = run_tqqq_challenger_revalidation_after_cache_fix(
        prices_path=prices_path,
        output_root=output_root,
    )
    readiness = run_forward_aging_unblock_readiness_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    dry_run = run_first_forward_aging_observation_dry_run(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        decision_date=date(2023, 10, 10),
    )
    preview = run_reader_brief_forward_aging_safe_preview(output_root=output_root)
    owner_pack = run_data_repair_owner_decision_pack(output_root=output_root)
    proof_source = dict(tqqq_rebuild)
    proof_source["summary"] = {
        **proof_source["summary"],
        "tqqq_rows_before": 0,
        "tqqq_rows_after": 420,
    }
    _write_json(output_root / "tqqq_cache_rebuild_validation.json", proof_source)
    reproducibility = run_data_repair_reproducibility_proof(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
        expected_tqqq_rows=420,
    )
    marketstack = run_marketstack_ssl_failure_triage(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    sgov_proxy = run_sgov_total_return_proxy_quality_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )

    payloads = [
        inventory,
        tqqq_rebuild,
        sgov_contract,
        manifest_audit,
        hardening,
        post_run,
        equal_recompute,
        challenger,
        readiness,
        dry_run,
        preview,
        owner_pack,
        reproducibility,
        marketstack,
        sgov_proxy,
    ]
    assert {payload["report_type"] for payload in payloads} == DATA_REPAIR_REPORT_IDS
    assert inventory["status"] == "DATA_SOURCE_INVENTORY_READY"
    assert tqqq_rebuild["status"] == "TQQQ_CACHE_REBUILT"
    assert tqqq_rebuild["repair_summary"]["repair_executed"] is False
    assert tqqq_rebuild["repair_summary"]["used_fixture"] is False
    assert tqqq_rebuild["repair_summary"]["new_unconfigured_source_used"] is False
    assert sgov_contract["status"] == "SGOV_TOTAL_RETURN_CONTRACT_READY"
    assert manifest_audit["status"] == "REPAIR_MANIFEST_PASS"
    assert hardening["status"] == "VALIDATE_DATA_HARDENED"
    assert hardening["summary"]["qqq_sgov_strategy_status"] == "READY"
    assert hardening["summary"]["tqqq_challenger_status"] == "READY"
    assert post_run["status"] in {"POST_REPAIR_REAL_RUN_PASS", "POST_REPAIR_REAL_RUN_WARN"}
    assert post_run["summary"]["formal_observation_written"] is False
    assert equal_recompute["status"] in {
        "EQUAL_RISK_RECOMPUTED",
        "CANDIDATE_CHANGED_AFTER_DATA_REPAIR",
    }
    assert challenger["status"] in {
        "TQQQ_CHALLENGER_REVALIDATED",
        "TQQQ_CHALLENGER_STILL_PAUSED",
    }
    assert readiness["status"] in {
        "FORWARD_AGING_READY",
        "FORWARD_AGING_READY_WITH_WARNINGS",
    }
    assert dry_run["summary"]["dry_run_only"] is True
    assert dry_run["summary"]["observation_written"] is False
    assert preview["status"] == "READER_FORWARD_PREVIEW_SAFE"
    assert preview["reader_brief_preview"]["paper_shadow_allowed"] is False
    assert preview["reader_brief_preview"]["production_allowed"] is False
    assert preview["reader_brief_preview"]["broker_action"] == "none"
    assert not preview["forbidden_phrase_hits"]
    assert owner_pack["status"] == "OWNER_APPROVE_FORWARD_AGING"
    assert reproducibility["status"] == "DATA_REPAIR_REPRODUCIBLE"
    assert reproducibility["summary"]["current_tqqq_rows"] == 420
    assert marketstack["status"] == "MARKETSTACK_FAIL_CLOSED_ACCEPTED"
    assert marketstack["failure_record"]["ssl_verification_disabled"] is False
    assert sgov_proxy["status"] == "SGOV_PROXY_ACCEPTABLE"
    assert sgov_proxy["required_answers"]["5_forward_aging_currently_allowed_with_warnings"]

    for payload in payloads:
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_simple_baseline_tqqq_missing_blocks_challenger_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_simple_baseline_caches(tmp_path)
    prices_without_tqqq = _write_symbol_filtered_cache(
        prices_path,
        tmp_path / "prices_daily_no_tqqq.csv",
        excluded_symbol="TQQQ",
    )
    marketstack_qqq_only = _write_symbol_filtered_cache(
        marketstack_path,
        tmp_path / "prices_marketstack_qqq_only.csv",
        included_symbol="QQQ",
    )
    manifest_path = _write_simple_baseline_manifest(
        tmp_path,
        prices_without_tqqq,
        symbols=("QQQ", "SGOV"),
    )
    output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"

    hardening = run_simple_baseline_validate_data_hardening(
        prices_path=prices_without_tqqq,
        marketstack_prices_path=marketstack_qqq_only,
        rates_path=rates_path,
        manifest_path=manifest_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    qqq_sgov = run_qqq_sgov_baseline_backtest(
        prices_path=prices_without_tqqq,
        marketstack_prices_path=marketstack_qqq_only,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    tqqq = run_tqqq_sgov_risk_controlled_baseline(
        prices_path=prices_without_tqqq,
        marketstack_prices_path=marketstack_qqq_only,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )

    assert hardening["status"] == "VALIDATE_DATA_WARN"
    assert hardening["summary"]["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert hardening["summary"]["qqq_sgov_strategy_status"] in {"READY", "WARN"}
    assert hardening["summary"]["tqqq_challenger_status"] == "BLOCKED"
    assert qqq_sgov["status"] == "QQQ_SGOV_BASELINE_READY"
    assert qqq_sgov["data_quality"]["passed"] is True
    assert tqqq["status"] == "TQQQ_BASELINE_BLOCKED"


def test_reader_brief_renders_portfolio_control_research_summary(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_portfolio_control_safety_summary.json"
    )
    _write_json(
        artifact_path,
        {
            "report_type": "daily_reader_portfolio_control_safety_summary",
            "status": "DAILY_SUMMARY_SAFE",
            "production_effect": "none",
            "broker_action": "none",
            "portfolio_control_research_status": {
                "top_simple_baseline_candidate": "qqq_80_sgov_20",
                "current_research_only_target_weights": {"QQQ": 0.8, "SGOV": 0.2},
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "major_blockers": [{"reason": "manual owner review still required"}],
            },
        },
    )
    forward_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_forward_aging_summary.json"
    )
    _write_json(
        forward_path,
        {
            "report_type": "daily_reader_forward_aging_summary",
            "status": "DAILY_FORWARD_SUMMARY_SAFE",
            "production_effect": "none",
            "broker_action": "none",
            "portfolio_control_forward_aging": {
                "primary_candidate": "equal_risk_qqq_sgov",
                "challenger_candidate": "dyn_tqqq_capped_trend",
                "latest_observation_date": "2023-10-10",
                "matured_20d_count": 3,
                "matured_60d_count": 2,
                "matured_120d_count": 1,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
        },
    )
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)

    summary = reader_brief._portfolio_control_research_summary()
    forward_summary = reader_brief._portfolio_control_forward_aging_summary()
    html = render_reader_brief_html(
        {
            "portfolio_control_research": summary,
            "portfolio_control_forward_aging": forward_summary,
        }
    )

    assert summary["status"] == "DAILY_SUMMARY_SAFE"
    assert summary["top_simple_baseline_candidate"] == "qqq_80_sgov_20"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["major_blocker_count"] == 1
    assert "Portfolio Control Research" in html
    assert "qqq_80_sgov_20" in html
    assert forward_summary["primary_candidate"] == "equal_risk_qqq_sgov"
    assert forward_summary["matured_120d_count"] == 1
    assert "Portfolio Control Forward Aging" in html
    assert "dyn_tqqq_capped_trend" in html
    assert "broker_action" in html


def _write_simple_baseline_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    dates = _business_dates(date(2022, 12, 1), 420)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0008 + 0.002 * math.sin(day_index / 17.0)
        if 145 <= day_index <= 165:
            qqq_return -= 0.004
        if 260 <= day_index <= 275:
            qqq_return -= 0.003
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.0002
        levels["SGOV"] *= 1.0 + 0.00015
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            adj_close = close + (0.01 if ticker == "SGOV" else 0.0)
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},{close * 1.002:.4f},"
                f"{close * 0.998:.4f},{close:.4f},{adj_close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0005:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path


def _write_simple_baseline_manifest(
    tmp_path: Path,
    prices_path: Path,
    *,
    symbols: tuple[str, ...] = ("QQQ", "TQQQ", "SGOV"),
) -> Path:
    manifest_path = tmp_path / "download_manifest.csv"
    dates = _cache_dates(prices_path)
    row_counts = _price_row_counts(prices_path)
    with manifest_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "downloaded_at",
                "source_id",
                "provider",
                "endpoint",
                "request_parameters",
                "output_path",
                "row_count",
                "checksum_sha256",
            ],
        )
        writer.writeheader()
        for symbol in symbols:
            params = {
                "symbols": [symbol],
                "start": min(dates).isoformat(),
                "end": max(dates).isoformat(),
                "interval": "1d",
                "repair_mode": "price_only" if symbol == "TQQQ" else "download",
                "symbol_mapping": {
                    symbol: {"source_symbol": symbol, "canonical_symbol": symbol}
                },
            }
            writer.writerow(
                {
                    "downloaded_at": "2024-07-10T00:00:00Z",
                    "source_id": "fmp_eod_daily_prices",
                    "provider": "Financial Modeling Prep",
                    "endpoint": "/stable/historical-price-eod/full",
                    "request_parameters": json.dumps(params, sort_keys=True),
                    "output_path": str(prices_path),
                    "row_count": row_counts.get(symbol, 0),
                    "checksum_sha256": f"fixture-{symbol.lower()}",
                }
            )
    return manifest_path


def _write_symbol_filtered_cache(
    source_path: Path,
    target_path: Path,
    *,
    included_symbol: str | None = None,
    excluded_symbol: str | None = None,
) -> Path:
    lines = source_path.read_text(encoding="utf-8").splitlines(keepends=True)
    filtered = [lines[0]]
    for line in lines[1:]:
        symbol = line.split(",", maxsplit=3)[1]
        if included_symbol and symbol != included_symbol:
            continue
        if excluded_symbol and symbol == excluded_symbol:
            continue
        filtered.append(line)
    target_path.write_text("".join(filtered), encoding="utf-8")
    return target_path


def _cache_dates(path: Path) -> list[date]:
    dates: set[date] = set()
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        dates.add(date.fromisoformat(line.split(",", maxsplit=1)[0]))
    return sorted(dates)


def _price_row_counts(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        symbol = line.split(",", maxsplit=3)[1]
        counts[symbol] = counts.get(symbol, 0) + 1
    return counts


def _business_dates(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_minimal_real_run_support(output_root: Path) -> None:
    _write_json(
        output_root / "simple_baseline_real_run_summary.json",
        {
            "report_type": "simple_baseline_real_run_summary",
            "status": "REAL_RUN_COMPLETED",
            "summary": {"top_recommended_candidate": "equal_risk_qqq_sgov"},
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
    _write_json(
        output_root / "simple_baseline_owner_decision_pack.json",
        {
            "report_type": "simple_baseline_owner_decision_pack",
            "status": "OWNER_DECISION_REQUIRED",
            "summary": {"owner_next_action": "narrow_to_watchlist_without_activation"},
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
    _write_json(
        output_root / "simple_baseline_paper_shadow_watchlist.json",
        {
            "report_type": "simple_baseline_paper_shadow_watchlist",
            "status": "WATCHLIST_CREATED_NO_ACTIVATION",
            "summary": {
                "watchlist_count": 5,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
            "watchlist": [
                {
                    "candidate_strategy_id": strategy_id,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
                for strategy_id in (
                    "equal_risk_qqq_sgov",
                    "dyn_tqqq_capped_trend",
                    "qqq_200dma_risk_off",
                    "dyn_balanced_qqq_tqqq_sgov",
                    "qqq_50_sgov_50",
                )
            ],
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )


def _write_simple_baseline_prerequisites(
    *,
    prices_path: Path,
    marketstack_path: Path,
    rates_path: Path,
    output_root: Path,
    docs_root: Path,
) -> None:
    run_simple_baseline_registry_review(output_root=output_root)
    run_qqq_sgov_baseline_backtest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_tqqq_sgov_risk_controlled_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_trend_vol_allocation_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_dominance_ranking(output_root=output_root)
    run_simple_baseline_pit_boundary_audit(output_root=output_root)
    run_simple_baseline_cost_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_regime_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_forward_aging_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_paper_shadow_readiness(output_root=output_root)
    run_simple_baseline_daily_reader_safety_summary(output_root=output_root)
    run_simple_baseline_portfolio_dry_run_mapper(output_root=output_root)
    run_simple_baseline_master_review(
        output_root=output_root,
        master_doc_path=docs_root / "simple_baseline_master_review.md",
    )
    run_options_next_stage_gate(output_root=output_root)
    _write_minimal_real_run_support(output_root)
    run_equal_risk_qqq_sgov_deep_dive(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_period_split_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_drawdown_episode_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_dynamic_vs_static_edge_significance_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_tqqq_heavy_pause_rationale_report(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    run_simple_baseline_watchlist_owner_decision(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_path=docs_root / "simple_baseline_watchlist_owner_decision.md",
        as_of_date=TEST_AS_OF,
    )
    _write_json(
        output_root / "simple_baseline_owner_decision_pack.json",
        {
            "report_type": "simple_baseline_owner_decision_pack",
            "status": "OWNER_DECISION_REQUIRED",
            "summary": {"owner_next_action": "narrow_to_watchlist_without_activation"},
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
    _write_json(
        output_root / "simple_baseline_paper_shadow_watchlist.json",
        {
            "report_type": "simple_baseline_paper_shadow_watchlist",
            "status": "WATCHLIST_CREATED_NO_ACTIVATION",
            "summary": {
                "watchlist_count": 5,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
            "watchlist": [
                {
                    "candidate_strategy_id": strategy_id,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
                for strategy_id in (
                    "equal_risk_qqq_sgov",
                    "dyn_tqqq_capped_trend",
                    "qqq_200dma_risk_off",
                    "dyn_balanced_qqq_tqqq_sgov",
                    "qqq_50_sgov_50",
                )
            ],
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
