from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    audit_cost_liquidity,
    audit_forward_evidence,
    audit_pit_feature_snapshot,
    audit_research_case_library,
    audit_research_execution_cache,
    audit_research_labels,
    audit_research_runs,
    audit_universe,
    build_oracle_diagnostic_set,
    build_pit_feature_snapshot,
    build_tradability_calendar,
    capture_forward_evidence,
    estimate_trading_costs,
    query_pit_feature,
    register_research_run,
    resume_research_execution,
    run_research_execution_batch,
    update_forward_outcomes,
    validate_asset_master,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS


def test_pit_feature_store_snapshot_audit_query_and_hash_stability(tmp_path: Path) -> None:
    output_root = tmp_path / "pit"
    snapshot = build_pit_feature_snapshot(
        as_of_date="2022-12-01",
        decision_time="2022-12-01T21:00:00Z",
        output_root=output_root,
    )
    repeat = build_pit_feature_snapshot(
        as_of_date="2022-12-01",
        decision_time="2022-12-01T21:00:00Z",
        output_root=output_root,
    )
    audit = audit_pit_feature_snapshot(
        snapshot_id=str(snapshot["snapshot_id"]),
        output_root=output_root,
    )
    query = query_pit_feature(
        feature_id="adjusted_close",
        asset_id="ETF_SPY",
        as_of_date="2022-12-01",
        output_root=output_root,
    )

    assert snapshot["summary"]["pit_snapshot_manifest_present"] is True
    assert snapshot["summary"]["feature_available_time_present_rate"] == 1.0
    assert snapshot["lookahead_violation_count"] == 0
    assert snapshot["snapshot_hash"] == repeat["snapshot_hash"]
    assert audit["summary"]["current_view_only_feature_count"] >= 1
    assert audit["summary"]["missing_source_manifest_count"] >= 1
    assert query["summary"]["match_count"] == 1
    assert query["records"][0]["available_time"] == "2022-12-01T21:00:00Z"


def test_asset_cost_label_run_execution_forward_and_case_foundation(
    tmp_path: Path,
) -> None:
    asset_root = tmp_path / "asset"
    cost_root = tmp_path / "cost"
    label_root = tmp_path / "labels"
    run_root = tmp_path / "runs"
    execution_root = tmp_path / "execution"
    forward_root = tmp_path / "forward"
    case_root = tmp_path / "cases"

    asset = validate_asset_master(output_root=asset_root)
    calendar = build_tradability_calendar(output_root=asset_root)
    universe = audit_universe(universe="data_foundation_minimum", output_root=asset_root)
    estimate = estimate_trading_costs(output_root=cost_root)
    cost = audit_cost_liquidity(output_root=cost_root)
    labels = audit_research_labels(output_root=label_root)
    registered_run = register_research_run(output_root=run_root)
    run_audit = audit_research_runs(output_root=run_root)
    execution = run_research_execution_batch(output_root=execution_root)
    checkpoint_id = execution["checkpoints"][0]["checkpoint_id"]
    resumed = resume_research_execution(
        checkpoint_id=checkpoint_id,
        output_root=execution_root,
    )
    cache = audit_research_execution_cache(output_root=execution_root)
    archive = capture_forward_evidence(
        as_of_date="2022-12-01",
        output_root=forward_root,
        feature_snapshot_id="pit_snapshot_20221201_data_foundation_minimum",
    )
    outcome = update_forward_outcomes(
        archive_id=str(archive["archive_id"]),
        output_root=forward_root,
    )
    forward_audit = audit_forward_evidence(output_root=forward_root)
    oracle = build_oracle_diagnostic_set(output_root=case_root)
    case_audit = audit_research_case_library(output_root=case_root)

    assert asset["summary"]["asset_id_stable"] is True
    assert asset["summary"]["ticker_history_present"] is True
    assert calendar["summary"]["tradability_calendar_present"] is True
    assert universe["summary"]["survivorship_bias_warning_available"] is True
    assert estimate["summary"]["net_return_available"] is True
    assert estimate["summary"]["turnover"] == 0
    assert cost["summary"]["cost_model_version_recorded"] is True
    assert cost["summary"]["liquidity_violation_count_reported"] is True
    assert labels["summary"]["labels_as_of_valid"] is True
    assert labels["summary"]["future_event_leakage_count"] == 0
    assert registered_run["summary"]["run_registry_present"] is True
    assert run_audit["summary"]["run_reproducibility_fields_present"] is True
    assert resumed["summary"]["checkpoint_resume_supported"] is True
    assert cache["summary"]["cache_hit_rate_reported"] is True
    assert outcome["summary"]["future_outcomes_appended_only"] is True
    assert forward_audit["summary"]["broker_action"] == "none"
    assert oracle["summary"]["oracle_cases_promotion_gate_allowed"] is False
    assert case_audit["summary"]["oracle_cases_promotion_gate_allowed"] is False
    assert case_audit["summary"]["case_reuse_in_strategy_pair_diagnostics"] is True


def test_data_foundation_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()
    pit_root = tmp_path / "pit"
    asset_root = tmp_path / "asset"
    cost_root = tmp_path / "cost"
    label_root = tmp_path / "labels"
    run_root = tmp_path / "runs"
    execution_root = tmp_path / "execution"
    forward_root = tmp_path / "forward"
    case_root = tmp_path / "cases"
    snapshot_id = "pit_snapshot_20221201_data_foundation_minimum"
    archive_id = "forward_evidence_20221201"

    commands = [
        [
            "data",
            "pit-feature-store",
            "build-snapshot",
            "--as-of-date",
            "2022-12-01",
            "--decision-time",
            "2022-12-01T21:00:00Z",
            "--output-root",
            str(pit_root),
        ],
        [
            "data",
            "pit-feature-store",
            "audit",
            "--snapshot-id",
            snapshot_id,
            "--output-root",
            str(pit_root),
        ],
        [
            "data",
            "pit-feature-store",
            "query",
            "--feature-id",
            "adjusted_close",
            "--asset-id",
            "ETF_SPY",
            "--as-of-date",
            "2022-12-01",
            "--output-root",
            str(pit_root),
        ],
        ["data", "asset-master", "validate", "--output-root", str(asset_root)],
        ["data", "asset-master", "build-tradability-calendar", "--output-root", str(asset_root)],
        [
            "data",
            "universe",
            "show",
            "--universe",
            "data_foundation_minimum",
            "--output-root",
            str(asset_root),
        ],
        [
            "data",
            "universe",
            "audit",
            "--universe",
            "data_foundation_minimum",
            "--output-root",
            str(asset_root),
        ],
        ["trading-costs", "estimate", "--output-root", str(cost_root)],
        ["trading-costs", "audit", "--output-root", str(cost_root)],
        ["research", "labels", "audit", "--output-root", str(label_root)],
        ["research", "runs", "register", "--output-root", str(run_root)],
        [
            "research",
            "runs",
            "query",
            "--research-id",
            "portfolio_decision_problem_v1",
            "--output-root",
            str(run_root),
        ],
        ["research", "runs", "compare", "--output-root", str(run_root)],
        ["research", "runs", "audit", "--output-root", str(run_root)],
        ["research", "execution", "plan", "--output-root", str(execution_root)],
        ["research", "execution", "run-batch", "--output-root", str(execution_root)],
        ["research", "execution", "cache-audit", "--output-root", str(execution_root)],
        ["research", "execution", "cache-prune", "--output-root", str(execution_root)],
        [
            "forward-evidence",
            "capture-daily",
            "--as-of-date",
            "2022-12-01",
            "--feature-snapshot-id",
            snapshot_id,
            "--output-root",
            str(forward_root),
        ],
        [
            "forward-evidence",
            "update-outcomes",
            "--archive-id",
            archive_id,
            "--output-root",
            str(forward_root),
        ],
        ["forward-evidence", "audit", "--output-root", str(forward_root)],
        ["forward-evidence", "report", "--output-root", str(forward_root)],
        ["research", "cases", "register", "--output-root", str(case_root)],
        ["research", "cases", "query", "--output-root", str(case_root)],
        ["research", "cases", "build-from-regret-casebook", "--output-root", str(case_root)],
        ["research", "cases", "build-oracle-diagnostic-set", "--output-root", str(case_root)],
        ["research", "cases", "audit", "--output-root", str(case_root)],
    ]

    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, f"{command}\n{result.output}"

    assert pit_root.joinpath(snapshot_id, "pit_feature_snapshot_manifest.json").exists()
    assert asset_root.joinpath("asset_master_validation.json").exists()
    assert cost_root.joinpath("cost_liquidity_audit.json").exists()
    assert label_root.joinpath("research_label_store_audit.json").exists()
    assert run_root.joinpath("run_registry.jsonl").exists()
    assert execution_root.joinpath("research_execution_cache_audit.json").exists()
    assert forward_root.joinpath("daily_archive", f"{archive_id}.json").exists()
    assert case_root.joinpath("research_case_library_audit.json").exists()


def test_data_foundation_registry_catalog_and_validation_tiers() -> None:
    test_path = "tests/test_data_foundation_roadmap.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    required_report_ids = {
        "pit_feature_store_snapshot",
        "asset_master_tradability",
        "cost_liquidity_audit",
        "research_label_store_audit",
        "research_run_registry_audit",
        "research_execution_cache_audit",
        "forward_evidence_archive",
        "research_case_library_audit",
    }

    assert required_report_ids <= set(report_ids)
    for report_id in required_report_ids:
        entry = report_ids[report_id]
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["artifact_globs"]

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "TRADING-726～733" in catalog
    assert (
        "outputs/data_quality/pit_feature_store/<snapshot_id>/pit_feature_snapshot_manifest.json"
        in catalog
    )
    assert "outputs/research_runs/run_registry.jsonl" in catalog
    assert "outputs/forward_evidence/daily_archive/forward_evidence_<as_of>.json/md" in catalog
