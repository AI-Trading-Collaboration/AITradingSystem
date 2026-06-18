from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands import reports as reports_cli
from ai_trading_system.reports import executable_research_binding as binding_reports
from ai_trading_system.reports import executable_research_evidence_repair as repair
from ai_trading_system.reports import next_research_cycle as next_cycle

RUN_DATE = date(2026, 6, 17)


def test_evidence_gap_ledger_builds_non_aggregated_gap_rows(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)

    payload = repair.build_executable_research_evidence_gap_ledger_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == "EXECUTABLE_RESEARCH_EVIDENCE_GAP_LEDGER_READY"
    assert payload["summary"]["source_cycle_snapshot_status"] == (
        "EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    )
    assert payload["summary"]["production_effect"] == "none"
    assert payload["summary"]["paper_shadow_activation_allowed"] is False
    assert payload["summary"]["gap_count"] >= 6
    categories = {row["gap_category"] for row in payload["evidence_gaps"]}
    assert set(repair.REQUIRED_GAP_CATEGORIES) <= categories
    assert any(
        row["gap_id"] == "backfill_coverage_normal_market_regime"
        for row in payload["evidence_gaps"]
    )
    assert any(
        row["gap_id"] == "signal_robustness_partial_signal_series"
        for row in payload["evidence_gaps"]
    )
    assert any(row["requires_candidate_redesign"] is True for row in payload["evidence_gaps"])

    validation = repair.validate_executable_research_evidence_gap_ledger_payload(payload)
    assert validation["status"] == "PASS"


def test_evidence_gap_ledger_cli_writes_and_validates(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "executable-research-evidence-gap-ledger",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    ledger_path = repair.default_evidence_repair_json_path(
        repair.EVIDENCE_GAP_LEDGER_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert payload["summary"]["blocking_gap_count"] > 0

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-executable-research-evidence-gap-ledger",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.EVIDENCE_GAP_LEDGER_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_backfill_partial_repair_plan_classifies_binding_repairable_windows(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    ledger = repair.build_executable_research_evidence_gap_ledger_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        ledger,
        repair.default_evidence_repair_json_path(
            repair.EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )

    payload = repair.build_backfill_partial_root_cause_repair_plan_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.BACKFILL_REPAIRABLE
    assert payload["summary"]["source_backfill_status"] == (
        next_cycle.CANDIDATE_BACKFILL_PARTIAL
    )
    assert payload["summary"]["incomplete_window_count"] == 6
    assert payload["summary"]["binding_repairable_window_count"] == 6
    assert payload["summary"]["candidate_spec_issue_window_count"] == 0
    diagnostics = payload["window_repair_diagnostics"]
    assert {row["window_id"] for row in diagnostics} == set(
        repair.REQUIRED_BACKFILL_WINDOWS
    )
    assert all(row["missing_dates"] == [] for row in diagnostics)
    assert all(
        row["missing_dates_status"] == "not_enumerated_in_source_artifact"
        for row in diagnostics
    )
    assert all("binding_repairable" in row["repairability"] for row in diagnostics)
    assert any(
        "historical_signal_series:rapid_drawdown" in row["missing_signal_outputs"]
        for row in diagnostics
    )

    validation = repair.validate_backfill_partial_root_cause_repair_plan_payload(
        payload
    )
    assert validation["status"] == "PASS"


def test_backfill_partial_repair_plan_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    ledger = repair.build_executable_research_evidence_gap_ledger_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        ledger,
        repair.default_evidence_repair_json_path(
            repair.EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "backfill-partial-root-cause-repair-plan",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    plan_path = repair.default_evidence_repair_json_path(
        repair.BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    assert payload["status"] == repair.BACKFILL_REPAIRABLE

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-backfill-partial-root-cause-repair-plan",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.BACKFILL_PARTIAL_REPAIR_PLAN_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_signal_robustness_drilldown_lists_exact_blockers(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_signal_robustness_blocker_drilldown_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.SIGNAL_ROBUSTNESS_REPAIRABLE
    assert payload["summary"]["blocker_count"] == 3
    assert payload["summary"]["repairable_without_rule_relaxation"] is True
    assert payload["summary"]["candidate_redesign_blocker_count"] == 0
    causes = {row["blocker_cause"] for row in payload["signal_blockers"]}
    assert causes == {
        "binding_fail_closed_condition",
        "stale_signal_series",
        "partial_market_coverage",
    }
    assert all(
        row["signal_completeness_rules_relaxed"] is False
        for row in payload["signal_blockers"]
    )
    assert all(row["expected_value"] for row in payload["signal_blockers"])
    assert any(
        row["failed_field"] == "signal_quality_checks[market_coverage_gap].status"
        for row in payload["signal_blockers"]
    )

    validation = repair.validate_signal_robustness_blocker_drilldown_payload(payload)
    assert validation["status"] == "PASS"


def test_signal_robustness_drilldown_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "signal-robustness-blocker-drilldown",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    drilldown_path = repair.default_evidence_repair_json_path(
        repair.SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(drilldown_path.read_text(encoding="utf-8"))
    assert payload["status"] == repair.SIGNAL_ROBUSTNESS_REPAIRABLE

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-signal-robustness-blocker-drilldown",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.SIGNAL_ROBUSTNESS_DRILLDOWN_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_window_fragility_attribution_separates_overfit_and_under_observed(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_window_fragility_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.WINDOW_FRAGILITY_ATTRIBUTION_READY
    assert payload["summary"]["fragility_judgment"] == (
        "MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED"
    )
    assert payload["summary"]["fragile_window_count"] == 2
    assert payload["summary"]["under_observed_window_count"] == 3
    assert payload["summary"]["acceptable_for_further_research"] is False
    modes = {row["failure_mode_id"] for row in payload["failure_modes"]}
    assert "drawdown_behavior_failure" in modes
    assert "under_observed_static_proxy" in modes
    assert "high_overfit_risk" in modes
    assert any(
        row["window_split_id"] == "recent_window"
        and row["drawdown_behavior"] == "attributed"
        for row in payload["window_attributions"]
    )

    validation = repair.validate_window_fragility_attribution_payload(payload)
    assert validation["status"] == "PASS"


def test_window_fragility_attribution_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "window-fragility-attribution",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    attribution_path = repair.default_evidence_repair_json_path(
        repair.WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(attribution_path.read_text(encoding="utf-8"))
    assert payload["summary"]["fragility_judgment"] == (
        "MIXED_OVERFIT_RISK_AND_UNDER_OBSERVED"
    )

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-window-fragility-attribution",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.WINDOW_FRAGILITY_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_stress_weakness_attribution_marks_redesign_required(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_stress_weakness_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.STRESS_WEAKNESS_ATTRIBUTION_READY
    assert payload["summary"]["source_stress_result"] == "WEAK"
    assert payload["summary"]["design_judgment"] == "REDESIGN_REQUIRED"
    assert payload["summary"]["redesign_required"] is True
    assert payload["summary"]["reject_current_candidate"] is False
    assert payload["summary"]["failed_scenario_count"] == 2
    assert payload["summary"]["warning_scenario_count"] == 4
    rows = {row["scenario_id"]: row for row in payload["stress_scenario_attributions"]}
    assert set(repair.REQUIRED_STRESS_SCENARIOS) == set(rows)
    assert rows["slow_drawdown"]["scenario_status"] == "FAIL"
    assert rows["slow_drawdown"]["drawdown_mismatch"] == (
        "blocking_drawdown_mismatch"
    )
    assert rows["slow_drawdown"]["redesign_required"] is True
    assert rows["v_shaped_recovery"]["scenario_status"] == "MISSING"
    assert rows["v_shaped_recovery"]["root_cause_category"] == (
        "required_stress_scenario_missing"
    )
    assert "turnover_proxy=0.85" in rows["rapid_drawdown"]["turnover_impact"]
    assert "rotation_count=1" in rows["false_risk_off_cluster"]["rotation_flip_issue"]
    root_causes = {row["root_cause_id"] for row in payload["stress_weakness_root_causes"]}
    assert {
        "structural_drawdown_failure",
        "required_stress_scenario_missing",
        "weak_return_drawdown_warning",
        "partial_static_proxy_evidence_limit",
    } <= root_causes

    validation = repair.validate_stress_weakness_attribution_payload(payload)
    assert validation["status"] == "PASS"


def test_stress_weakness_attribution_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "stress-weakness-attribution",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    attribution_path = repair.default_evidence_repair_json_path(
        repair.STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(attribution_path.read_text(encoding="utf-8"))
    assert payload["summary"]["design_judgment"] == "REDESIGN_REQUIRED"

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-stress-weakness-attribution",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.STRESS_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_cost_benchmark_weakness_attribution_marks_redesign_required(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_cost_benchmark_weakness_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.COST_BENCHMARK_WEAKNESS_ATTRIBUTION_READY
    assert payload["summary"]["source_cost_benchmark_status"] == (
        "COST_BENCHMARK_REVIEW_WEAK"
    )
    assert payload["summary"]["source_cost_sensitivity_status"] == (
        "NOT_MEANINGFUL_UNDER_COSTS"
    )
    assert payload["summary"]["design_judgment"] == "REDESIGN_REQUIRED"
    assert payload["summary"]["fixable_by_candidate_redesign"] is True
    assert payload["summary"]["reject_current_candidate"] is False
    cost_rows = {
        row["scenario_id"]: row for row in payload["cost_scenario_attributions"]
    }
    assert set(cost_rows) == set(repair.REQUIRED_COST_SCENARIOS)
    assert cost_rows["zero"]["cost_weakness_reason"] == "weak_net_return_proxy"
    assert "turnover_proxy=0.85" in cost_rows["high"]["high_turnover_assessment"]
    assert "cost_drag=0.002125" in cost_rows["high"]["cost_drag_assessment"]
    benchmark_rows = {
        row["baseline_id"]: row for row in payload["benchmark_baseline_attributions"]
    }
    assert set(benchmark_rows) == set(repair.REQUIRED_BENCHMARK_BASELINES)
    assert benchmark_rows["equal_weight_etf"]["benchmark_weakness_reason"] == (
        "benchmark_underperformance"
    )
    assert benchmark_rows["static_allocation"]["benchmark_weakness_reason"] == (
        "insufficient_outperformance_margin"
    )
    root_causes = {row["root_cause_id"] for row in payload["cost_benchmark_root_causes"]}
    assert {
        "weak_gross_return_proxy",
        "weak_net_return_proxy",
        "benchmark_underperformance",
        "insufficient_benchmark_outperformance",
        "partial_static_proxy_distortion",
        "turnover_cost_exposure",
    } <= root_causes

    validation = repair.validate_cost_benchmark_weakness_attribution_payload(payload)
    assert validation["status"] == "PASS"


def test_cost_benchmark_weakness_attribution_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "cost-benchmark-weakness-attribution",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    attribution_path = repair.default_evidence_repair_json_path(
        repair.COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(attribution_path.read_text(encoding="utf-8"))
    assert payload["summary"]["design_judgment"] == "REDESIGN_REQUIRED"

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-cost-benchmark-weakness-attribution",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.COST_BENCHMARK_WEAKNESS_ATTRIBUTION_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_redesign_hypothesis_v2_covers_required_targets(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_candidate_redesign_hypothesis_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.CANDIDATE_REDESIGN_HYPOTHESIS_READY
    assert payload["summary"]["hypothesis_count"] == 6
    assert payload["summary"]["p0_hypothesis_count"] == 3
    assert payload["summary"]["p1_hypothesis_count"] == 2
    assert payload["summary"]["p2_hypothesis_count"] == 1
    assert payload["summary"]["target_coverage_count"] == len(
        repair.REQUIRED_REDESIGN_TARGETS
    )
    hypotheses = {
        row["hypothesis_id"]: row for row in payload["candidate_redesign_hypotheses"]
    }
    assert "v2_dynamic_signal_repair" in hypotheses
    assert hypotheses["v2_dynamic_signal_repair"]["priority"] == "P0"
    assert "signal_robustness_repair" in hypotheses["v2_dynamic_signal_repair"][
        "target_areas"
    ]
    assert "v2_turnover_cost_benchmark_guard" in hypotheses
    assert {
        "lower_turnover",
        "benchmark_relative_behavior",
        "cost_survival",
    } <= set(hypotheses["v2_turnover_cost_benchmark_guard"]["target_areas"])
    assert all(
        row["paper_shadow_activation_allowed"] is False
        for row in payload["candidate_redesign_hypotheses"]
    )
    coverage = {row["target_area"]: row["covered"] for row in payload["target_coverage"]}
    assert all(coverage[target] is True for target in repair.REQUIRED_REDESIGN_TARGETS)
    assert payload["selection_boundary"]["selects_final_spec"] is False
    assert payload["selection_boundary"]["implements_binding"] is False

    validation = repair.validate_candidate_redesign_hypothesis_payload(payload)
    assert validation["status"] == "PASS"


def test_candidate_redesign_hypothesis_v2_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-redesign-hypothesis-v2",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    hypothesis_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(hypothesis_path.read_text(encoding="utf-8"))
    assert payload["summary"]["hypothesis_count"] == 6

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-redesign-hypothesis-v2",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_REDESIGN_HYPOTHESIS_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_v2_spec_freeze_selects_p0_hypothesis(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)

    payload = repair.build_candidate_v2_spec_freeze_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.CANDIDATE_V2_SPEC_FREEZE_READY
    assert payload["summary"]["selected_hypothesis_id"] == (
        "v2_turnover_cost_benchmark_guard"
    )
    assert payload["summary"]["selected_hypothesis_priority"] == "P0"
    assert payload["summary"]["paper_shadow_eligible"] is False
    spec = payload["frozen_candidate_spec"]
    assert spec["candidate_id"] == (
        "median_plus_regime_mismatch_filter_v2_turnover_cost_benchmark_guard"
    )
    assert "historical_candidate_signal_series_by_date" in spec["signal_inputs"]
    assert spec["paper_shadow_eligible"] is False
    assert any("Equal-weight ETF" in item for item in spec["benchmark_expectations"])
    assert any("signal robustness" in item for item in spec["stop_conditions"])
    differences = {
        row["difference_id"] for row in payload["differences_from_trading_470_candidate"]
    }
    assert {
        "dynamic_signal_series_required",
        "turnover_aware_rotation_guard",
        "cost_benchmark_precheck",
        "explicit_stress_validation_contexts",
    } <= differences
    assert payload["freeze_boundary"]["implements_binding"] is False
    assert payload["freeze_boundary"]["runs_backfill"] is False

    validation = repair.validate_candidate_v2_spec_freeze_payload(payload)
    assert validation["status"] == "PASS"


def test_candidate_v2_spec_freeze_cli_writes_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-v2-spec-freeze",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    spec_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    assert payload["summary"]["candidate_id"].endswith(
        "v2_turnover_cost_benchmark_guard"
    )

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-v2-spec-freeze",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_SPEC_FREEZE_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_v2_executable_binding_update_generates_v2_rows(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    _write_candidate_v2_spec_freeze(reports_dir)
    _write_v2_feature_fixture(feature_path)

    payload = repair.build_candidate_v2_executable_binding_update_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        feature_path=feature_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] == repair.CANDIDATE_V2_EXECUTABLE_BINDING_READY_WITH_WARNINGS
    assert payload["summary"]["signal_row_count"] > 1
    assert payload["summary"]["weight_row_count"] == payload["summary"]["signal_row_count"]
    assert payload["summary"]["safety_audit_status"] in {
        binding_reports.SAFETY_PASS,
        binding_reports.SAFETY_WARNING,
    }
    assert payload["summary"]["safety_audit_allows_mini_backfill"] is True
    assert payload["summary"]["paper_shadow_eligible"] is False
    assert any(
        row["turnover_guard_applied"] is True
        for row in payload["v2_candidate_signal_series"]
    )
    assert all(
        row["hypothetical_research_weight"]["official_target_weights"] is False
        for row in payload["v2_hypothetical_research_weight_series"]
    )

    validation = repair.validate_candidate_v2_executable_binding_update_payload(
        payload
    )
    assert validation["status"] == "PASS"


def test_candidate_v2_executable_binding_update_cli_writes_and_validates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    _write_candidate_v2_spec_freeze(reports_dir)
    _write_v2_feature_fixture(feature_path)
    monkeypatch.setattr(
        reports_cli,
        "_run_next_research_data_quality_gate",
        lambda **_: _passing_data_quality_gate(reports_dir),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-v2-executable-binding-update",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--feature-path",
            str(feature_path),
        ],
    )
    assert result.exit_code == 0, result.output

    binding_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    assert payload["summary"]["signal_row_count"] > 1

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-v2-executable-binding-update",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_EXECUTABLE_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_v2_mini_backfill_generates_compact_window_metrics(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    _write_candidate_v2_spec_freeze(reports_dir)
    _write_v2_feature_fixture(feature_path)
    _write_candidate_v2_executable_binding_update(reports_dir, feature_path)
    _write_v2_price_fixture(prices_path)

    payload = repair.build_candidate_v2_mini_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        prices_path=prices_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] in {
        repair.V2_MINI_BACKFILL_PROMISING,
        repair.V2_MINI_BACKFILL_NEEDS_MORE_EVIDENCE,
        repair.V2_MINI_BACKFILL_WEAK,
    }
    assert payload["summary"]["mini_window_count"] == 4
    assert payload["summary"]["completed_window_count"] == 4
    assert payload["summary"]["aggregate_return_proxy"] is not None
    assert payload["summary"]["aggregate_drawdown_proxy"] is not None
    assert payload["summary"]["turnover_proxy"] is not None
    assert {row["window_id"] for row in payload["mini_backfill_windows"]} == {
        "normal_market_regime",
        "slow_drawdown",
        "high_volatility_sideways_market",
        "false_risk_off_cluster",
    }
    assert all(
        row["cost_scenario_inputs_available"] is True
        for row in payload["cost_proxy_inputs"]
    )
    assert payload["summary"]["official_target_weights"] is False
    assert payload["summary"]["paper_shadow_activation_allowed"] is False
    assert payload["safety_boundary"]["full_backfill_executed"] is False
    assert payload["safety_boundary"]["paper_shadow_outputs_generated"] is False

    validation = repair.validate_candidate_v2_mini_backfill_payload(payload)
    assert validation["status"] == "PASS"


def test_candidate_v2_mini_backfill_cli_writes_and_validates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    _write_candidate_v2_spec_freeze(reports_dir)
    _write_v2_feature_fixture(feature_path)
    _write_candidate_v2_executable_binding_update(reports_dir, feature_path)
    _write_v2_price_fixture(prices_path)
    monkeypatch.setattr(
        reports_cli,
        "_run_next_research_data_quality_gate",
        lambda **_: _passing_data_quality_gate(reports_dir),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-v2-mini-backfill",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--prices-path",
            str(prices_path),
        ],
    )
    assert result.exit_code == 0, result.output

    mini_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(mini_path.read_text(encoding="utf-8"))
    assert payload["summary"]["mini_window_count"] == 4

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-v2-mini-backfill",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_v2_mini_gate_blocks_full_backfill_when_mini_is_weak(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_candidate_v2_mini_gate_prerequisites(
        reports_dir=reports_dir,
        feature_path=feature_path,
        prices_path=prices_path,
    )

    payload = repair.build_candidate_v2_mini_gate_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] == repair.V2_NEEDS_REDESIGN
    assert payload["summary"]["source_mini_backfill_status"] == (
        repair.V2_MINI_BACKFILL_WEAK
    )
    assert payload["summary"]["full_backfill_allowed"] is False
    assert payload["summary"]["full_backfill_blocked_reason"] == "mini_backfill_weak"
    assert any(
        row["evidence_id"] == "mini_backfill_weak"
        for row in payload["strongest_negative_evidence"]
    )
    assert any(
        row["evidence_id"] == "mini_windows_complete"
        for row in payload["strongest_positive_evidence"]
    )
    assert payload["safety_boundary"]["full_backfill_executed"] is False
    assert payload["safety_boundary"]["paper_shadow_outputs_generated"] is False

    validation = repair.validate_candidate_v2_mini_gate_payload(payload)
    assert validation["status"] == "PASS"


def test_candidate_v2_mini_gate_cli_writes_and_validates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_candidate_v2_mini_gate_prerequisites(
        reports_dir=reports_dir,
        feature_path=feature_path,
        prices_path=prices_path,
    )
    monkeypatch.setattr(
        reports_cli,
        "_run_next_research_data_quality_gate",
        lambda **_: _passing_data_quality_gate(reports_dir),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-v2-mini-gate",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--prices-path",
            str(prices_path),
        ],
    )
    assert result.exit_code == 0, result.output

    gate_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_MINI_GATE_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(gate_path.read_text(encoding="utf-8"))
    assert payload["status"] == repair.V2_NEEDS_REDESIGN
    assert payload["summary"]["full_backfill_allowed"] is False

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-v2-mini-gate",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def test_candidate_v2_full_backfill_blocks_when_mini_gate_does_not_proceed(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_candidate_v2_full_backfill_prerequisites(
        reports_dir=reports_dir,
        feature_path=feature_path,
        prices_path=prices_path,
    )

    payload = repair.build_candidate_v2_full_backfill_if_approved_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    assert payload["status"] == repair.V2_FULL_BACKFILL_BLOCKED_BY_MINI_GATE
    assert payload["summary"]["source_mini_gate_decision"] == repair.V2_NEEDS_REDESIGN
    assert payload["summary"]["full_backfill_executed"] is False
    assert payload["full_backfill_windows"] == []
    assert {
        row["output_id"] for row in payload["blocked_full_backfill_outputs"]
    } == set(repair.V2_FULL_BACKFILL_REQUIRED_OUTPUTS)
    assert all(
        row["generated"] is False
        for row in payload["blocked_full_backfill_outputs"]
    )
    assert payload["safety_boundary"]["full_backfill_executed"] is False
    assert payload["safety_boundary"]["paper_shadow_outputs_generated"] is False

    validation = repair.validate_candidate_v2_full_backfill_if_approved_payload(payload)
    assert validation["status"] == "PASS"


def test_candidate_v2_full_backfill_cli_writes_blocked_and_validates(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    feature_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices_daily.csv"
    _write_candidate_v2_full_backfill_prerequisites(
        reports_dir=reports_dir,
        feature_path=feature_path,
        prices_path=prices_path,
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "candidate-v2-full-backfill-if-approved",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert result.exit_code == 0, result.output

    full_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_FULL_BACKFILL_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    payload = json.loads(full_path.read_text(encoding="utf-8"))
    assert payload["status"] == repair.V2_FULL_BACKFILL_BLOCKED_BY_MINI_GATE
    assert payload["summary"]["full_backfill_executed"] is False

    validate_result = runner.invoke(
        app,
        [
            "reports",
            "validate-candidate-v2-full-backfill-if-approved",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output

    validation_path = repair.default_evidence_repair_json_path(
        repair.CANDIDATE_V2_FULL_BACKFILL_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["status"] == "PASS"


def _write_evidence_repair_prerequisites(reports_dir: Path) -> None:
    ledger = repair.build_executable_research_evidence_gap_ledger_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        ledger,
        repair.default_evidence_repair_json_path(
            repair.EVIDENCE_GAP_LEDGER_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    repair_plan = repair.build_backfill_partial_root_cause_repair_plan_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        repair_plan,
        repair.default_evidence_repair_json_path(
            repair.BACKFILL_PARTIAL_REPAIR_PLAN_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    signal_drilldown = repair.build_signal_robustness_blocker_drilldown_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        signal_drilldown,
        repair.default_evidence_repair_json_path(
            repair.SIGNAL_ROBUSTNESS_DRILLDOWN_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    window_attribution = repair.build_window_fragility_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        window_attribution,
        repair.default_evidence_repair_json_path(
            repair.WINDOW_FRAGILITY_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    stress_attribution = repair.build_stress_weakness_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        stress_attribution,
        repair.default_evidence_repair_json_path(
            repair.STRESS_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    cost_attribution = repair.build_cost_benchmark_weakness_attribution_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        cost_attribution,
        repair.default_evidence_repair_json_path(
            repair.COST_BENCHMARK_WEAKNESS_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    hypotheses = repair.build_candidate_redesign_hypothesis_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        hypotheses,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_REDESIGN_HYPOTHESIS_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _write_candidate_v2_spec_freeze(reports_dir: Path) -> None:
    spec = repair.build_candidate_v2_spec_freeze_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    repair.write_evidence_repair_json(
        spec,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_SPEC_FREEZE_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _write_candidate_v2_executable_binding_update(
    reports_dir: Path,
    feature_path: Path,
) -> None:
    binding = repair.build_candidate_v2_executable_binding_update_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        feature_path=feature_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    repair.write_evidence_repair_json(
        binding,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_EXECUTABLE_BINDING_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _write_candidate_v2_mini_gate_prerequisites(
    *,
    reports_dir: Path,
    feature_path: Path,
    prices_path: Path,
) -> None:
    _write_trading_470_sources(reports_dir)
    _write_evidence_repair_prerequisites(reports_dir)
    _write_candidate_v2_spec_freeze(reports_dir)
    _write_v2_feature_fixture(feature_path)
    _write_candidate_v2_executable_binding_update(reports_dir, feature_path)
    _write_v2_price_fixture(prices_path)
    mini = repair.build_candidate_v2_mini_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        prices_path=prices_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    repair.write_evidence_repair_json(
        mini,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_MINI_BACKFILL_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    mini_validation = repair.validate_candidate_v2_mini_backfill_payload(mini)
    repair.write_evidence_repair_json(
        mini_validation,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_MINI_BACKFILL_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _write_candidate_v2_full_backfill_prerequisites(
    *,
    reports_dir: Path,
    feature_path: Path,
    prices_path: Path,
) -> None:
    _write_candidate_v2_mini_gate_prerequisites(
        reports_dir=reports_dir,
        feature_path=feature_path,
        prices_path=prices_path,
    )
    mini_gate = repair.build_candidate_v2_mini_gate_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    repair.write_evidence_repair_json(
        mini_gate,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_MINI_GATE_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    mini_gate_validation = repair.validate_candidate_v2_mini_gate_payload(mini_gate)
    repair.write_evidence_repair_json(
        mini_gate_validation,
        repair.default_evidence_repair_json_path(
            repair.CANDIDATE_V2_MINI_GATE_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _passing_data_quality_gate(reports_dir: Path) -> dict[str, object]:
    return {
        "status": "PASS",
        "passed": True,
        "error_count": 0,
        "warning_count": 0,
        "report_path": str(reports_dir / "data_quality_fixture.md"),
    }


def _write_v2_feature_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dates = [
        "2023-01-03",
        "2023-09-01",
        "2024-03-08",
        "2024-07-10",
        "2025-01-02",
    ]
    symbols = ("QQQ", "SMH", "SOXX", "SPY")
    rows = [
        "date,symbol,above_ma_20,above_ma_50,above_ma_100,above_ma_200,"
        "ma_20_slope,ret_20d,ret_60d,ret_120d,rs_vs_spy_60d,rs_vs_qqq_60d,"
        "rs_vs_smh_60d,realized_vol_20d,drawdown_63d"
    ]
    for item in dates:
        for symbol in symbols:
            rs_spy = 0.0 if symbol == "SPY" else 0.06
            rs_qqq = 0.0 if symbol in {"SPY", "QQQ"} else 0.04
            rs_smh = 0.0 if symbol != "SOXX" else 0.01
            drawdown = -0.2 if item == "2025-01-02" else -0.03
            rows.append(
                ",".join(
                    [
                        item,
                        symbol,
                        "true",
                        "true",
                        "true",
                        "true",
                        "0.01",
                        "0.04",
                        "0.08",
                        "0.12",
                        str(rs_spy),
                        str(rs_qqq),
                        str(rs_smh),
                        "0.18",
                        str(drawdown),
                    ]
                )
            )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_v2_price_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        "date,ticker,symbol,open,high,low,close,adj_close,volume,source,updated_at,"
        "source_symbol,canonical_symbol"
    ]
    base_prices = {
        "QQQ": 100.0,
        "SMH": 80.0,
        "SOXX": 70.0,
        "SPY": 400.0,
    }
    date_multipliers = {
        "2023-01-03": 1.0,
        "2023-01-04": 1.02,
        "2023-09-01": 1.10,
        "2023-09-05": 1.11,
        "2025-01-02": 1.20,
        "2025-01-03": 1.18,
    }
    for item_date, multiplier in date_multipliers.items():
        for symbol, base_price in base_prices.items():
            price = round(base_price * multiplier, 4)
            rows.append(
                ",".join(
                    [
                        item_date,
                        symbol,
                        symbol,
                        str(price),
                        str(price),
                        str(price),
                        str(price),
                        str(price),
                        "1000",
                        "fixture",
                        "",
                        symbol,
                        symbol,
                    ]
                )
            )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_trading_470_sources(reports_dir: Path) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    binding_sources = {
        binding_reports.CONTRACT_REPORT_TYPE: {
            "report_type": binding_reports.CONTRACT_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "EXECUTABLE_BINDING_CONTRACT_READY",
            "summary": {"candidate_id": "candidate_v1"},
            "production_effect": "none",
        },
        binding_reports.SIGNAL_BINDING_REPORT_TYPE: {
            "report_type": binding_reports.SIGNAL_BINDING_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": binding_reports.SIGNAL_BINDING_COMPLETE_WITH_WARNINGS,
            "summary": {
                "candidate_id": "candidate_v1",
                "signal_row_count": 1,
                "latest_signal_date": "2026-06-16",
                "warning_reason": (
                    "signal_date_outside_frozen_validation_windows,"
                    "signal_input_completeness_warning"
                ),
            },
            "production_effect": "none",
        },
        binding_reports.WEIGHT_BINDING_REPORT_TYPE: {
            "report_type": binding_reports.WEIGHT_BINDING_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": binding_reports.WEIGHT_BINDING_COMPLETE_WITH_WARNINGS,
            "summary": {"candidate_id": "candidate_v1", "turnover_proxy": 0.85},
            "production_effect": "none",
        },
        binding_reports.SAFETY_AUDIT_REPORT_TYPE: {
            "report_type": binding_reports.SAFETY_AUDIT_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": binding_reports.SAFETY_WARNING,
            "summary": {"candidate_id": "candidate_v1", "acceptable_warning": True},
            "production_effect": "none",
        },
    }
    for report_type, payload in binding_sources.items():
        path = binding_reports.default_executable_binding_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        )
        path.write_text(json.dumps(payload), encoding="utf-8")

    cost_source_path = reports_dir / "fixtures" / "cost_sensitivity_review.json"
    cost_source_path.parent.mkdir(parents=True, exist_ok=True)
    cost_source_path.write_text(
        json.dumps(_cost_sensitivity_source_fixture()),
        encoding="utf-8",
    )
    benchmark_source_path = (
        reports_dir / "fixtures" / "benchmark_baseline_control_pack.json"
    )
    benchmark_source_path.write_text(
        json.dumps(_benchmark_baseline_source_fixture()),
        encoding="utf-8",
    )

    next_sources = {
        next_cycle.BACKFILL_REPORT_TYPE: {
            "report_type": next_cycle.BACKFILL_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
            "requested_date_range": "2023-01-03..2025-04-30",
            "summary": {
                "candidate_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
                "candidate_id": "candidate_v1",
                "requested_date_range": "2023-01-03..2025-04-30",
            },
            "backfill_windows": _backfill_windows_fixture(),
            "partial_reasons": [
                {
                    "issue_id": "historical_dynamic_binding_unavailable",
                    "recommended_action": (
                        "extend_binding_to_historical_signal_and_weight_series"
                    ),
                },
                {
                    "issue_id": "single_point_signal_binding_used_as_static_proxy",
                    "recommended_action": "repair_backfill_input",
                },
                {
                    "issue_id": "single_point_weight_binding_used_as_static_proxy",
                    "recommended_action": "repair_backfill_input",
                },
            ],
            "production_effect": "none",
        },
        next_cycle.STRESS_REVIEW_REPORT_TYPE: {
            "report_type": next_cycle.STRESS_REVIEW_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "WEAK",
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": "2023-01-03..2025-04-30",
            "summary": {
                "stress_result": "WEAK",
                "candidate_id": "candidate_v1",
                "requested_date_range": "2023-01-03..2025-04-30",
                "scenario_count": 6,
                "blocking_scenario_count": 1,
                "warning_scenario_count": 5,
                "source_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
                "partial_static_proxy": True,
            },
            "scenario_reviews": _stress_reviews_fixture(),
            "production_effect": "none",
        },
        next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE: {
            "report_type": next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "COST_BENCHMARK_REVIEW_WEAK",
            "requested_date_range": "2023-01-03..2025-04-30",
            "input_artifacts": {
                "cost_sensitivity_framework": str(cost_source_path),
                "benchmark_baseline_control": str(benchmark_source_path),
            },
            "summary": {
                "cost_survival_status": "COST_SURVIVAL_WARNING",
                "benchmark_relative_status": "BENCHMARK_UNDERPERFORMS",
                "turnover_penalty": 0.002125,
                "net_proxy_result": "AVAILABLE",
                "source_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
                "turnover_proxy": 0.85,
                "aggregate_return_proxy": 0.006,
                "scenario_count": 4,
                "baseline_count": 5,
                "major_blocker_count": 1,
                "major_warning_count": 4,
                "production_effect": "none",
            },
            "cost_scenario_reviews": _cost_scenario_reviews_fixture(),
            "benchmark_reviews": _benchmark_reviews_fixture(),
            "major_blockers": [
                {
                    "issue_id": "equal_weight_etf",
                    "reason": "BENCHMARK_UNDERPERFORMS",
                }
            ],
            "major_warnings": [
                {"issue_id": "static_allocation", "reason": "BENCHMARK_MIXED"},
                {"issue_id": "no_trade", "reason": "BENCHMARK_MIXED"},
                {"issue_id": "qqq_only", "reason": "BENCHMARK_MIXED"},
                {"issue_id": "spy_only", "reason": "BENCHMARK_MIXED"},
            ],
            "production_effect": "none",
        },
        next_cycle.VS_RETURNED_REPORT_TYPE: {
            "report_type": next_cycle.VS_RETURNED_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "MIXED_VS_RETURNED_CANDIDATE",
            "comparison_rows": [
                {
                    "metric_id": "signal_robustness",
                    "comparison_status": "NO_IMPROVEMENT",
                    "new_candidate_evidence": "signal_completeness=PARTIAL_STATIC_BINDING",
                    "returned_failure_mode_id": "signal_input_stability_warning",
                    "interpretation": "Historical signal series remain incomplete.",
                }
            ],
            "production_effect": "none",
        },
        next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE: {
            "report_type": next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "SIGNAL_ROBUSTNESS_BLOCKED",
            "summary": {
                "signal_robustness_status": "SIGNAL_ROBUSTNESS_BLOCKED",
                "source_signal_binding_status": (
                    binding_reports.SIGNAL_BINDING_COMPLETE_WITH_WARNINGS
                ),
                "signal_row_count": 1,
                "backfill_signal_completeness": "PARTIAL_STATIC_BINDING",
                "blocking_check_count": 3,
                "production_effect": "none",
            },
            "signal_quality_checks": [
                {
                    "check_id": "missing_feature_columns",
                    "status": "PASS",
                    "evidence": "required feature/signal columns present",
                    "fail_closed": True,
                    "signal_completeness_rules_relaxed": False,
                    "recommended_action": "retain_signal_binding_evidence",
                },
                {
                    "check_id": "partial_signal_series",
                    "status": "BLOCKING",
                    "evidence": "signal_row_count=1",
                    "fail_closed": True,
                    "signal_completeness_rules_relaxed": False,
                    "recommended_action": "repair_signal_binding_inputs",
                },
                {
                    "check_id": "stale_signal_series",
                    "status": "BLOCKING",
                    "evidence": (
                        "latest_signal_date=2026-06-16; "
                        "warning_reasons=signal_date_outside_frozen_validation_windows"
                    ),
                    "fail_closed": True,
                    "signal_completeness_rules_relaxed": False,
                    "recommended_action": "repair_signal_binding_inputs",
                },
                {
                    "check_id": "schema_version_mismatch",
                    "status": "PASS",
                    "evidence": "schema and feature versions inspected",
                    "fail_closed": True,
                    "signal_completeness_rules_relaxed": False,
                    "recommended_action": "retain_signal_binding_evidence",
                },
                {
                    "check_id": "market_coverage_gap",
                    "status": "BLOCKING",
                    "evidence": "missing_data_count=6",
                    "fail_closed": True,
                    "signal_completeness_rules_relaxed": False,
                    "recommended_action": "repair_signal_binding_inputs",
                }
            ],
            "production_effect": "none",
        },
        next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE: {
            "report_type": next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "WINDOW_FRAGILE",
            "summary": {
                "window_sensitivity_status": "WINDOW_FRAGILE",
                "source_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
                "split_count": 5,
                "weak_split_count": 2,
                "partial_static_proxy_split_count": 3,
                "performance_dispersion": 0.6,
                "turnover_dispersion": 0.0,
                "drawdown_behavior_dispersion": 0.2,
                "overfit_risk": "HIGH",
                "production_effect": "none",
            },
            "blocking_issues": [
                {
                    "issue_id": "recent_window",
                    "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
                }
            ],
            "window_splits": _window_splits_fixture(),
            "production_effect": "none",
        },
        next_cycle.RESEARCH_GATE_REPORT_TYPE: {
            "report_type": next_cycle.RESEARCH_GATE_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "NEEDS_MORE_EVIDENCE",
            "summary": {"research_gate_decision": "NEEDS_MORE_EVIDENCE"},
            "production_effect": "none",
        },
        next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE: {
            "report_type": next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE",
            "summary": {
                "research_cycle_snapshot_status": (
                    "EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
                ),
                "research_gate_decision": "NEEDS_MORE_EVIDENCE",
                "candidate_id": "candidate_v1",
                "requested_date_range": "2023-01-03..2025-04-30",
            },
            "production_effect": "none",
        },
    }
    for report_type, payload in next_sources.items():
        path = next_cycle.default_next_research_cycle_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        )
        path.write_text(json.dumps(payload), encoding="utf-8")


def _backfill_windows_fixture() -> list[dict[str, object]]:
    windows = [
        ("normal_market_regime", "2023-01-03", "2023-07-31", 0.1, -0.05),
        ("rapid_drawdown", "2024-07-10", "2024-08-09", -0.15, -0.19),
        ("slow_drawdown", "2025-01-02", "2025-04-30", -0.1, -0.27),
        (
            "high_volatility_sideways_market",
            "2023-08-01",
            "2023-11-15",
            -0.01,
            -0.13,
        ),
        (
            "ai_semiconductor_correction",
            "2024-03-08",
            "2024-04-19",
            -0.08,
            -0.09,
        ),
        ("false_risk_off_cluster", "2023-09-01", "2023-10-31", -0.09, -0.1),
    ]
    return [
        {
            "window_id": window_id,
            "start": start,
            "end": end,
            "backfill_window_status": "PARTIAL",
            "signal_completeness": "PARTIAL_STATIC_BINDING",
            "missing_data_list": [f"historical_signal_series:{window_id}"],
            "return_proxy": return_proxy,
            "drawdown_proxy": drawdown_proxy,
            "turnover": 0.85,
            "price_observation_count": 10,
            "return_observation_count": 9,
            "production_effect": "none",
        }
        for window_id, start, end, return_proxy, drawdown_proxy in windows
    ]


def _stress_reviews_fixture() -> list[dict[str, object]]:
    return [
        {
            "scenario_id": "normal_market_regime",
            "scenario_status": "WARNING",
            "return_proxy": 0.48,
            "drawdown_proxy": -0.06,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 0,
            "evaluation": "Metric is real but partial because binding lacks historical signals.",
            "recommended_action": "complete_executable_backfill_before_stress_review",
            "production_effect": "none",
        },
        {
            "scenario_id": "rapid_drawdown",
            "scenario_status": "WARNING",
            "return_proxy": -0.15,
            "drawdown_proxy": -0.19,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 0,
            "evaluation": "Return/drawdown proxy remains weak in this stress window.",
            "recommended_action": "redesign_candidate_stress_handling",
            "production_effect": "none",
        },
        {
            "scenario_id": "slow_drawdown",
            "scenario_status": "FAIL",
            "return_proxy": -0.1,
            "drawdown_proxy": -0.27,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 0,
            "evaluation": "Drawdown proxy breaches conservative stress blocker.",
            "recommended_action": "complete_executable_backfill_before_stress_review",
            "production_effect": "none",
        },
        {
            "scenario_id": "high_volatility_sideways_market",
            "scenario_status": "WARNING",
            "return_proxy": -0.01,
            "drawdown_proxy": -0.13,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 1,
            "evaluation": "Return/drawdown proxy remains weak in this stress window.",
            "recommended_action": "redesign_candidate_stress_handling",
            "production_effect": "none",
        },
        {
            "scenario_id": "ai_semiconductor_correction",
            "scenario_status": "WARNING",
            "return_proxy": -0.08,
            "drawdown_proxy": -0.09,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 0,
            "evaluation": "Return/drawdown proxy remains weak in this stress window.",
            "recommended_action": "redesign_candidate_stress_handling",
            "production_effect": "none",
        },
        {
            "scenario_id": "false_risk_off_cluster",
            "scenario_status": "WARNING",
            "return_proxy": -0.09,
            "drawdown_proxy": -0.1,
            "turnover_proxy": 0.85,
            "rotation_count": 1,
            "false_risk_off_count": 2,
            "evaluation": "Return/drawdown proxy remains weak in this stress window.",
            "recommended_action": "redesign_candidate_stress_handling",
            "production_effect": "none",
        },
    ]


def _cost_sensitivity_source_fixture() -> dict[str, object]:
    return {
        "report_type": "etf_dynamic_v3_cost_sensitivity_review",
        "as_of": RUN_DATE.isoformat(),
        "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
        "turnover": 0.005945,
        "gross_performance_proxy": 0.00638,
        "gross_improvement_proxy": 0.001144,
        "worst_net_improvement_proxy": 0.001129,
        "meaningful_improvement_threshold": 0.0025,
        "scenario_results": [
            {
                "scenario_id": scenario_id,
                "label": label,
                "classification": "NOT_MEANINGFUL",
                "total_cost_bps": total_cost_bps,
                "turnover": 0.005945,
                "cost_drag": cost_drag,
                "gross_improvement_proxy": 0.001144,
                "gross_performance_proxy": 0.00638,
                "net_improvement_proxy": net_improvement,
                "net_performance_proxy": 0.00638 - cost_drag,
                "meaningful_improvement_threshold": 0.0025,
                "improvement_remains_meaningful": False,
                "production_effect": "none",
            }
            for scenario_id, label, total_cost_bps, cost_drag, net_improvement in [
                ("zero", "Zero Cost", 0.0, 0.0, 0.001144),
                ("low", "Low Cost", 3.0, 0.000255, 0.001142),
                ("medium", "Medium Cost", 10.0, 0.00085, 0.001138),
                ("high", "High Cost", 25.0, 0.002125, 0.001129),
            ]
        ],
        "production_effect": "none",
    }


def _benchmark_baseline_source_fixture() -> dict[str, object]:
    return {
        "report_type": "etf_dynamic_v3_benchmark_baseline_control_pack",
        "as_of": RUN_DATE.isoformat(),
        "benchmark_baseline_status": "CANDIDATE_UNDERPERFORMS_BASELINES",
        "required_baselines_present": True,
        "missing_required_baselines": [],
        "minimum_outperformance_threshold": 0.0025,
        "comparison_summary": {
            "baseline_count": 5,
            "outperformed_baseline_count": 0,
            "underperformed_baseline_count": 5,
        },
        "baselines": [
            {
                "baseline_id": baseline_id,
                "comparison_classification": "NOT_OUTPERFORMED",
                "baseline_net_performance_proxy": baseline_return,
                "candidate_net_performance_proxy": 0.006,
                "candidate_delta_vs_baseline": 0.006 - baseline_return,
                "minimum_outperformance_threshold": 0.0025,
                "production_effect": "none",
            }
            for baseline_id, baseline_return in [
                ("static_allocation", 0.0052),
                ("no_trade", 0.0052),
                ("qqq_only", 0.0058),
                ("spy_only", 0.004),
                ("equal_weight_etf", 0.008),
            ]
        ],
        "production_effect": "none",
    }


def _cost_scenario_reviews_fixture() -> list[dict[str, object]]:
    return [
        {
            "scenario_id": scenario_id,
            "label": label,
            "total_cost_bps": total_cost_bps,
            "turnover_proxy": 0.85,
            "gross_return_proxy": 0.006,
            "cost_drag": cost_drag,
            "net_proxy_result": 0.006 - cost_drag,
            "meaningful_threshold": 0.0025,
            "cost_survival_status": "COST_SURVIVAL_PASS",
            "source_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
            "production_effect": "none",
        }
        for scenario_id, label, total_cost_bps, cost_drag in [
            ("zero", "Zero Cost", 0.0, 0.0),
            ("low", "Low Cost", 3.0, 0.000255),
            ("medium", "Medium Cost", 10.0, 0.00085),
            ("high", "High Cost", 25.0, 0.002125),
        ]
    ]


def _benchmark_reviews_fixture() -> list[dict[str, object]]:
    return [
        {
            "baseline_id": baseline_id,
            "benchmark_relative_status": status,
            "candidate_return_proxy": 0.006,
            "baseline_return_proxy": baseline_return,
            "candidate_delta_vs_baseline": round(0.006 - baseline_return, 6),
            "minimum_outperformance_threshold": 0.0025,
            "source_backfill_status": next_cycle.CANDIDATE_BACKFILL_PARTIAL,
            "production_effect": "none",
        }
        for baseline_id, baseline_return, status in [
            ("static_allocation", 0.0052, "BENCHMARK_MIXED"),
            ("no_trade", 0.0052, "BENCHMARK_MIXED"),
            ("qqq_only", 0.0058, "BENCHMARK_MIXED"),
            ("spy_only", 0.004, "BENCHMARK_MIXED"),
            ("equal_weight_etf", 0.008, "BENCHMARK_UNDERPERFORMS"),
        ]
    ]


def _window_splits_fixture() -> list[dict[str, object]]:
    return [
        {
            "window_split_id": "early_window",
            "source_windows": [
                "normal_market_regime",
                "high_volatility_sideways_market",
                "false_risk_off_cluster",
            ],
            "status": "PARTIAL_STATIC_PROXY",
            "average_return_proxy": 0.12,
            "average_turnover_proxy": 0.85,
            "worst_drawdown_proxy": -0.13,
            "evaluation": "Metrics exist but rely on partial static binding evidence.",
            "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
            "production_effect": "none",
        },
        {
            "window_split_id": "middle_window",
            "source_windows": ["rapid_drawdown", "ai_semiconductor_correction"],
            "status": "PARTIAL_STATIC_PROXY",
            "average_return_proxy": -0.11,
            "average_turnover_proxy": 0.85,
            "worst_drawdown_proxy": -0.19,
            "evaluation": "Metrics exist but rely on partial static binding evidence.",
            "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
            "production_effect": "none",
        },
        {
            "window_split_id": "recent_window",
            "source_windows": ["slow_drawdown"],
            "status": "WEAK",
            "average_return_proxy": -0.1,
            "average_turnover_proxy": 0.85,
            "worst_drawdown_proxy": -0.27,
            "evaluation": "Worst drawdown proxy breaches conservative stress blocker.",
            "recommended_action": "repair_or_revise_candidate_before_research_gate",
            "production_effect": "none",
        },
        {
            "window_split_id": "stress_heavy_window",
            "source_windows": [
                "rapid_drawdown",
                "slow_drawdown",
                "ai_semiconductor_correction",
            ],
            "status": "WEAK",
            "average_return_proxy": -0.12,
            "average_turnover_proxy": 0.85,
            "worst_drawdown_proxy": -0.27,
            "evaluation": "Worst drawdown proxy breaches conservative stress blocker.",
            "recommended_action": "repair_or_revise_candidate_before_research_gate",
            "production_effect": "none",
        },
        {
            "window_split_id": "calm_market_window",
            "source_windows": ["normal_market_regime"],
            "status": "PARTIAL_STATIC_PROXY",
            "average_return_proxy": 0.48,
            "average_turnover_proxy": 0.85,
            "worst_drawdown_proxy": -0.06,
            "evaluation": "Metrics exist but rely on partial static binding evidence.",
            "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
            "production_effect": "none",
        },
    ]
