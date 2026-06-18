from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
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
            "scenario_reviews": [
                {
                    "scenario_id": "slow_drawdown",
                    "scenario_status": "FAIL",
                    "return_proxy": -0.1,
                    "drawdown_proxy": -0.27,
                    "turnover_proxy": 0.85,
                    "rotation_count": 1,
                    "evaluation": "Drawdown proxy breaches conservative stress blocker.",
                    "recommended_action": "complete_executable_backfill_before_stress_review",
                }
            ],
            "production_effect": "none",
        },
        next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE: {
            "report_type": next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
            "as_of": RUN_DATE.isoformat(),
            "status": "COST_BENCHMARK_REVIEW_WEAK",
            "summary": {
                "cost_survival_status": "COST_SURVIVAL_WARNING",
                "turnover_penalty": 0.002125,
                "net_proxy_result": "AVAILABLE",
                "turnover_proxy": 0.85,
                "aggregate_return_proxy": 0.006,
            },
            "benchmark_reviews": [
                {
                    "baseline_id": "equal_weight_etf",
                    "benchmark_relative_status": "BENCHMARK_UNDERPERFORMS",
                    "candidate_delta_vs_baseline": -0.001,
                    "minimum_outperformance_threshold": 0.0025,
                    "candidate_return_proxy": 0.006,
                    "baseline_return_proxy": 0.007,
                }
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
            "blocking_issues": [
                {
                    "issue_id": "recent_window",
                    "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
                }
            ],
            "window_splits": [
                {
                    "window_split_id": "recent_window",
                    "source_windows": ["slow_drawdown"],
                    "status": "WEAK",
                    "average_return_proxy": -0.1,
                    "worst_drawdown_proxy": -0.27,
                    "evaluation": "Worst drawdown proxy breaches conservative stress blocker.",
                    "recommended_action": "repair_or_revise_candidate_before_research_gate",
                }
            ],
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
