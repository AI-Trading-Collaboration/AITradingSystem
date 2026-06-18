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
            "summary": {"candidate_id": "candidate_v1", "signal_row_count": 1},
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
            "backfill_windows": [
                {
                    "window_id": "normal_market_regime",
                    "start": "2023-01-03",
                    "end": "2023-07-31",
                    "backfill_window_status": "PARTIAL",
                    "signal_completeness": "PARTIAL_STATIC_BINDING",
                    "missing_data_list": ["historical_signal_series:normal_market_regime"],
                    "return_proxy": 0.1,
                    "drawdown_proxy": -0.05,
                    "turnover": 0.85,
                }
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
            "signal_quality_checks": [
                {
                    "check_id": "partial_signal_series",
                    "status": "BLOCKING",
                    "evidence": "signal_row_count=1",
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
