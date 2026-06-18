from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from test_return_to_research_reset import _write_decision_stage_inputs
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import executable_research_binding as binding_reports
from ai_trading_system.reports import next_research_cycle as next_cycle
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports import return_to_research_reset as return_reset

RUN_DATE = date(2026, 6, 17)


def test_next_research_cycle_builds_fail_closed_research_chain(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)

    payloads = next_cycle.build_next_research_cycle_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
    )

    assert payloads[next_cycle.INTAKE_REPORT_TYPE]["status"] == (
        "NEXT_RESEARCH_CYCLE_INTAKE_READY"
    )
    frozen = payloads[next_cycle.FROZEN_SPEC_REPORT_TYPE]
    assert frozen["summary"]["paper_shadow_eligible"] is False
    assert frozen["summary"]["market_regime"] == "ai_after_chatgpt"

    backfill = payloads[next_cycle.BACKFILL_REPORT_TYPE]
    assert backfill["status"] == next_cycle.CANDIDATE_BACKFILL_BLOCKED
    assert backfill["summary"]["data_quality_status"] == "PASS"
    assert backfill["summary"]["official_target_weights_generated"] is False
    assert backfill["missing_data_list"]

    gate = payloads[next_cycle.RESEARCH_GATE_REPORT_TYPE]
    assert gate["status"] == "NEEDS_MORE_EVIDENCE"
    assert gate["summary"]["paper_shadow_activation_allowed"] is False

    snapshot = payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE]
    assert snapshot["status"] == "NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    assert snapshot["summary"]["live_trading_allowed"] is False
    assert snapshot["summary"]["broker_order_allowed"] is False

    for report_type, payload in payloads.items():
        validation = next_cycle.validate_next_research_cycle_payload(
            payload,
            expected_report_type=report_type,
        )
        assert validation["status"] == "PASS", report_type
        assert payload["safety_boundary"]["paper_shadow_candidate_created"] is False
        assert payload["safety_boundary"]["official_target_weights_generated"] is False
        assert payload["production_effect"] == "none"


def test_next_candidate_backfill_runs_partial_static_binding_metrics(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    prices_path = _write_backfill_price_fixture(tmp_path)

    payload = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
        prices_path=prices_path,
    )

    assert payload["status"] == next_cycle.CANDIDATE_BACKFILL_PARTIAL
    assert payload["summary"]["real_metrics_generated"] is True
    assert payload["summary"]["return_proxy_available"] is True
    assert payload["summary"]["drawdown_proxy_available"] is True
    assert payload["summary"]["official_target_weights_generated"] is False
    assert payload["summary"]["broker_order_generated"] is False
    assert payload["summary"]["signal_completeness"] == "PARTIAL_STATIC_BINDING"
    assert "historical_dynamic_binding_unavailable" in {
        row["issue_id"] for row in payload["partial_reasons"]
    }
    assert all(row["return_proxy"] is not None for row in payload["backfill_windows"])

    validation = next_cycle.validate_next_research_cycle_payload(
        payload,
        expected_report_type=next_cycle.BACKFILL_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"


def test_next_candidate_stress_cost_benchmark_reviews_use_real_backfill_metrics(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    _write_cost_benchmark_sources(tmp_path)
    backfill = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
        prices_path=_write_backfill_price_fixture(tmp_path),
    )

    stress = next_cycle.build_next_candidate_stress_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    cost = next_cycle.build_next_candidate_cost_benchmark_review_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        backfill_payload=backfill,
    )

    assert stress["status"] == "MIXED_WITH_WARNINGS"
    assert stress["summary"]["source_backfill_status"] == next_cycle.CANDIDATE_BACKFILL_PARTIAL
    assert stress["summary"]["partial_static_proxy"] is True
    assert stress["summary"]["major_warning_count"] == len(next_cycle.REQUIRED_BACKFILL_WINDOWS)
    assert all(
        row["return_proxy"] is not None
        and row["drawdown_proxy"] is not None
        and row["scenario_status"] == "WARNING"
        for row in stress["scenario_reviews"]
    )

    assert cost["status"] == "COST_BENCHMARK_REVIEW_MIXED"
    assert cost["summary"]["source_backfill_status"] == next_cycle.CANDIDATE_BACKFILL_PARTIAL
    assert cost["summary"]["net_proxy_result"] == "AVAILABLE"
    assert cost["summary"]["cost_survival_status"] == "COST_SURVIVAL_WARNING"
    assert cost["summary"]["benchmark_relative_status"] == "BENCHMARK_MIXED"
    assert cost["cost_scenario_reviews"][0]["net_proxy_result"] is not None
    assert cost["benchmark_reviews"][0]["candidate_delta_vs_baseline"] is not None
    assert cost["summary"]["production_effect"] == "none"

    stress_validation = next_cycle.validate_next_research_cycle_payload(
        stress,
        expected_report_type=next_cycle.STRESS_REVIEW_REPORT_TYPE,
    )
    cost_validation = next_cycle.validate_next_research_cycle_payload(
        cost,
        expected_report_type=next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
    )
    assert stress_validation["status"] == "PASS"
    assert cost_validation["status"] == "PASS"

    stress_path = next_cycle.write_next_research_cycle_json(
        stress,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.STRESS_REVIEW_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    cost_path = next_cycle.write_next_research_cycle_json(
        cost,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    stress_validation_path = next_cycle.write_next_research_cycle_json(
        stress_validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.STRESS_REVIEW_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    cost_validation_path = next_cycle.write_next_research_cycle_json(
        cost_validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE}"
            f"{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": next_cycle.STRESS_REVIEW_REPORT_TYPE,
                "latest_artifact_path": str(stress_path),
            },
            {
                "report_id": f"{next_cycle.STRESS_REVIEW_REPORT_TYPE}"
                f"{next_cycle.VALIDATION_SUFFIX}",
                "latest_artifact_path": str(stress_validation_path),
            },
            {
                "report_id": next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE,
                "latest_artifact_path": str(cost_path),
            },
            {
                "report_id": f"{next_cycle.COST_BENCHMARK_REVIEW_REPORT_TYPE}"
                f"{next_cycle.VALIDATION_SUFFIX}",
                "latest_artifact_path": str(cost_validation_path),
            },
        ]
    }

    summary = reader_brief._next_candidate_stress_cost_benchmark_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["stress_status"] == "MIXED_WITH_WARNINGS"
    assert summary["cost_benchmark_status"] == "COST_BENCHMARK_REVIEW_MIXED"
    assert summary["stress_validation_status"] == "PASS"
    assert summary["cost_validation_status"] == "PASS"


def test_next_candidate_vs_returned_comparison_marks_repeated_failure(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    _write_cost_benchmark_sources(tmp_path, baseline_proxy=0.99)
    backfill = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
        prices_path=_write_backfill_price_fixture(tmp_path),
    )
    stress = next_cycle.build_next_candidate_stress_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    cost = next_cycle.build_next_candidate_cost_benchmark_review_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        backfill_payload=backfill,
    )

    comparison = next_cycle.build_next_candidate_vs_returned_comparison_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost,
    )

    rows = {row["metric_id"]: row for row in comparison["comparison_rows"]}
    assert comparison["status"] == "MIXED_VS_RETURNED_CANDIDATE"
    assert comparison["summary"]["real_metrics_available"] is True
    assert comparison["summary"]["benchmark_relative_status"] == "BENCHMARK_UNDERPERFORMS"
    assert comparison["summary"]["repeated_failure_mode_count"] == 1
    assert rows["benchmark_relative_behavior"]["comparison_status"] == (
        "REPEATS_FAILURE_MODE"
    )
    assert rows["drawdown_mismatch"]["comparison_status"] == "MIXED"
    assert rows["signal_robustness"]["comparison_status"] == "NO_IMPROVEMENT"
    assert comparison["reader_brief"]["blocking_issues"] != "none"

    validation = next_cycle.validate_next_research_cycle_payload(
        comparison,
        expected_report_type=next_cycle.VS_RETURNED_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"

    comparison_path = next_cycle.write_next_research_cycle_json(
        comparison,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.VS_RETURNED_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = next_cycle.write_next_research_cycle_json(
        validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.VS_RETURNED_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    summary = reader_brief._next_candidate_vs_returned_comparison_summary(
        {
            "reports": [
                {
                    "report_id": next_cycle.VS_RETURNED_REPORT_TYPE,
                    "latest_artifact_path": str(comparison_path),
                },
                {
                    "report_id": (
                        f"{next_cycle.VS_RETURNED_REPORT_TYPE}"
                        f"{next_cycle.VALIDATION_SUFFIX}"
                    ),
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["comparison_result"] == "MIXED_VS_RETURNED_CANDIDATE"
    assert summary["validation_status"] == "PASS"


def test_next_candidate_signal_window_rerun_uses_binding_and_backfill_metrics(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    backfill = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
        prices_path=_write_backfill_price_fixture(tmp_path),
    )
    signal_binding_path = binding_reports.default_executable_binding_json_path(
        binding_reports.SIGNAL_BINDING_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    signal_binding = json.loads(signal_binding_path.read_text(encoding="utf-8"))

    signal_review = next_cycle.build_next_candidate_signal_robustness_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
        signal_binding_payload=signal_binding,
    )
    window_review = next_cycle.build_next_candidate_window_sensitivity_payload(
        as_of=RUN_DATE,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )

    signal_checks = {
        row["check_id"]: row["status"] for row in signal_review["signal_quality_checks"]
    }
    assert signal_review["status"] == "SIGNAL_ROBUSTNESS_BLOCKED"
    assert signal_review["summary"]["source_signal_binding_status"] == (
        binding_reports.SIGNAL_BINDING_COMPLETE_WITH_WARNINGS
    )
    assert signal_review["summary"]["backfill_signal_completeness"] == (
        "PARTIAL_STATIC_BINDING"
    )
    assert signal_checks["partial_signal_series"] == "BLOCKING"
    assert signal_checks["market_coverage_gap"] == "BLOCKING"

    assert window_review["status"] == "WINDOW_FRAGILE"
    assert window_review["summary"]["overfit_risk"] == "HIGH"
    assert window_review["summary"]["partial_static_proxy_split_count"] == len(
        next_cycle.WINDOW_SENSITIVITY_SPLITS
    )
    assert all(
        row["average_return_proxy"] is not None
        and row["worst_drawdown_proxy"] is not None
        for row in window_review["window_splits"]
    )

    signal_validation = next_cycle.validate_next_research_cycle_payload(
        signal_review,
        expected_report_type=next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
    )
    window_validation = next_cycle.validate_next_research_cycle_payload(
        window_review,
        expected_report_type=next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
    )
    assert signal_validation["status"] == "PASS"
    assert window_validation["status"] == "PASS"

    signal_path = next_cycle.write_next_research_cycle_json(
        signal_review,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    window_path = next_cycle.write_next_research_cycle_json(
        window_review,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    signal_validation_path = next_cycle.write_next_research_cycle_json(
        signal_validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE}"
            f"{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    window_validation_path = next_cycle.write_next_research_cycle_json(
        window_validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE}"
            f"{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    summary = reader_brief._next_candidate_signal_window_sensitivity_summary(
        {
            "reports": [
                {
                    "report_id": next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE,
                    "latest_artifact_path": str(signal_path),
                },
                {
                    "report_id": next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE,
                    "latest_artifact_path": str(window_path),
                },
                {
                    "report_id": (
                        f"{next_cycle.SIGNAL_ROBUSTNESS_REPORT_TYPE}"
                        f"{next_cycle.VALIDATION_SUFFIX}"
                    ),
                    "latest_artifact_path": str(signal_validation_path),
                },
                {
                    "report_id": (
                        f"{next_cycle.WINDOW_SENSITIVITY_REPORT_TYPE}"
                        f"{next_cycle.VALIDATION_SUFFIX}"
                    ),
                    "latest_artifact_path": str(window_validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["signal_status"] == "SIGNAL_ROBUSTNESS_BLOCKED"
    assert summary["window_status"] == "WINDOW_FRAGILE"
    assert summary["signal_validation_status"] == "PASS"
    assert summary["window_validation_status"] == "PASS"
    assert summary["overfit_risk"] == "HIGH"


def test_next_candidate_research_gate_uses_real_metric_reviews_and_stays_safe(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    _write_cost_benchmark_sources(tmp_path, baseline_proxy=0.99)
    backfill = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
        prices_path=_write_backfill_price_fixture(tmp_path),
    )
    stress = next_cycle.build_next_candidate_stress_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    cost = next_cycle.build_next_candidate_cost_benchmark_review_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        backfill_payload=backfill,
    )
    comparison = next_cycle.build_next_candidate_vs_returned_comparison_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost,
    )
    signal = next_cycle.build_next_candidate_signal_robustness_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    window = next_cycle.build_next_candidate_window_sensitivity_payload(
        as_of=RUN_DATE,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    safety_path = binding_reports.default_executable_binding_json_path(
        binding_reports.SAFETY_AUDIT_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    safety = json.loads(safety_path.read_text(encoding="utf-8"))

    gate = next_cycle.build_next_candidate_research_gate_payload(
        as_of=RUN_DATE,
        frozen_spec_payload=frozen,
        safety_audit_payload=safety,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost,
        comparison_payload=comparison,
        signal_robustness_payload=signal,
        window_sensitivity_payload=window,
    )

    blocker_ids = {row["issue_id"] for row in gate["blocker_list"]}
    assert gate["status"] == "NEEDS_MORE_EVIDENCE"
    assert gate["summary"]["safety_audit_status"] == binding_reports.SAFETY_WARNING
    assert gate["summary"]["paper_shadow_activation_allowed"] is False
    assert gate["summary"]["official_target_weights_generated"] is False
    assert gate["summary"]["broker_order_allowed"] is False
    assert "signal_robustness_blocked" in blocker_ids
    assert "window_sensitivity_fragile" in blocker_ids
    assert "cost_benchmark_weak" in blocker_ids
    assert gate["strongest_positive_evidence"]
    assert gate["strongest_negative_evidence"]

    validation = next_cycle.validate_next_research_cycle_payload(
        gate,
        expected_report_type=next_cycle.RESEARCH_GATE_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"

    gate_path = next_cycle.write_next_research_cycle_json(
        gate,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.RESEARCH_GATE_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = next_cycle.write_next_research_cycle_json(
        validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.RESEARCH_GATE_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    summary = reader_brief._next_candidate_research_gate_summary(
        {
            "reports": [
                {
                    "report_id": next_cycle.RESEARCH_GATE_REPORT_TYPE,
                    "latest_artifact_path": str(gate_path),
                },
                {
                    "report_id": (
                        f"{next_cycle.RESEARCH_GATE_REPORT_TYPE}"
                        f"{next_cycle.VALIDATION_SUFFIX}"
                    ),
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["research_gate_decision"] == "NEEDS_MORE_EVIDENCE"
    assert summary["validation_status"] == "PASS"
    assert summary["paper_shadow_activation_allowed"] is False


def test_next_candidate_owner_packet_lists_all_manual_options_without_append(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    intake = next_cycle.build_next_research_cycle_intake_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    frozen = next_cycle.build_next_candidate_spec_frozen_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    _write_minimal_executable_binding_artifacts(reports_dir)
    _write_cost_benchmark_sources(tmp_path, baseline_proxy=0.99)
    backfill = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
        prices_path=_write_backfill_price_fixture(tmp_path),
    )
    stress = next_cycle.build_next_candidate_stress_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    cost = next_cycle.build_next_candidate_cost_benchmark_review_payload(
        as_of=RUN_DATE,
        project_root=tmp_path,
        backfill_payload=backfill,
    )
    comparison = next_cycle.build_next_candidate_vs_returned_comparison_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost,
    )
    signal = next_cycle.build_next_candidate_signal_robustness_review_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    window = next_cycle.build_next_candidate_window_sensitivity_payload(
        as_of=RUN_DATE,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    safety = json.loads(
        binding_reports.default_executable_binding_json_path(
            binding_reports.SAFETY_AUDIT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ).read_text(encoding="utf-8")
    )
    gate = next_cycle.build_next_candidate_research_gate_payload(
        as_of=RUN_DATE,
        frozen_spec_payload=frozen,
        safety_audit_payload=safety,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost,
        comparison_payload=comparison,
        signal_robustness_payload=signal,
        window_sensitivity_payload=window,
    )

    packet = next_cycle.build_next_candidate_owner_research_review_packet_payload(
        as_of=RUN_DATE,
        research_gate_payload=gate,
    )

    option_ids = {row["option_id"] for row in packet["owner_options"]}
    assert packet["status"] == "OWNER_RESEARCH_REVIEW_PACKET_READY"
    assert packet["summary"]["source_research_gate_decision"] == "NEEDS_MORE_EVIDENCE"
    assert packet["summary"]["option_count"] == 5
    assert option_ids == {
        "continue_research_validation",
        "revise_hypothesis",
        "return_to_hypothesis_backlog",
        "reject_research_candidate",
        "hold_for_more_data",
    }
    assert packet["summary"]["owner_decision_appended"] is False
    assert packet["summary"]["paper_shadow_activation_allowed"] is False
    assert packet["summary"]["official_target_weights_generated"] is False
    assert all(
        row["evidence_required"] and row["risks"] and row["next_action"]
        for row in packet["owner_options"]
    )

    validation = next_cycle.validate_next_research_cycle_payload(
        packet,
        expected_report_type=next_cycle.OWNER_REVIEW_PACKET_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"

    packet_path = next_cycle.write_next_research_cycle_json(
        packet,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.OWNER_REVIEW_PACKET_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = next_cycle.write_next_research_cycle_json(
        validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.OWNER_REVIEW_PACKET_REPORT_TYPE}"
            f"{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    summary = reader_brief._next_candidate_owner_research_review_packet_summary(
        {
            "reports": [
                {
                    "report_id": next_cycle.OWNER_REVIEW_PACKET_REPORT_TYPE,
                    "latest_artifact_path": str(packet_path),
                },
                {
                    "report_id": (
                        f"{next_cycle.OWNER_REVIEW_PACKET_REPORT_TYPE}"
                        f"{next_cycle.VALIDATION_SUFFIX}"
                    ),
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "OWNER_RESEARCH_REVIEW_PACKET_READY"
    assert summary["validation_status"] == "PASS"
    assert summary["owner_decision_appended"] is False


def test_next_research_cycle_cli_writes_intake_freeze_and_validations(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    runner = CliRunner()

    intake_result = runner.invoke(
        app,
        [
            "reports",
            "next-research-cycle-intake",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert intake_result.exit_code == 0, intake_result.output

    freeze_result = runner.invoke(
        app,
        [
            "reports",
            "next-candidate-spec-freeze",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert freeze_result.exit_code == 0, freeze_result.output

    frozen_path = next_cycle.default_next_research_cycle_json_path(
        next_cycle.FROZEN_SPEC_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    frozen_payload = json.loads(frozen_path.read_text(encoding="utf-8"))
    backfill_payload = next_cycle.build_next_candidate_backfill_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen_payload,
        data_quality_gate={"status": "PASS", "passed": True, "report_path": "dq.md"},
    )
    next_cycle.write_next_research_cycle_json(
        backfill_payload,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.BACKFILL_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    next_cycle.write_next_research_cycle_markdown(
        backfill_payload,
        next_cycle.default_next_research_cycle_markdown_path(
            next_cycle.BACKFILL_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-next-candidate-backfill",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    for report_type in (
        next_cycle.INTAKE_REPORT_TYPE,
        next_cycle.FROZEN_SPEC_REPORT_TYPE,
        next_cycle.BACKFILL_REPORT_TYPE,
        f"{next_cycle.BACKFILL_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
    ):
        assert next_cycle.default_next_research_cycle_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        ).exists()


def test_reader_brief_summarizes_next_research_cycle_snapshot(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_return_to_research_inputs(reports_dir, tmp_path)
    payloads = next_cycle.build_next_research_cycle_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
    )
    snapshot = payloads[next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE]
    validation = next_cycle.validate_next_research_cycle_payload(
        snapshot,
        expected_report_type=next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
    )
    snapshot_path = next_cycle.write_next_research_cycle_json(
        snapshot,
        next_cycle.default_next_research_cycle_json_path(
            next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = next_cycle.write_next_research_cycle_json(
        validation,
        next_cycle.default_next_research_cycle_json_path(
            f"{next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE}{next_cycle.VALIDATION_SUFFIX}",
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
                "latest_artifact_path": str(snapshot_path),
            },
            {
                "report_id": (
                    f"{next_cycle.CYCLE_SNAPSHOT_REPORT_TYPE}"
                    f"{next_cycle.VALIDATION_SUFFIX}"
                ),
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._next_research_cycle_snapshot_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    assert summary["research_gate_decision"] == "NEEDS_MORE_EVIDENCE"
    assert summary["validation_status"] == "PASS"
    assert summary["paper_shadow_activation_allowed"] is False
    assert summary["live_trading_allowed"] is False
    assert summary["official_target_weights_generated"] is False
    assert summary["broker_order_allowed"] is False


def _write_minimal_executable_binding_artifacts(reports_dir: Path) -> None:
    signal_payload = {
        "schema_version": 1,
        "report_type": binding_reports.SIGNAL_BINDING_REPORT_TYPE,
        "as_of": RUN_DATE.isoformat(),
        "status": binding_reports.SIGNAL_BINDING_COMPLETE_WITH_WARNINGS,
        "production_effect": "none",
        "manual_review_only": True,
        "research_only": True,
        "requested_date_range": "2023-01-03..2025-04-30",
        "candidate_signal_series": [
            {
                "signal_date": RUN_DATE.isoformat(),
                "signal_score": 0.7,
                "risk_state": "risk_on",
                "rotation_state": "increase_ai_risk",
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "production_effect": "none",
            }
        ],
        "summary": {
            "candidate_id": "median_plus_regime_mismatch_filter_research_redesign_v2",
            "research_only": True,
            "official_target_weights": False,
            "production_effect": "none",
        },
        "safety_boundary": _test_safety_boundary(),
    }
    signal_validation = _validation_payload(
        binding_reports.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
        binding_reports.SIGNAL_BINDING_REPORT_TYPE,
    )
    weight_payload = {
        "schema_version": 1,
        "report_type": binding_reports.WEIGHT_BINDING_REPORT_TYPE,
        "as_of": RUN_DATE.isoformat(),
        "status": binding_reports.WEIGHT_BINDING_COMPLETE_WITH_WARNINGS,
        "production_effect": "none",
        "manual_review_only": True,
        "research_only": True,
        "requested_date_range": "2023-01-03..2025-04-30",
        "hypothetical_research_weight": {
            "weight_type": "hypothetical_research_weight",
            "weights": {"QQQ": 0.5, "SPY": 0.5, "CASH": 0.0},
            "research_only": True,
            "manual_review_only": True,
            "official_target_weights": False,
            "not_official_target_weights": True,
            "production_effect": "none",
        },
        "previous_hypothetical_weight": {
            "weight_type": "hypothetical_research_weight",
            "weights": {"QQQ": 0.0, "SPY": 1.0, "CASH": 0.0},
            "research_only": True,
            "manual_review_only": True,
            "official_target_weights": False,
            "not_official_target_weights": True,
            "production_effect": "none",
        },
        "hypothetical_research_weight_series": [
            {
                "signal_date": RUN_DATE.isoformat(),
                "risk_state": "risk_on",
                "rotation_state": "increase_ai_risk",
                "constraint_hit": [],
                "hypothetical_research_weight": {
                    "weights": {"QQQ": 0.5, "SPY": 0.5, "CASH": 0.0}
                },
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "production_effect": "none",
            }
        ],
        "summary": {
            "candidate_id": "median_plus_regime_mismatch_filter_research_redesign_v2",
            "research_only": True,
            "official_target_weights": False,
            "not_official_target_weights": True,
            "production_effect": "none",
        },
        "safety_boundary": _test_safety_boundary(),
    }
    weight_validation = _validation_payload(
        binding_reports.WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
        binding_reports.WEIGHT_BINDING_REPORT_TYPE,
    )
    safety_payload = {
        "schema_version": 1,
        "report_type": binding_reports.SAFETY_AUDIT_REPORT_TYPE,
        "as_of": RUN_DATE.isoformat(),
        "status": binding_reports.SAFETY_WARNING,
        "production_effect": "none",
        "manual_review_only": True,
        "research_only": True,
        "summary": {
            "acceptable_warning": True,
            "failed_artifact_check_count": 0,
            "blocking_static_finding_count": 0,
            "production_effect": "none",
        },
        "safety_boundary": _test_safety_boundary(),
    }
    safety_validation = _validation_payload(
        binding_reports.SAFETY_AUDIT_VALIDATION_REPORT_TYPE,
        binding_reports.SAFETY_AUDIT_REPORT_TYPE,
    )
    for report_type, payload in {
        binding_reports.SIGNAL_BINDING_REPORT_TYPE: signal_payload,
        binding_reports.SIGNAL_BINDING_VALIDATION_REPORT_TYPE: signal_validation,
        binding_reports.WEIGHT_BINDING_REPORT_TYPE: weight_payload,
        binding_reports.WEIGHT_BINDING_VALIDATION_REPORT_TYPE: weight_validation,
        binding_reports.SAFETY_AUDIT_REPORT_TYPE: safety_payload,
        binding_reports.SAFETY_AUDIT_VALIDATION_REPORT_TYPE: safety_validation,
    }.items():
        path = binding_reports.default_executable_binding_json_path(
            report_type,
            reports_dir,
            RUN_DATE,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_backfill_price_fixture(tmp_path: Path) -> Path:
    windows = [
        ("2023-01-03", "2023-07-31"),
        ("2024-07-10", "2024-08-09"),
        ("2025-01-02", "2025-04-30"),
        ("2023-08-01", "2023-11-15"),
        ("2024-03-08", "2024-04-19"),
        ("2023-09-01", "2023-10-31"),
    ]
    rows = ["date,symbol,close,adj_close"]
    for index, (start, end) in enumerate(windows, start=1):
        rows.append(f"{start},QQQ,{100 + index},{100 + index}")
        rows.append(f"{end},QQQ,{104 + index},{104 + index}")
        rows.append(f"{start},SPY,{90 + index},{90 + index}")
        rows.append(f"{end},SPY,{92 + index},{92 + index}")
    path = tmp_path / "prices_daily.csv"
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _write_cost_benchmark_sources(
    project_root: Path,
    *,
    baseline_proxy: float = 0.001,
) -> None:
    cost_path = (
        project_root
        / "reports"
        / "etf_portfolio"
        / "dynamic_v3_rescue"
        / "cost_sensitivity_review"
        / "fixture"
        / "cost_sensitivity_review.json"
    )
    cost_path.parent.mkdir(parents=True, exist_ok=True)
    cost_path.write_text(
        json.dumps(
            {
                "meaningful_improvement_threshold": 0.001,
                "policy": {
                    "scenarios": [
                        {
                            "scenario_id": "medium",
                            "label": "Medium Cost",
                            "total_cost_bps": 10.0,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    benchmark_path = (
        project_root
        / "reports"
        / "etf_portfolio"
        / "dynamic_v3_rescue"
        / "benchmark_baseline_control"
        / "fixture"
        / "benchmark_baseline_control_pack.json"
    )
    benchmark_path.parent.mkdir(parents=True, exist_ok=True)
    benchmark_path.write_text(
        json.dumps(
            {
                "minimum_outperformance_threshold": 0.001,
                "baselines": [
                    {
                        "baseline_id": "low_return_static_baseline",
                        "baseline_net_performance_proxy": baseline_proxy,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _validation_payload(report_type: str, source_report_type: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": report_type,
        "as_of": RUN_DATE.isoformat(),
        "status": "PASS",
        "production_effect": "none",
        "manual_review_only": True,
        "research_only": True,
        "summary": {
            "validation_status": "PASS",
            "source_report_type": source_report_type,
            "production_effect": "none",
        },
        "safety_boundary": _test_safety_boundary(),
    }


def _test_safety_boundary() -> dict[str, object]:
    return {
        "paper_shadow_candidate_created": False,
        "paper_shadow_activation_allowed": False,
        "normal_paper_shadow_resumed": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_decision_appended": False,
        "production_state_mutated": False,
        "production_effect": "none",
    }


def _write_return_to_research_inputs(reports_dir: Path, project_root: Path) -> None:
    decision_dir = project_root / "docs" / "decisions"
    log_path = project_root / "data" / "governance" / "owner_decision_audit_log.jsonl"
    _write_decision_stage_inputs(reports_dir, project_root)
    payloads = return_reset.build_return_to_research_reset_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        decision_source_dir=decision_dir,
        owner_decision_log_path=log_path,
        append_owner_decision=True,
    )
    for report_type, payload in payloads.items():
        return_reset.write_return_to_research_json(
            payload,
            return_reset.default_return_to_research_json_path(
                report_type,
                reports_dir,
                RUN_DATE,
            ),
        )
