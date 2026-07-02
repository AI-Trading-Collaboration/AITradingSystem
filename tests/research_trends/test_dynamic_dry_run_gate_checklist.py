from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_2332_readiness_matrix,
    build_dynamic_dry_run_data_quality_precheck,
    build_dynamic_dry_run_gate_checklist,
    build_dynamic_dry_run_market_data_alignment_matrix,
    build_dynamic_dry_run_pit_caveat_acceptance_report,
    build_dynamic_dry_run_policy_compatibility_matrix,
    build_dynamic_dry_run_risk_cap_alignment_matrix,
    build_dynamic_dry_run_timestamp_alignment_matrix,
    build_dynamic_dry_run_wrapper_field_validation_matrix,
)


def test_gate_checklist_allows_2332_with_pit_caveat_when_no_blockers() -> None:
    rows = [dry_run_wrapper_row()]
    gate = build_dynamic_dry_run_gate_checklist(
        timestamp_summary={
            "next_task": "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat",
            "2331_allowed": True,
        },
        wrapper_validation={"validation_status": "PASS_WITH_WARNINGS"},
        wrapper_field_rows=build_dynamic_dry_run_wrapper_field_validation_matrix(rows),
        timestamp_alignment_rows=build_dynamic_dry_run_timestamp_alignment_matrix(rows),
        risk_cap_alignment_rows=build_dynamic_dry_run_risk_cap_alignment_matrix(
            wrapper_rows=rows,
            risk_cap_alignment={
                "alignment_readiness_status": "TIMESTAMP_ALIGNMENT_READY_WITH_WARNINGS",
                "risk_cap_trigger_series_available": True,
                "overlap_record_count": 1,
            },
        ),
        market_data_alignment_rows=build_dynamic_dry_run_market_data_alignment_matrix(
            wrapper_rows=rows,
            source_binding={
                "market_data_binding": {
                    "target_assets": ["QQQ"],
                    "coverage_start": "2023-01-06",
                    "coverage_end": "2026-06-18",
                }
            },
            static_dry_run={"summary": {}, "data_quality_report": {}},
        ),
        policy_compatibility_rows=build_dynamic_dry_run_policy_compatibility_matrix(
            wrapper_rows=rows,
            timestamp_remediation={"summary": {"known_at_policy": "NEXT_SESSION_DECISION_POLICY"}},
            source_binding={"summary": {"status": "SOURCE_BOUND_READY"}},
            simulation_policy={"summary": {"status": "POLICY_CONTEXT_READY"}},
            static_dry_run={"summary": {"status": "STATIC_DRY_RUN_READY"}},
        ),
        pit_acceptance=build_dynamic_dry_run_pit_caveat_acceptance_report(
            wrapper_rows=rows,
            pit_caveat={"pit_approximation_ready": True},
            known_at_report={"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
            risk_cap_alignment={},
        ),
        data_quality_precheck=build_dynamic_dry_run_data_quality_precheck(
            source_binding={"market_data_binding": {}},
            static_dry_run={"data_quality_report": {}},
        ),
    )
    readiness = build_dynamic_dry_run_2332_readiness_matrix(
        selected_source=rows[0],
        wrapper_validation={"validation_status": "PASS_WITH_WARNINGS"},
        gate_checklist=gate,
    )

    assert gate["gate_status"] == "DYNAMIC_DRY_RUN_READY_WITH_PIT_CAVEAT"
    assert gate["2332_allowed"] is True
    assert "pit_caveat_acceptance" in gate["warnings"]
    assert readiness["readiness_status"] == "DYNAMIC_DRY_RUN_READY_FOR_2332_WITH_PIT_CAVEAT"


def test_gate_checklist_blocks_when_wrapper_field_matrix_has_blocker() -> None:
    rows = [dry_run_wrapper_row(missing_target_exposure=True)]
    gate = build_dynamic_dry_run_gate_checklist(
        timestamp_summary={
            "next_task": "TRADING-2331_Dynamic_Target_Baseline_Dry_Run_Readiness_With_PIT_Caveat",
            "2331_allowed": True,
        },
        wrapper_validation={"validation_status": "PASS_WITH_WARNINGS"},
        wrapper_field_rows=build_dynamic_dry_run_wrapper_field_validation_matrix(rows),
        timestamp_alignment_rows=build_dynamic_dry_run_timestamp_alignment_matrix(rows),
        risk_cap_alignment_rows=[],
        market_data_alignment_rows=[],
        policy_compatibility_rows=[],
        pit_acceptance={"pit_caveat_accepted": True, "acceptance_status": "PASS"},
        data_quality_precheck={"precheck_status": "PASS_WITH_WARNINGS"},
    )

    assert gate["2332_allowed"] is False
    assert "wrapper_required_fields" in gate["blockers"]
