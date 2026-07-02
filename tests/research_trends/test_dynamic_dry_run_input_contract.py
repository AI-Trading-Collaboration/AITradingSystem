from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_data_quality_precheck,
    build_dynamic_dry_run_input_contract,
    build_dynamic_dry_run_pit_caveat_acceptance_report,
)


def test_input_contract_requires_2332_data_quality_gate_and_wrapper_fields() -> None:
    rows = [dry_run_wrapper_row()]
    pit_acceptance = build_dynamic_dry_run_pit_caveat_acceptance_report(
        wrapper_rows=rows,
        pit_caveat={"pit_approximation_ready": True},
        known_at_report={"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
        risk_cap_alignment={},
    )
    data_quality = build_dynamic_dry_run_data_quality_precheck(
        source_binding={
            "market_data_binding": {
                "data_quality_status": "PASS_WITH_WARNINGS",
                "data_quality_gate": {"warning_count": 1},
            }
        },
        static_dry_run={
            "data_quality_report": {
                "data_quality_status": "PASS_WITH_WARNINGS",
                "warning_count": 1,
            }
        },
    )

    contract = build_dynamic_dry_run_input_contract(
        wrapper_rows=rows,
        selected_source=rows[0],
        pit_acceptance=pit_acceptance,
        data_quality_precheck=data_quality,
    )

    assert contract["contract_status"] == "READY_WITH_WARNINGS"
    assert contract["wrapper_record_count"] == 1
    assert "target_exposure" in contract["required_wrapper_fields"]
    assert contract["data_quality_boundary"]["2332_data_quality_gate_required"] is True
    assert contract["promotion_allowed"] is False
