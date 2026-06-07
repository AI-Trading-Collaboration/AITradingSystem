from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    apply_evidence_gate_policy,
    run_candidate_recovery,
    run_evidence_diagnosis,
    run_evidence_summary,
    run_gate_impact,
    update_research_decision,
    validate_research_decision_update_artifact,
)


def test_research_decision_update_reports_go_no_go(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_summary = run_evidence_summary(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "evidence_summary",
        candidate_attribution_dir=tmp_path / "missing_attribution",
        overfit_dir=tmp_path / "missing_overfit",
        data_provenance_dir=tmp_path / "missing_provenance",
        window_audit_dir=tmp_path / "missing_window",
    )
    diagnosis = run_evidence_diagnosis(
        sweep_id=sweep["sweep_id"],
        summary_id=evidence_summary["summary_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        evidence_summary_dir=tmp_path / "evidence_summary",
        regime_coverage_dir=tmp_path / "missing_regime",
        output_dir=tmp_path / "evidence_diagnosis",
    )
    impact = run_gate_impact(
        diagnosis_id=diagnosis["diagnosis_id"],
        diagnosis_dir=tmp_path / "evidence_diagnosis",
        output_dir=tmp_path / "gate_impact",
    )
    policy = apply_evidence_gate_policy(
        sweep_id=sweep["sweep_id"],
        policy_path=DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
        sweep_output_dir=sweep["sweep_output_dir"],
        evidence_summary_dir=tmp_path / "evidence_summary",
        regime_coverage_dir=tmp_path / "missing_regime",
        output_dir=tmp_path / "gate_policy",
    )
    recovery = run_candidate_recovery(
        sweep_id=sweep["sweep_id"],
        policy_run_id=policy["policy_run_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        gate_policy_dir=tmp_path / "gate_policy",
        output_dir=tmp_path / "candidate_recovery",
    )

    decision = update_research_decision(
        sweep_id=sweep["sweep_id"],
        diagnosis_id=diagnosis["diagnosis_id"],
        impact_id=impact["impact_id"],
        recovery_id=recovery["recovery_id"],
        diagnosis_dir=tmp_path / "evidence_diagnosis",
        gate_impact_dir=tmp_path / "gate_impact",
        recovery_dir=tmp_path / "candidate_recovery",
        overnight_readiness_dir=tmp_path / "missing_readiness",
        output_dir=tmp_path / "research_decision_update",
    )

    assert decision["go_no_go_matrix"]["go_no_go"] == "NO_GO"
    assert decision["go_no_go_matrix"]["required_owner_approval"] is True
    assert (
        validate_research_decision_update_artifact(
            decision_update_id=decision["decision_update_id"],
            output_dir=tmp_path / "research_decision_update",
        )["status"]
        == "PASS"
    )
