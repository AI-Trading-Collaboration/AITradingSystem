from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
    apply_evidence_gate_policy,
    run_evidence_summary,
    validate_evidence_gate_policy,
)


def test_evidence_gate_policy_validates_and_applies_manual_review(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    run_evidence_summary(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "evidence_summary",
        candidate_attribution_dir=tmp_path / "missing_attribution",
        overfit_dir=tmp_path / "missing_overfit",
        data_provenance_dir=tmp_path / "missing_provenance",
        window_audit_dir=tmp_path / "missing_window",
    )

    validation = validate_evidence_gate_policy(policy_path=DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH)
    applied = apply_evidence_gate_policy(
        sweep_id=sweep["sweep_id"],
        policy_path=DEFAULT_EVIDENCE_GATE_POLICY_CONFIG_PATH,
        sweep_output_dir=sweep["sweep_output_dir"],
        evidence_summary_dir=tmp_path / "evidence_summary",
        regime_coverage_dir=tmp_path / "missing_regime",
        output_dir=tmp_path / "gate_policy",
    )

    assert validation["status"] == "PASS"
    assert "DATA_QUALITY_FAIL" in validation["hard_fail"]
    assert "ATTRIBUTION_INCOMPLETE" in validation["manual_review_allowed"]
    assert applied["manifest"]["observe_only_candidates"] > 0
    assert applied["manifest"]["production_candidate_generated"] is False
