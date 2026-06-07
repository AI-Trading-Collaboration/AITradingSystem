from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_evidence_diagnosis,
    run_evidence_summary,
    run_gate_impact,
    validate_gate_impact_artifact,
)


def test_gate_impact_simulates_recovery_without_mutating_sweep(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    before = (sweep["sweep_dir"] / "candidate_results.jsonl").read_text(encoding="utf-8")
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
    scenarios = {row["scenario"]: row for row in impact["scenarios"]}

    assert scenarios["current_rules"]["observe_candidates"] == 0
    assert scenarios["true_hard_failures_only"]["observe_candidates"] > 0
    assert (sweep["sweep_dir"] / "candidate_results.jsonl").read_text(encoding="utf-8") == before
    assert (
        validate_gate_impact_artifact(
            impact_id=impact["impact_id"],
            output_dir=tmp_path / "gate_impact",
        )["status"]
        == "PASS"
    )
