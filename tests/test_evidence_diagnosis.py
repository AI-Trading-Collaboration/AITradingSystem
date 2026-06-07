from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_evidence_diagnosis,
    run_evidence_summary,
    validate_evidence_diagnosis_artifact,
)


def test_evidence_diagnosis_classifies_soft_and_warning_reasons(tmp_path):
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

    assert diagnosis["manifest"]["candidate_count"] > 0
    assert diagnosis["manifest"]["soft_blocked_candidates"] > 0
    assert diagnosis["manifest"]["hard_blocked_candidates"] == 0
    assert "ATTRIBUTION_INCOMPLETE" in {
        row["reason"] for row in diagnosis["blocking_summary"]["blocking_reasons"]
    }
    assert (
        validate_evidence_diagnosis_artifact(
            diagnosis_id=diagnosis["diagnosis_id"],
            output_dir=tmp_path / "evidence_diagnosis",
        )["status"]
        == "PASS"
    )
