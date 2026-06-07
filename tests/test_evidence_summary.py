from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep, write_candidate_evidence

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_evidence_summary,
    validate_evidence_summary_artifact,
)


def test_evidence_summary_generates_candidate_matrix(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_dirs = write_candidate_evidence(tmp_path, sweep)

    result = run_evidence_summary(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "evidence_summary",
        **evidence_dirs,
    )

    manifest = result["manifest"]
    assert manifest["status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert manifest["candidate_count"] > 0
    assert manifest["usable_for_research_count"] > 0
    assert (
        validate_evidence_summary_artifact(
            summary_id=result["summary_id"],
            output_dir=tmp_path / "evidence_summary",
        )["status"]
        == "PASS"
    )
