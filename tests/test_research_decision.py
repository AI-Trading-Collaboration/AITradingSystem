from __future__ import annotations

from dynamic_v3_research_helpers import (
    prepared_real_like_sweep,
    write_candidate_evidence,
    write_regime_price_cache,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_observe_pool,
    run_evidence_summary,
    run_interpretation_pack,
    run_overnight_readiness,
    run_regime_coverage,
    run_research_decision,
    validate_research_decision_artifact,
)


def test_research_decision_pack_links_stage_outputs(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_dirs = write_candidate_evidence(tmp_path, sweep)
    evidence_dir = tmp_path / "evidence_summary"
    regime_dir = tmp_path / "regime_coverage"
    interpretation_dir = tmp_path / "interpretation_pack"
    observe_dir = tmp_path / "observe_pool"
    readiness_dir = tmp_path / "overnight_readiness"

    run_evidence_summary(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=evidence_dir,
        **evidence_dirs,
    )
    run_regime_coverage(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        prices_path=write_regime_price_cache(tmp_path),
        output_dir=regime_dir,
    )
    run_interpretation_pack(
        sweep_id=sweep["sweep_id"],
        top_n=3,
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=interpretation_dir,
        regime_coverage_dir=regime_dir,
        **evidence_dirs,
    )
    build_observe_pool(
        sweep_id=sweep["sweep_id"],
        top_n=5,
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=observe_dir,
        regime_coverage_dir=regime_dir,
        **evidence_dirs,
    )
    run_overnight_readiness(
        source_sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=readiness_dir,
    )

    result = run_research_decision(
        sweep_id=sweep["sweep_id"],
        output_dir=tmp_path / "research_decision",
        evidence_summary_dir=evidence_dir,
        regime_coverage_dir=regime_dir,
        interpretation_pack_dir=interpretation_dir,
        observe_pool_dir=observe_dir,
        overnight_readiness_dir=readiness_dir,
    )

    assert result["manifest"]["status"] == "PASS"
    assert result["recommendation"]["recommendation"] in {
        "run_overnight_real",
        "rerun_medium_real",
        "narrow_parameter_space",
        "expand_stress_windows",
        "fix_evidence_gaps",
        "manual_review_observe_pool",
    }
    assert (
        validate_research_decision_artifact(
            decision_id=result["decision_id"],
            output_dir=tmp_path / "research_decision",
        )["status"]
        == "PASS"
    )
