from __future__ import annotations

from dynamic_v3_research_helpers import (
    prepared_real_like_sweep,
    write_candidate_evidence,
    write_regime_price_cache,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    build_observe_pool,
    run_regime_coverage,
    validate_observe_pool_artifact,
)


def test_observe_pool_filters_real_candidates(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_dirs = write_candidate_evidence(tmp_path, sweep)
    run_regime_coverage(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        prices_path=write_regime_price_cache(tmp_path),
        output_dir=tmp_path / "regime_coverage",
    )

    result = build_observe_pool(
        sweep_id=sweep["sweep_id"],
        top_n=5,
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "observe_pool",
        regime_coverage_dir=tmp_path / "regime_coverage",
        **evidence_dirs,
    )

    assert result["manifest"]["observe_candidate_count"] > 0
    assert all(row["overfit_status"] != "HIGH_RISK" for row in result["candidates"])
    assert (
        validate_observe_pool_artifact(
            pool_id=result["pool_id"],
            output_dir=tmp_path / "observe_pool",
        )["status"]
        == "PASS"
    )
