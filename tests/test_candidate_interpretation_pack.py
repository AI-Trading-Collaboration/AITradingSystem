from __future__ import annotations

from dynamic_v3_research_helpers import (
    prepared_real_like_sweep,
    write_candidate_evidence,
    write_regime_price_cache,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_interpretation_pack,
    run_regime_coverage,
    validate_interpretation_pack_artifact,
)


def test_candidate_interpretation_pack_writes_reports(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    evidence_dirs = write_candidate_evidence(tmp_path, sweep)
    run_regime_coverage(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        prices_path=write_regime_price_cache(tmp_path),
        output_dir=tmp_path / "regime_coverage",
    )

    result = run_interpretation_pack(
        sweep_id=sweep["sweep_id"],
        top_n=3,
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "interpretation_pack",
        regime_coverage_dir=tmp_path / "regime_coverage",
        **evidence_dirs,
    )

    assert result["manifest"]["candidate_count"] == 3
    assert result["manifest"]["incomplete_weight_path_count"] == 0
    assert (
        validate_interpretation_pack_artifact(
            pack_id=result["pack_id"],
            output_dir=tmp_path / "interpretation_pack",
        )["status"]
        == "PASS"
    )
