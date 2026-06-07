from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep, write_regime_price_cache

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_regime_coverage,
    validate_regime_coverage_artifact,
)


def test_regime_coverage_marks_tech_semiconductor_relevance(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)
    prices_path = write_regime_price_cache(tmp_path)

    result = run_regime_coverage(
        sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        prices_path=prices_path,
        output_dir=tmp_path / "regime_coverage",
    )

    manifest = result["manifest"]
    assert manifest["coverage_status"] in {"PASS", "PASS_WITH_WARNINGS", "FAIL"}
    assert manifest["tech_semiconductor_relevance"] in {"HIGH", "MEDIUM", "LOW"}
    assert manifest["ai_bull_market_overfit_risk"] in {"LOW", "REVIEW_REQUIRED", "HIGH"}
    assert (
        validate_regime_coverage_artifact(
            coverage_id=result["coverage_id"],
            output_dir=tmp_path / "regime_coverage",
        )["status"]
        == "PASS"
    )
