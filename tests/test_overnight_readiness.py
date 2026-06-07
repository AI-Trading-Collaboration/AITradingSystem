from __future__ import annotations

from dynamic_v3_research_helpers import prepared_real_like_sweep

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    run_overnight_readiness,
    validate_overnight_readiness_artifact,
)


def test_overnight_readiness_estimates_runtime_without_starting_run(tmp_path):
    sweep = prepared_real_like_sweep(tmp_path)

    result = run_overnight_readiness(
        source_sweep_id=sweep["sweep_id"],
        sweep_output_dir=sweep["sweep_output_dir"],
        output_dir=tmp_path / "overnight_readiness",
    )

    manifest = result["manifest"]
    assert manifest["overnight_readiness"] in {"READY", "READY_WITH_WARNINGS", "NOT_READY"}
    assert manifest["projected_candidate_count"] == 5000
    assert manifest["production_state_mutated"] is False
    assert (
        validate_overnight_readiness_artifact(
            readiness_id=result["readiness_id"],
            output_dir=tmp_path / "overnight_readiness",
        )["status"]
        == "PASS"
    )
