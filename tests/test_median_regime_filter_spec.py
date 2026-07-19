from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_median_regime_filter_spec_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_median_regime_filter_spec_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_median_regime_filter_spec_fixture(tmp_path, monkeypatch)
    spec = fixture["median_regime_filter_spec"]
    validation = readiness.validate_median_regime_filter_spec_artifact(
        spec_id=spec["spec_id"],
        output_dir=tmp_path / "median_regime_filter_spec",
    )
    assert validation["status"] == "PASS"
    spec_yaml = spec["median_regime_filter_spec"]
    assert spec_yaml["method"]["base_method"] == "median_target_weights"
    assert spec_yaml["filters"]["risk_off"]["block_risk_increase"] is True
    assert spec_yaml["filters"]["strong_recovery"]["allow_risk_restore"] is True
    contract = spec["median_regime_filter_contract"]
    assert contract["contract_status"] == "RESEARCH_SPEC_ONLY"
    assert contract["observed_performance_available"] is False
    assert contract["requires_new_external_data"] is True
    assert_research_safe(spec)
