from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_signal_vs_parameter_attribution_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_signal_vs_parameter_attribution_selects_research_shift(tmp_path) -> None:
    fixture = run_signal_vs_parameter_attribution_fixture(tmp_path)
    attribution = fixture["signal_vs_parameter"]

    assert attribution["manifest"]["status"] == "PASS"
    assert attribution["failure_source_attribution"]["failure_source"]
    assert attribution["recommended_research_shift"]["recommended_shift"]
    assert attribution["manifest"]["broker_action_allowed"] is False
    assert "Signal vs Parameter Attribution" in attribution["reader_brief_section"]

    validation = weight_search.validate_signal_vs_parameter_attribution_artifact(
        attribution_id=attribution["signal_vs_parameter_id"],
        output_dir=tmp_path / "signal_vs_parameter_attribution",
    )
    assert validation["status"] == "PASS"
