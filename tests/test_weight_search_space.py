from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_weight_search_space_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_weight_search_space_config_and_artifact_are_auditable(tmp_path) -> None:
    fixture = run_weight_search_space_fixture(tmp_path)
    search_space = fixture["search_space"]

    assert search_space["manifest"]["status"] == "PASS"
    assert search_space["manifest"]["market_regime"] == "ai_after_chatgpt"
    assert len(search_space["manifest"]["families"]) >= 8
    assert search_space["manifest"]["broker_action_allowed"] is False
    assert search_space["manifest"]["production_effect"] == "none"

    validation = weight_search.validate_weight_search_space_artifact(
        search_space_id=search_space["search_space_id"],
        output_dir=tmp_path / "weight_search_space",
    )
    assert validation["status"] == "PASS"
