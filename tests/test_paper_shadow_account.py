from __future__ import annotations

from dynamic_v3_system_target_helpers import write_paper_shadow_config

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_initialization_is_paper_only(tmp_path) -> None:
    config_path = write_paper_shadow_config(tmp_path)

    result = system_target.init_paper_shadow_account(
        config_path=config_path,
        output_dir=tmp_path / "paper_shadow",
        model_target_dir=tmp_path / "model_target",
    )

    state = result["state"]
    assert state["state_status"] == "INITIALIZED"
    assert state["paper_shadow_only"] is True
    assert state["not_official_target_weights"] is True
    assert state["broker_action_taken"] is False
    assert len(state["method_states"]) == len(system_target.TARGET_METHODS)
    assert {row["target_method"] for row in state["method_states"]} == set(
        system_target.TARGET_METHODS
    )

    validation = system_target.validate_paper_shadow_artifact(
        paper_shadow_id=result["paper_shadow_id"],
        output_dir=tmp_path / "paper_shadow",
    )
    assert validation["status"] == "PASS"
