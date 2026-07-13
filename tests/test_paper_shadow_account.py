from __future__ import annotations

import json

import pytest
from dynamic_v3_system_target_helpers import (
    build_model_target_fixture,
    write_paper_shadow_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_paper_shadow_initialization_is_paper_only(tmp_path) -> None:
    build_model_target_fixture(tmp_path)
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


def test_paper_shadow_rejects_tampered_model_target(tmp_path) -> None:
    target = build_model_target_fixture(tmp_path)
    manifest_path = target["target_dir"] / "model_target_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "FAIL"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="selected model target validation failed"):
        system_target.init_paper_shadow_account(
            config_path=write_paper_shadow_config(tmp_path),
            output_dir=tmp_path / "paper_shadow",
            model_target_dir=tmp_path / "model_target",
        )
