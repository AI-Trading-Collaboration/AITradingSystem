from __future__ import annotations

from ai_trading_system.channel_specific_first_layer_v4 import (
    validate_channel_specific_v4_start,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

FREEZE_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_first_layer_v4_freeze_contract.yaml"
)


def test_channel_v4_requires_reopen_gate_and_owner_approval() -> None:
    result = validate_channel_specific_v4_start(
        gate_decision={"status": "FIRST_LAYER_REOPEN_DENIED", "summary": {}},
        freeze_contract=_freeze_contract(),
        owner_approval=False,
    )

    assert result["start_allowed"] is False
    assert "reopen_gate_not_allowed" in result["blockers"]
    assert "owner_approval_missing" in result["blockers"]
    assert result["promotion_allowed"] is False
    assert result["broker_action"] == "none"


def test_channel_v4_can_start_only_after_gate_and_owner_but_still_no_promotion() -> None:
    result = validate_channel_specific_v4_start(
        gate_decision={"status": "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4", "summary": {}},
        freeze_contract=_freeze_contract(),
        owner_approval=True,
    )

    assert result["start_allowed"] is True
    assert result["candidate_count"] == 0
    assert result["promotion_allowed"] is False
    assert result["paper_shadow_allowed"] is False
    assert result["production_allowed"] is False
    assert result["broker_action"] == "none"


def _freeze_contract() -> dict[str, object]:
    raw = safe_load_yaml_path(FREEZE_CONTRACT_PATH)
    assert isinstance(raw, dict)
    return raw
