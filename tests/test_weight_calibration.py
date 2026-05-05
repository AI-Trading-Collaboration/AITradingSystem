from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.weight_calibration import (
    CalibrationOverlay,
    WeightProfile,
    apply_calibration_overlays,
    load_calibration_overlays,
    load_weight_profile,
)


def test_default_weight_profile_loads_current_scoring_modules() -> None:
    profile = load_weight_profile()

    assert profile.status == "production"
    assert profile.metadata["confidence_delta_unit"] == "score_points"
    assert set(profile.base_weights) == {
        "trend",
        "fundamentals",
        "macro_liquidity",
        "risk_sentiment",
        "valuation",
        "policy_geopolitics",
    }
    assert sum(profile.base_weights.values()) == pytest.approx(1.0)


def test_weight_profile_rejects_base_weights_that_do_not_sum_to_one() -> None:
    with pytest.raises(ValidationError, match="base_weights must sum to 1.0"):
        WeightProfile.model_validate(
            {
                "version": "test",
                "status": "production",
                "owner": "system_review",
                "valid_from": "2026-05-06",
                "base_weights": {"trend": 0.60, "fundamentals": 0.30},
            }
        )


def test_approved_overlay_requires_approval_and_rollback() -> None:
    with pytest.raises(ValidationError, match="approved overlay requires approval"):
        CalibrationOverlay.model_validate(
            {
                "overlay_id": "macro_event_soft",
                "version": "v1",
                "status": "approved_soft",
                "valid_from": "2026-05-06",
                "expires_at": "2026-06-06",
                "rollback_condition": "shadow underperforms",
                "match": {"macro_event_within_2d": True},
                "effect": {"confidence_delta": -5.0},
            }
        )


def test_approved_soft_overlay_rejects_hard_effects() -> None:
    with pytest.raises(
        ValidationError,
        match="approved_soft overlay must not include hard effects",
    ):
        CalibrationOverlay.model_validate(
            {
                "overlay_id": "soft_block",
                "version": "v1",
                "status": "approved_soft",
                "valid_from": "2026-05-06",
                "expires_at": "2026-06-06",
                "rollback_condition": "owner rollback",
                "approval": {
                    "owner": "manual_owner",
                    "approved_at": "2026-05-06T00:00:00Z",
                    "approval_id": "approval_001",
                },
                "match": {"signal_family": ["breakout"]},
                "effect": {"block_trade": True},
            }
        )


def test_apply_calibration_overlays_matches_context_and_computes_effective_weights() -> None:
    profile = load_weight_profile()
    overlay = _overlay(
        overlay_id="macro_event_breakout_haircut",
        status="approved_soft",
        match={
            "macro_event_within_2d": True,
            "signal_family": ["breakout", "momentum"],
            "horizon": ["1D", "5D"],
        },
        effect={
            "weight_multipliers": {
                "trend": 0.80,
                "macro_liquidity": 1.30,
            },
            "confidence_delta": -5.0,
            "position_multiplier": 0.75,
            "required_confirmation": "post_event_follow_through",
        },
    )
    candidate = _overlay(
        overlay_id="unapproved_candidate",
        status="candidate",
        match={"macro_event_within_2d": True},
        effect={"confidence_delta": -10.0},
    )
    expired = _overlay(
        overlay_id="expired_overlay",
        status="approved_soft",
        match={"macro_event_within_2d": True},
        effect={"confidence_delta": -10.0},
        valid_from="2025-12-01",
        expires_at="2026-01-01",
    )
    context = {
        "signal_family": ["relative_strength", "breakout"],
        "horizon": ["5D", "20D"],
        "event_risk": {"macro_event_within_2d": True},
    }

    application = apply_calibration_overlays(
        context=context,
        profile=profile,
        overlays=(overlay, candidate, expired),
        as_of=date(2026, 5, 6),
    )

    assert application.matched_overlays == ("macro_event_breakout_haircut",)
    assert application.confidence_delta == -5.0
    assert application.position_multiplier == 0.75
    assert application.required_confirmations == ("post_event_follow_through",)
    assert application.effective_weights["trend"] == pytest.approx(0.2 / 0.995)
    assert application.effective_weights["macro_liquidity"] == pytest.approx(0.195 / 0.995)
    assert sum(application.effective_weights.values()) == pytest.approx(1.0)
    assert any("status candidate" in reason for reason in application.audit["why_not_applied"])
    assert any("expired" in reason for reason in application.audit["why_not_applied"])


def test_apply_calibration_overlays_without_match_keeps_base_weights() -> None:
    profile = load_weight_profile()
    overlay = _overlay(
        overlay_id="earnings_overlay",
        status="approved_soft",
        match={"earnings_within_5d": True},
        effect={"weight_multipliers": {"risk_sentiment": 0.5}},
    )

    application = apply_calibration_overlays(
        context={"event_risk": {"earnings_within_5d": False}},
        profile=profile,
        overlays=(overlay,),
        as_of=date(2026, 5, 6),
    )

    assert application.matched_overlays == ()
    assert application.effective_weights == profile.base_weights
    assert application.confidence_delta == 0.0
    assert application.position_multiplier == 1.0
    assert application.audit["why_not_applied"]


def test_load_calibration_overlays_missing_file_returns_empty_tuple(tmp_path: Path) -> None:
    assert load_calibration_overlays(tmp_path / "missing.json") == ()


def test_load_calibration_overlays_from_json(tmp_path: Path) -> None:
    overlays_path = tmp_path / "approved_calibration_overlay.json"
    overlays_path.write_text(
        json.dumps(
            [
                {
                    "overlay_id": "macro_event_breakout_haircut",
                    "version": "v1",
                    "status": "approved_soft",
                    "valid_from": "2026-05-06",
                    "expires_at": "2026-06-06",
                    "rollback_condition": "owner rollback",
                    "approval": {
                        "owner": "manual_owner",
                        "approved_at": "2026-05-06T00:00:00Z",
                        "approval_id": "approval_001",
                    },
                    "match": {"macro_event_within_2d": True},
                    "effect": {"confidence_delta": -5.0},
                }
            ]
        ),
        encoding="utf-8",
    )

    overlays = load_calibration_overlays(overlays_path)

    assert len(overlays) == 1
    assert overlays[0].overlay_id == "macro_event_breakout_haircut"


def _overlay(
    *,
    overlay_id: str,
    status: str,
    match: dict[str, object],
    effect: dict[str, object],
    valid_from: str = "2026-05-06",
    expires_at: str = "2026-06-06",
) -> CalibrationOverlay:
    payload = {
        "overlay_id": overlay_id,
        "version": "v1",
        "status": status,
        "valid_from": valid_from,
        "expires_at": expires_at,
        "rollback_condition": "owner rollback",
        "match": match,
        "effect": effect,
    }
    if status.startswith("approved"):
        payload["approval"] = {
            "owner": "manual_owner",
            "approved_at": "2026-05-06T00:00:00Z",
            "approval_id": f"{overlay_id}_approval",
        }
    return CalibrationOverlay.model_validate(payload)
