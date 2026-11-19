"""Read position policy without treating a policy band as a gate result.

In full-exposure mode the final band is the policy's 1.0 and gates only inform;
``gated_risk_asset_ai_band`` records the old-rule band. Snapshots before
TRADING-2572 have no ``position_policy`` block: they used the gated rule, so
their final band is also their old-rule band.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

POSITION_POLICY_FULL_EXPOSURE_DEFAULT = "full_exposure_default"
POSITION_POLICY_GATED_MIN = "gated_min"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _has_bounds(band: Mapping[str, Any]) -> bool:
    return (
        _number(band.get("min_position")) is not None
        and _number(band.get("max_position")) is not None
    )


def snapshot_positions(snapshot: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return _mapping(_mapping(snapshot).get("positions"))


def position_policy_mode(positions: Mapping[str, Any]) -> str:
    mode = _mapping(positions.get("position_policy")).get("mode")
    return mode if isinstance(mode, str) and mode else POSITION_POLICY_GATED_MIN


def is_full_exposure(positions: Mapping[str, Any]) -> bool:
    return position_policy_mode(positions) == POSITION_POLICY_FULL_EXPOSURE_DEFAULT


def gated_band_record(positions: Mapping[str, Any]) -> Mapping[str, Any]:
    gated = _mapping(positions.get("gated_risk_asset_ai_band"))
    return gated if _has_bounds(gated) else _mapping(positions.get("final_risk_asset_ai_band"))


def binding_reference_max(positions: Mapping[str, Any]) -> float | None:
    return _number(gated_band_record(positions).get("max_position"))


def band_text(band: Mapping[str, Any]) -> str:
    low = _number(band.get("min_position"))
    high = _number(band.get("max_position"))
    return f"{low:.0%}-{high:.0%}" if low is not None and high is not None else "UNKNOWN"


def full_exposure_note(positions: Mapping[str, Any]) -> str:
    if not is_full_exposure(positions):
        return ""
    return f"（满仓为底，闸门只提示；按原规则会给 {band_text(gated_band_record(positions))}）"
