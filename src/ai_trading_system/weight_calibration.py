from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_WEIGHT_PROFILE_PATH = (
    PROJECT_ROOT / "config" / "weights" / "weight_profile_current.yaml"
)
DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH = (
    PROJECT_ROOT / "data" / "processed" / "approved_calibration_overlay.json"
)
DEFAULT_EFFECTIVE_WEIGHTS_PATH = PROJECT_ROOT / "outputs" / "current_effective_weights.json"

OverlayStatus = Literal[
    "candidate",
    "policy_replay",
    "shadow_report_only",
    "approved_soft",
    "approved_hard",
    "retired",
]
WeightProfileStatus = Literal["production", "shadow", "candidate", "retired"]

APPROVED_OVERLAY_STATUSES = frozenset({"approved_soft", "approved_hard"})
_FLOAT_EPSILON = 1e-6


class WeightProfileBounds(BaseModel):
    min_weight: float = Field(default=0.0, ge=0.0)
    max_weight: float = Field(default=1.0, ge=0.0)
    min_single_overlay_multiplier: float = Field(default=0.30, gt=0.0)
    max_single_overlay_multiplier: float = Field(default=1.50, gt=0.0)
    min_total_overlay_multiplier: float = Field(default=0.20, gt=0.0)
    max_total_overlay_multiplier: float = Field(default=2.00, gt=0.0)
    min_total_confidence_delta: float = -15.0
    max_total_confidence_delta: float = 10.0
    min_total_position_multiplier: float = Field(default=0.20, gt=0.0)
    max_total_position_multiplier: float = Field(default=1.0, gt=0.0)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.min_weight > self.max_weight:
            raise ValueError("min_weight must be <= max_weight")
        if self.min_single_overlay_multiplier > self.max_single_overlay_multiplier:
            raise ValueError(
                "min_single_overlay_multiplier must be <= max_single_overlay_multiplier"
            )
        if self.min_total_overlay_multiplier > self.max_total_overlay_multiplier:
            raise ValueError(
                "min_total_overlay_multiplier must be <= max_total_overlay_multiplier"
            )
        if self.min_total_confidence_delta > self.max_total_confidence_delta:
            raise ValueError(
                "min_total_confidence_delta must be <= max_total_confidence_delta"
            )
        if self.min_total_position_multiplier > self.max_total_position_multiplier:
            raise ValueError(
                "min_total_position_multiplier must be <= max_total_position_multiplier"
            )
        return self


class WeightNormalization(BaseModel):
    enabled: bool = True
    method: Literal["sum_to_one"] = "sum_to_one"


class WeightProfile(BaseModel):
    version: str = Field(min_length=1)
    status: WeightProfileStatus
    owner: str = Field(min_length=1)
    valid_from: date
    base_weights: dict[str, float]
    bounds: WeightProfileBounds = Field(default_factory=WeightProfileBounds)
    normalization: WeightNormalization = Field(default_factory=WeightNormalization)
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_base_weights(self) -> Self:
        if not self.base_weights:
            raise ValueError("base_weights must not be empty")
        empty_keys = [key for key in self.base_weights if not key.strip()]
        if empty_keys:
            raise ValueError("base_weights keys must not be empty")
        negative_keys = [key for key, value in self.base_weights.items() if value < 0.0]
        if negative_keys:
            raise ValueError(
                "base_weights must be non-negative: " + ", ".join(sorted(negative_keys))
            )
        overweight_keys = [
            key
            for key, value in self.base_weights.items()
            if value > self.bounds.max_weight + _FLOAT_EPSILON
        ]
        if overweight_keys:
            raise ValueError(
                "base_weights exceed max_weight: " + ", ".join(sorted(overweight_keys))
            )
        weight_sum = sum(self.base_weights.values())
        if abs(weight_sum - 1.0) > _FLOAT_EPSILON:
            raise ValueError("base_weights must sum to 1.0")
        return self


class OverlayEffect(BaseModel):
    weight_multipliers: dict[str, float] = Field(default_factory=dict)
    target_weights: dict[str, float] = Field(default_factory=dict)
    confidence_delta: float = 0.0
    position_multiplier: float = 1.0
    required_confirmation: str | list[str] | None = None
    hard_gate_max_position: float | None = None
    block_trade: bool = False
    observe_only: bool = False

    @model_validator(mode="after")
    def validate_effect_values(self) -> Self:
        if self.weight_multipliers and self.target_weights:
            raise ValueError("effect cannot define both weight_multipliers and target_weights")
        non_positive = [
            signal
            for signal, multiplier in self.weight_multipliers.items()
            if multiplier <= 0.0
        ]
        if non_positive:
            raise ValueError(
                "weight_multipliers must be positive: " + ", ".join(sorted(non_positive))
            )
        negative_targets = [
            signal for signal, weight in self.target_weights.items() if weight < 0.0
        ]
        if negative_targets:
            raise ValueError(
                "target_weights must be non-negative: "
                + ", ".join(sorted(negative_targets))
            )
        if self.target_weights and abs(sum(self.target_weights.values()) - 1.0) > _FLOAT_EPSILON:
            raise ValueError("target_weights must sum to 1.0")
        if self.position_multiplier <= 0.0:
            raise ValueError("position_multiplier must be positive")
        if self.hard_gate_max_position is not None and not 0 <= self.hard_gate_max_position <= 1:
            raise ValueError("hard_gate_max_position must be between 0 and 1")
        return self

    @property
    def required_confirmations(self) -> tuple[str, ...]:
        if self.required_confirmation is None:
            return ()
        if isinstance(self.required_confirmation, str):
            item = self.required_confirmation.strip()
            return (item,) if item else ()
        return tuple(item.strip() for item in self.required_confirmation if item.strip())

    @property
    def has_hard_effect(self) -> bool:
        return (
            self.hard_gate_max_position is not None
            or self.block_trade
            or self.observe_only
        )


class OverlayApproval(BaseModel):
    owner: str = Field(min_length=1)
    approved_at: str = Field(min_length=1)
    approval_id: str = Field(min_length=1)


class CalibrationOverlay(BaseModel):
    overlay_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.:-]+$")
    version: str = Field(min_length=1)
    status: OverlayStatus
    priority: int = Field(default=100)
    mutual_exclusion_group: str | None = Field(default=None, min_length=1)
    conflict_policy: Literal["highest_priority_wins", "fail_closed"] = (
        "highest_priority_wins"
    )
    match: dict[str, Any] = Field(default_factory=dict)
    effect: OverlayEffect = Field(default_factory=OverlayEffect)
    evidence: dict[str, Any] = Field(default_factory=dict)
    approval: OverlayApproval | None = None
    valid_from: date
    expires_at: date | None = None
    no_expiry_reason: str = ""
    rollback_condition: str = ""

    @model_validator(mode="after")
    def validate_overlay(self) -> Self:
        if not self.match:
            raise ValueError("overlay match must not be empty")
        if self.status in APPROVED_OVERLAY_STATUSES:
            if self.approval is None:
                raise ValueError("approved overlay requires approval")
            if self.expires_at is None and not self.no_expiry_reason.strip():
                raise ValueError("approved overlay requires expires_at or no_expiry_reason")
            if not self.rollback_condition.strip():
                raise ValueError("approved overlay requires rollback_condition")
        if self.expires_at is not None and self.expires_at < self.valid_from:
            raise ValueError("expires_at must not be before valid_from")
        if self.status == "approved_soft" and self.effect.has_hard_effect:
            raise ValueError("approved_soft overlay must not include hard effects")
        return self

    def production_eligible(self, *, as_of: date) -> bool:
        if self.status not in APPROVED_OVERLAY_STATUSES:
            return False
        if self.valid_from > as_of:
            return False
        return self.expires_at is None or self.expires_at >= as_of


@dataclass(frozen=True)
class OverlayMatchResult:
    overlay: CalibrationOverlay
    matched: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CalibrationApplication:
    weight_profile_version: str
    matched_overlays: tuple[str, ...]
    base_weights: dict[str, float]
    effective_weights: dict[str, float]
    confidence_delta: float
    position_multiplier: float
    required_confirmations: tuple[str, ...]
    audit: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "weight_profile_version": self.weight_profile_version,
            "matched_overlays": list(self.matched_overlays),
            "base_weights": self.base_weights,
            "effective_weights": self.effective_weights,
            "confidence_delta": self.confidence_delta,
            "position_multiplier": self.position_multiplier,
            "required_confirmations": list(self.required_confirmations),
            "audit": self.audit,
        }


def load_weight_profile(path: Path | str = DEFAULT_WEIGHT_PROFILE_PATH) -> WeightProfile:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)
    return WeightProfile.model_validate(raw)


def load_calibration_overlays(
    path: Path | str = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
) -> tuple[CalibrationOverlay, ...]:
    input_path = Path(path)
    if not input_path.exists():
        return ()
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("calibration overlay file must contain a JSON list")
    return tuple(CalibrationOverlay.model_validate(item) for item in raw)


def production_calibration_overlays(
    overlays: tuple[CalibrationOverlay, ...],
    *,
    as_of: date,
) -> tuple[CalibrationOverlay, ...]:
    return tuple(overlay for overlay in overlays if overlay.production_eligible(as_of=as_of))


def match_overlay(
    context: dict[str, Any],
    overlay: CalibrationOverlay,
) -> OverlayMatchResult:
    reasons: list[str] = []
    for key, expected in overlay.match.items():
        actual = _context_value(context, key)
        if actual is _MISSING:
            reasons.append(f"{overlay.overlay_id}: {key} missing")
            return OverlayMatchResult(overlay=overlay, matched=False, reasons=tuple(reasons))
        if not _match_value(actual, expected):
            reasons.append(
                f"{overlay.overlay_id}: {key} expected {expected!r}, actual {actual!r}"
            )
            return OverlayMatchResult(overlay=overlay, matched=False, reasons=tuple(reasons))
        reasons.append(f"{overlay.overlay_id}: {key} matched {expected!r}")
    return OverlayMatchResult(overlay=overlay, matched=True, reasons=tuple(reasons))


def apply_calibration_overlays(
    *,
    context: dict[str, Any],
    profile: WeightProfile,
    overlays: tuple[CalibrationOverlay, ...],
    as_of: date,
) -> CalibrationApplication:
    matched_results: list[OverlayMatchResult] = []
    why_not_applied: list[str] = []
    for overlay in overlays:
        if not overlay.production_eligible(as_of=as_of):
            why_not_applied.append(_overlay_ineligible_reason(overlay, as_of=as_of))
            continue
        _validate_overlay_signals(profile, overlay)
        result = match_overlay(context, overlay)
        if result.matched:
            matched_results.append(result)
        else:
            why_not_applied.extend(result.reasons)

    matched_results, conflict_audit = _resolve_overlay_conflicts(matched_results)
    why_not_applied.extend(conflict_audit)
    matched_overlays = tuple(
        result.overlay
        for result in sorted(
            matched_results,
            key=lambda item: (item.overlay.priority, item.overlay.overlay_id),
        )
    )
    cumulative_multipliers = {signal: 1.0 for signal in profile.base_weights}
    effective_weights = dict(profile.base_weights)
    confidence_delta = 0.0
    position_multiplier = 1.0
    confirmations: list[str] = []
    weight_changes: list[str] = []
    for overlay in matched_overlays:
        before_weights = dict(effective_weights)
        if overlay.effect.target_weights:
            effective_weights = _target_weights_for_profile(profile, overlay)
            cumulative_multipliers = {signal: 1.0 for signal in profile.base_weights}
            weight_changes.extend(
                _weight_change_audit_lines(
                    overlay_id=overlay.overlay_id,
                    before=before_weights,
                    after=effective_weights,
                )
            )
        for signal, raw_multiplier in overlay.effect.weight_multipliers.items():
            single_multiplier = _clamp(
                raw_multiplier,
                profile.bounds.min_single_overlay_multiplier,
                profile.bounds.max_single_overlay_multiplier,
            )
            next_total_multiplier = _clamp(
                cumulative_multipliers[signal] * single_multiplier,
                profile.bounds.min_total_overlay_multiplier,
                profile.bounds.max_total_overlay_multiplier,
            )
            effective_single_multiplier = (
                next_total_multiplier / cumulative_multipliers[signal]
            )
            cumulative_multipliers[signal] = next_total_multiplier
            effective_weights[signal] *= effective_single_multiplier
        if overlay.effect.weight_multipliers:
            if profile.normalization.enabled:
                effective_weights = _normalize_weights(effective_weights)
            weight_changes.extend(
                _weight_change_audit_lines(
                    overlay_id=overlay.overlay_id,
                    before=before_weights,
                    after=effective_weights,
                )
            )
        confidence_delta += overlay.effect.confidence_delta
        position_multiplier *= overlay.effect.position_multiplier
        confirmations.extend(overlay.effect.required_confirmations)

    if profile.normalization.enabled:
        effective_weights = _normalize_weights(effective_weights)

    confidence_delta = _clamp(
        confidence_delta,
        profile.bounds.min_total_confidence_delta,
        profile.bounds.max_total_confidence_delta,
    )
    position_multiplier = _clamp(
        position_multiplier,
        profile.bounds.min_total_position_multiplier,
        profile.bounds.max_total_position_multiplier,
    )
    return CalibrationApplication(
        weight_profile_version=profile.version,
        matched_overlays=tuple(overlay.overlay_id for overlay in matched_overlays),
        base_weights=dict(profile.base_weights),
        effective_weights=effective_weights,
        confidence_delta=confidence_delta,
        position_multiplier=position_multiplier,
        required_confirmations=tuple(dict.fromkeys(confirmations)),
        audit={
            "why_applied": [
                reason for result in matched_results for reason in result.reasons
            ],
            "why_not_applied": why_not_applied
            if why_not_applied
            else ["No approved overlays matched"]
            if not matched_overlays
            else [],
            "overlay_order": [
                f"{overlay.overlay_id}: priority={overlay.priority}"
                for overlay in matched_overlays
            ],
            "weight_changes": weight_changes
            if weight_changes
            else ["No overlay weight changes applied"],
        },
    )


def resolve_calibration_application(
    *,
    context: dict[str, Any],
    as_of: date,
    weight_profile_path: Path | str = DEFAULT_WEIGHT_PROFILE_PATH,
    overlays_path: Path | str = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
) -> CalibrationApplication:
    profile = load_weight_profile(weight_profile_path)
    overlays = load_calibration_overlays(overlays_path)
    return apply_calibration_overlays(
        context=context,
        profile=profile,
        overlays=overlays,
        as_of=as_of,
    )


def write_effective_weights(application: CalibrationApplication, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            application.to_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return output_path


def _validate_overlay_signals(
    profile: WeightProfile,
    overlay: CalibrationOverlay,
) -> None:
    referenced_signals = set(overlay.effect.weight_multipliers) | set(
        overlay.effect.target_weights
    )
    unknown = sorted(referenced_signals - set(profile.base_weights))
    if unknown:
        raise ValueError(
            f"approved overlay {overlay.overlay_id} references unknown signal(s): "
            + ", ".join(unknown)
        )
    if overlay.effect.target_weights:
        missing = sorted(set(profile.base_weights) - set(overlay.effect.target_weights))
        if missing:
            raise ValueError(
                f"approved overlay {overlay.overlay_id} target_weights missing signal(s): "
                + ", ".join(missing)
            )
        overweight = [
            signal
            for signal, weight in overlay.effect.target_weights.items()
            if weight > profile.bounds.max_weight + _FLOAT_EPSILON
            or weight < profile.bounds.min_weight - _FLOAT_EPSILON
        ]
        if overweight:
            raise ValueError(
                f"approved overlay {overlay.overlay_id} target_weights outside profile bounds: "
                + ", ".join(sorted(overweight))
            )


def _resolve_overlay_conflicts(
    matched_results: list[OverlayMatchResult],
) -> tuple[list[OverlayMatchResult], list[str]]:
    selected: list[OverlayMatchResult] = []
    audit: list[str] = []
    grouped: dict[str, list[OverlayMatchResult]] = {}
    for result in matched_results:
        group = result.overlay.mutual_exclusion_group
        if group is None:
            selected.append(result)
        else:
            grouped.setdefault(group, []).append(result)

    for group, results in sorted(grouped.items()):
        if len(results) == 1:
            selected.append(results[0])
            continue
        if any(result.overlay.conflict_policy == "fail_closed" for result in results):
            overlay_ids = ", ".join(result.overlay.overlay_id for result in results)
            raise ValueError(
                f"matched overlays conflict in mutual_exclusion_group {group}: "
                f"{overlay_ids}"
            )
        best_priority = max(result.overlay.priority for result in results)
        winners = [result for result in results if result.overlay.priority == best_priority]
        if len(winners) != 1:
            overlay_ids = ", ".join(result.overlay.overlay_id for result in winners)
            raise ValueError(
                f"matched overlays tie on priority in mutual_exclusion_group {group}: "
                f"{overlay_ids}"
            )
        winner = winners[0]
        selected.append(winner)
        for result in results:
            if result is winner:
                continue
            audit.append(
                f"{result.overlay.overlay_id}: skipped because "
                f"{winner.overlay.overlay_id} has higher priority in group {group}"
            )
    return selected, audit


def _target_weights_for_profile(
    profile: WeightProfile,
    overlay: CalibrationOverlay,
) -> dict[str, float]:
    weights = {
        signal: float(overlay.effect.target_weights[signal])
        for signal in profile.base_weights
    }
    if profile.normalization.enabled:
        weights = _normalize_weights(weights)
    return weights


def _weight_change_audit_lines(
    *,
    overlay_id: str,
    before: dict[str, float],
    after: dict[str, float],
) -> list[str]:
    lines: list[str] = []
    for signal in sorted(set(before) | set(after)):
        before_weight = before.get(signal, 0.0)
        after_weight = after.get(signal, 0.0)
        delta = after_weight - before_weight
        if abs(delta) <= _FLOAT_EPSILON:
            continue
        lines.append(
            f"{overlay_id}: {signal} {before_weight:.2%} -> "
            f"{after_weight:.2%} ({delta:+.2%})"
        )
    return lines


def _overlay_ineligible_reason(overlay: CalibrationOverlay, *, as_of: date) -> str:
    if overlay.status not in APPROVED_OVERLAY_STATUSES:
        return f"{overlay.overlay_id}: status {overlay.status} is not production eligible"
    if overlay.valid_from > as_of:
        return f"{overlay.overlay_id}: valid_from {overlay.valid_from.isoformat()} is after as_of"
    if overlay.expires_at is not None and overlay.expires_at < as_of:
        return f"{overlay.overlay_id}: expired at {overlay.expires_at.isoformat()}"
    return f"{overlay.overlay_id}: not production eligible"


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("effective weights total must be positive")
    return {signal: value / total for signal, value in weights.items()}


def _match_value(actual: Any, expected: Any) -> bool:
    if isinstance(expected, list):
        expected_values = set(_scalar_tokens(expected))
        if isinstance(actual, list):
            return bool(expected_values & set(_scalar_tokens(actual)))
        return _scalar_token(actual) in expected_values
    if isinstance(actual, list):
        return _scalar_token(expected) in set(_scalar_tokens(actual))
    return actual == expected


def _scalar_tokens(values: list[Any]) -> list[str]:
    return [_scalar_token(value) for value in values]


def _scalar_token(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class _MissingValue:
    pass


_MISSING = _MissingValue()


def _context_value(context: dict[str, Any], key: str) -> Any:
    if key in context:
        return context[key]
    if "." in key:
        current: Any = context
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                return _MISSING
            current = current[part]
        return current
    matches = _find_nested_key(context, key)
    if len(matches) == 1:
        return matches[0]
    return _MISSING


def _find_nested_key(value: Any, key: str) -> list[Any]:
    if not isinstance(value, dict):
        return []
    matches: list[Any] = []
    for item_key, item_value in value.items():
        if item_key == key:
            matches.append(item_value)
        matches.extend(_find_nested_key(item_value, key))
    return matches


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
