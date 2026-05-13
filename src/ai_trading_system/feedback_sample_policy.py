from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH = PROJECT_ROOT / "config" / "feedback_sample_policy.yaml"


@dataclass(frozen=True)
class OutcomeSampleFloors:
    reporting_floor: int
    pilot_floor: int
    diagnostic_floor: int
    promotion_floor: int


@dataclass(frozen=True)
class FeedbackSamplePolicy:
    version: str
    status: str
    market_regime_id: str
    review_after_reports: int
    decision_outcomes: OutcomeSampleFloors
    prediction_outcomes: OutcomeSampleFloors


def load_feedback_sample_policy(
    path: Path = DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
) -> FeedbackSamplePolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"feedback sample policy must be a mapping: {path}")
    policy = FeedbackSamplePolicy(
        version=_required_string(payload, "version"),
        status=_required_string(payload, "status"),
        market_regime_id=str(payload.get("market_regime_id", "ai_after_chatgpt")),
        review_after_reports=int(payload.get("review_after_reports", 8)),
        decision_outcomes=_outcome_sample_floors(payload, "decision_outcomes"),
        prediction_outcomes=_outcome_sample_floors(payload, "prediction_outcomes"),
    )
    validate_feedback_sample_policy(policy, path)
    return policy


def validate_feedback_sample_policy(policy: FeedbackSamplePolicy, path: Path) -> None:
    for label, floors in (
        ("decision_outcomes", policy.decision_outcomes),
        ("prediction_outcomes", policy.prediction_outcomes),
    ):
        if not (
            floors.reporting_floor
            <= floors.pilot_floor
            <= floors.diagnostic_floor
            <= floors.promotion_floor
        ):
            raise ValueError(
                "sample floors must be ordered reporting <= pilot <= diagnostic <= "
                f"promotion for {label}: {path}"
            )
    if policy.review_after_reports <= 0:
        raise ValueError(f"review_after_reports must be positive: {path}")


def sample_floor_summary(floors: OutcomeSampleFloors) -> str:
    return (
        "reporting/pilot/diagnostic/promotion="
        f"{floors.reporting_floor}/{floors.pilot_floor}/"
        f"{floors.diagnostic_floor}/{floors.promotion_floor}"
    )


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"feedback sample policy missing required string: {key}")
    return value.strip()


def _outcome_sample_floors(
    payload: dict[str, Any],
    key: str,
) -> OutcomeSampleFloors:
    raw = payload.get(key)
    if not isinstance(raw, dict):
        raise ValueError(f"feedback sample policy missing section: {key}")
    return OutcomeSampleFloors(
        reporting_floor=_non_negative_int(raw, "reporting_floor"),
        pilot_floor=_non_negative_int(raw, "pilot_floor"),
        diagnostic_floor=_non_negative_int(raw, "diagnostic_floor"),
        promotion_floor=_non_negative_int(raw, "promotion_floor"),
    )


def _non_negative_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"sample floor must be a non-negative integer: {key}")
    return value
