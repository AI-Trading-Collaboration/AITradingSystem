from __future__ import annotations

from collections.abc import Sequence


def moving_average(values: Sequence[float], window: int) -> list[float | None]:
    if window <= 0:
        raise ValueError("window must be positive")

    averages: list[float | None] = []
    rolling_sum = 0.0
    for index, value in enumerate(values):
        rolling_sum += value
        if index >= window:
            rolling_sum -= values[index - window]
        if index + 1 < window:
            averages.append(None)
        else:
            averages.append(rolling_sum / window)
    return averages


def latest_moving_average(values: Sequence[float], window: int) -> float:
    averages = moving_average(values, window)
    latest = averages[-1] if averages else None
    if latest is None:
        raise ValueError("not enough values for requested window")
    return latest


def is_above_moving_average(values: Sequence[float], window: int) -> bool:
    if not values:
        raise ValueError("values must not be empty")
    return values[-1] > latest_moving_average(values, window)


def relative_strength(numerator: Sequence[float], denominator: Sequence[float]) -> list[float]:
    if len(numerator) != len(denominator):
        raise ValueError("numerator and denominator must have the same length")
    if any(value == 0 for value in denominator):
        raise ValueError("denominator must not contain zero")
    return [left / right for left, right in zip(numerator, denominator, strict=True)]


def percentage_change(current: float, previous: float) -> float:
    if previous == 0:
        raise ValueError("previous must not be zero")
    return (current / previous) - 1.0
