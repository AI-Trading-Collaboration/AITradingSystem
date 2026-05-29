from __future__ import annotations

import itertools
from dataclasses import dataclass
from hashlib import sha256

from ai_trading_system.trading_engine.parameters.parameter_schema import (
    ProductionParameters,
    ShadowSearchConfig,
)


@dataclass(frozen=True)
class CandidateWeightSet:
    version: str
    weights: dict[str, float]
    l1_change: float
    max_abs_change: float

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "weights": dict(self.weights),
            "l1_change": self.l1_change,
            "max_abs_change": self.max_abs_change,
        }


def generate_bounded_weight_candidates(
    baseline: ProductionParameters,
    search: ShadowSearchConfig,
) -> tuple[CandidateWeightSet, ...]:
    names = tuple(sorted(baseline.weights))
    value_grid: list[tuple[float, ...]] = []
    for name in names:
        entry = search.search_space.get(name)
        if entry is None:
            value_grid.append((baseline.weights[name],))
            continue
        max_abs_change = search.parameter_change_guardrails.max_abs_change_per_weight
        lower = max(entry.min, baseline.weights[name] - max_abs_change)
        upper = min(entry.max, baseline.weights[name] + max_abs_change)
        value_grid.append(_grid_values(lower, upper, entry.step))
    raw_candidates: list[CandidateWeightSet] = []
    target_sum = search.constraints.total_weight_sum
    for values in itertools.product(*value_grid):
        weights = {name: round(float(value), 10) for name, value in zip(names, values, strict=True)}
        total = sum(weights.values())
        if abs(total - target_sum) > 1e-9:
            continue
        if any(value > search.constraints.max_single_weight for value in weights.values()):
            continue
        if any(value < search.constraints.min_single_weight for value in weights.values()):
            continue
        if all(abs(weights[name] - baseline.weights[name]) <= 1e-12 for name in names):
            continue
        changes = [abs(weights[name] - baseline.weights[name]) for name in names]
        l1_change = sum(changes)
        max_abs_change = max(changes) if changes else 0.0
        if l1_change > search.parameter_change_guardrails.max_total_l1_change + 1e-12:
            continue
        raw_candidates.append(
            CandidateWeightSet(
                version=_candidate_version(weights),
                weights=weights,
                l1_change=l1_change,
                max_abs_change=max_abs_change,
            )
        )
    raw_candidates.sort(key=lambda item: (item.l1_change, item.max_abs_change, item.version))
    return tuple(raw_candidates[: search.max_candidates])


def _grid_values(lower: float, upper: float, step: float) -> tuple[float, ...]:
    values: list[float] = []
    current = lower
    while current <= upper + 1e-12:
        values.append(round(current, 10))
        current += step
    return tuple(values)


def _candidate_version(weights: dict[str, float]) -> str:
    digest = sha256()
    for key in sorted(weights):
        digest.update(f"{key}={weights[key]:.10f};".encode())
    return "shadow-" + digest.hexdigest()[:12]
