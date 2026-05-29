from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.trading_engine.parameters.parameter_schema import ProductionParameters


@dataclass(frozen=True)
class ParameterChange:
    name: str
    baseline: float
    candidate: float
    delta: float
    reason: str
    risk: str
    source_windows: tuple[str, ...]
    improved_metrics: tuple[str, ...]
    worsened_metrics: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "delta": self.delta,
            "reason": self.reason,
            "risk": self.risk,
            "source_windows": list(self.source_windows),
            "improved_metrics": list(self.improved_metrics),
            "worsened_metrics": list(self.worsened_metrics),
        }


def diff_parameters(
    baseline: ProductionParameters,
    candidate_weights: dict[str, float],
    *,
    reasons: dict[str, str] | None = None,
    source_windows: dict[str, tuple[str, ...]] | None = None,
    improved_metrics: dict[str, tuple[str, ...]] | None = None,
    worsened_metrics: dict[str, tuple[str, ...]] | None = None,
) -> tuple[ParameterChange, ...]:
    reasons = reasons or {}
    source_windows = source_windows or {}
    improved_metrics = improved_metrics or {}
    worsened_metrics = worsened_metrics or {}
    changes: list[ParameterChange] = []
    for name in sorted(set(baseline.weights) | set(candidate_weights)):
        baseline_value = float(baseline.weights.get(name, 0.0))
        candidate_value = float(candidate_weights.get(name, 0.0))
        delta = candidate_value - baseline_value
        if abs(delta) <= 1e-12:
            continue
        changes.append(
            ParameterChange(
                name=name,
                baseline=baseline_value,
                candidate=candidate_value,
                delta=delta,
                reason=reasons.get(name) or _default_reason(name, delta),
                risk=_default_risk(name, delta),
                source_windows=source_windows.get(name, ()),
                improved_metrics=improved_metrics.get(name, ()),
                worsened_metrics=worsened_metrics.get(name, ()),
            )
        )
    return tuple(changes)


def _default_reason(name: str, delta: float) -> str:
    direction = "increased" if delta > 0 else "reduced"
    return (
        f"{name} weight was {direction} by bounded grid validation because the candidate "
        "ranked better than baseline on walk-forward validation windows."
    )


def _default_risk(name: str, delta: float) -> str:
    if delta > 0 and name in {"trend_momentum", "sector_strength"}:
        return "May increase participation in crowded momentum regimes and underperform in chop."
    if delta > 0 and name in {"valuation_risk", "event_risk", "macro_liquidity"}:
        return "May reduce upside capture when risk signals are too conservative."
    if delta < 0 and name in {"valuation_risk", "event_risk", "macro_liquidity"}:
        return "May increase drawdown if risk conditions deteriorate faster than prices."
    return "Requires manual review for regime sensitivity and turnover impact."
