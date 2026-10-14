"""De-risk episodes on held exposure; descriptive summaries, never a promotion rule (S4c)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.screen_settings import EventSettings as EventSettings
from ai_trading_system.research_bench.settings import EpisodeSpec

# Numerical equality tolerance, not an investment threshold (same scale as the simulator).
_EPSILON = 1e-12


def derisk_events(
    exposure: pd.Series,
    qqq_returns: pd.Series,
    *,
    reference_exposure: float,
    settings: EventSettings,
    episodes: Sequence[EpisodeSpec],
    trading_days_per_year: int = 252,
) -> dict[str, Any]:
    """Group de-risked trading dates, compound QQQ over each event including merged gaps,
    and measure exit/re-entry lags for the bench's fixed monthly episodes.
    """

    if not exposure.index.equals(qqq_returns.index):
        raise ValueError("exposure and QQQ returns must have the same dates")
    if trading_days_per_year < 1:
        raise ValueError("trading_days_per_year must be positive")
    index = pd.DatetimeIndex(exposure.index)
    held = exposure.to_numpy(dtype=float)
    returns = qqq_returns.to_numpy(dtype=float)
    threshold = float(settings.derisk_ratio * reference_exposure)
    derisked = held <= threshold + _EPSILON
    positions = np.flatnonzero(derisked)
    spans: list[tuple[int, int]] = []
    for position in positions:
        day = int(position)
        if spans and day - spans[-1][1] - 1 < settings.merge_gap_days:
            spans[-1] = (spans[-1][0], day)
        else:
            spans.append((day, day))

    events: list[dict[str, Any]] = []
    classes = ("protective", "false_alarm", "neutral")
    outcomes: dict[str, Any] = {name: {"count": 0, "qqq_return": 0.0} for name in classes}
    outcome_masks = {name: np.zeros(len(index), dtype=bool) for name in classes}
    for first, last in spans:
        qqq_return = float(np.prod(1.0 + returns[first : last + 1]) - 1.0)
        outcome = (
            "protective"
            if qqq_return <= settings.protective_loss
            else "false_alarm"
            if qqq_return >= settings.false_alarm_gain
            else "neutral"
        )
        events.append(
            {
                "start": index[first].date().isoformat(),
                "end": index[last].date().isoformat(),
                "days": last - first + 1,
                "min_exposure": float(held[first : last + 1].min()),
                "qqq_return": qqq_return,
                "outcome": outcome,
            }
        )
        outcomes[outcome]["count"] += 1
        outcome_masks[outcome][first : last + 1] = True
    for name in classes:
        outcomes[name]["qqq_return"] = float(np.prod(1.0 + returns[outcome_masks[name]]) - 1.0)

    monthly = index.to_period("M")
    episode_table: dict[str, Any] = {}
    for episode in episodes:
        inside = (monthly >= pd.Period(episode.start, "M")) & (
            monthly <= pd.Period(episode.end, "M")
        )
        days = np.flatnonzero(inside)
        if not len(days):
            continue
        first, last = int(days[0]), int(days[-1])
        exits = np.flatnonzero(derisked[inside])
        reentries = np.flatnonzero(held[last:] >= reference_exposure - _EPSILON)
        episode_table[episode.id] = {
            "days": int(len(days)),
            "derisked_share": float(derisked[inside].mean()),
            "missed_loss": float(np.prod(1.0 + returns[inside & ~derisked]) - 1.0),
            "exit_lag_days": int(days[int(exits[0])]) - first if len(exits) else None,
            "reentry_lag_days": int(reentries[0]) if len(reentries) else None,
        }
    return {
        "reference_exposure": float(reference_exposure),
        "threshold": threshold,
        "events": events,
        "count": len(events),
        "per_year": float(len(events) / (len(index) / trading_days_per_year))
        if len(index)
        else 0.0,
        "short_count": sum(event["days"] < settings.short_event_days for event in events),
        "outcomes": outcomes,
        "episodes": episode_table,
    }
