"""Checks every run passes before it is simulated: no look-ahead, every parameter is read.

These are the two measurement defects that most often made past results meaningless: targets that
used later data, and parameters that were declared but never reached the code (TRADING-1946,
2452/2458). Both checks re-run the strategy, so they work for any strategy without inspecting it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

Strategy = Callable[[pd.DataFrame, Mapping[str, Any]], pd.Series]

# Later values are reversed in time, rescaled and pushed once up and once down. A target that reads
# any later value (a level, a ratio of two levels, a return or only its sign) changes under at least
# one of the two.
_PERTURB_SCALE = 1.37
_PERTURB_SIGNS = (1.0, -1.0)


def strategy_targets(
    strategy: Strategy, inputs: pd.DataFrame, params: Mapping[str, Any]
) -> pd.Series:
    """Run the strategy and insist on one numeric target per input date."""

    targets = strategy(inputs, params)
    if not isinstance(targets, pd.Series):
        raise TypeError("a strategy must return a pandas Series of target exposures")
    if not targets.index.equals(inputs.index):
        raise ValueError("a strategy must return one target per input date, on the input index")
    return targets.astype(float)


def cut_dates(
    index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp, count: int
) -> list[pd.Timestamp]:
    """Evenly spaced dates inside [start, end), excluding the last date so later data exists."""

    inside = index[(index >= start) & (index < end)]
    if len(inside) == 0:
        return []
    positions = np.unique(np.linspace(0, len(inside) - 1, num=min(count, len(inside))).astype(int))
    return [inside[position] for position in positions]


def check_no_lookahead(
    strategy: Strategy,
    inputs: pd.DataFrame,
    params: Mapping[str, Any],
    dates: Sequence[pd.Timestamp],
) -> list[str]:
    """Problems found: a target on or before a cut date that changes when later data changes."""

    base = strategy_targets(strategy, inputs, params).to_numpy()
    problems: list[str] = []
    for cut in dates:
        after = inputs.index > cut
        if not after.any():
            continue
        upto = ~after
        for sign in _PERTURB_SIGNS:
            perturbed = inputs.copy()
            for column in perturbed.columns:
                later = perturbed.loc[after, column].to_numpy(dtype=float)
                perturbed.loc[after, column] = sign * (later[::-1] * _PERTURB_SCALE + 1.0)
            try:
                moved = strategy_targets(strategy, perturbed, params).to_numpy()
            except Exception as exc:  # any failure on changed later data is a finding
                problems.append(f"strategy failed when data after {cut.date()} changed: {exc}")
                break
            if not np.allclose(base[upto], moved[upto], rtol=0.0, atol=1e-12, equal_nan=True):
                changed = inputs.index[upto][~np.isclose(base[upto], moved[upto], equal_nan=True)]
                problems.append(
                    f"target on {changed[0].date()} changed when data after {cut.date()} changed"
                )
                break
    return problems


def check_parameters_consumed(
    strategy: Strategy,
    inputs: pd.DataFrame,
    params: Mapping[str, Any],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[list[str], list[str]]:
    """(problems, unchecked): a numeric or boolean parameter whose change moves no target inside
    the window is not read; other parameter types are listed as unchecked."""

    window = (inputs.index >= start) & (inputs.index <= end)
    base = strategy_targets(strategy, inputs, params).to_numpy()[window]
    problems: list[str] = []
    unchecked: list[str] = []
    for name, value in params.items():
        changed = _changed_value(value)
        if changed is None:
            unchecked.append(name)
            continue
        try:
            moved = strategy_targets(strategy, inputs, {**params, name: changed}).to_numpy()[window]
        except Exception as exc:  # reported; the run stops
            problems.append(f"parameter {name}={changed!r} could not be evaluated: {exc}")
            continue
        if np.allclose(base, moved, rtol=0.0, atol=1e-12, equal_nan=True):
            problems.append(f"parameter {name} does not change any target in the window")
    return problems, unchecked


def _changed_value(value: Any) -> Any:
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1 if value != 0 else 1
    if isinstance(value, float):
        return value * 1.5 if value != 0.0 else 0.1
    return None
