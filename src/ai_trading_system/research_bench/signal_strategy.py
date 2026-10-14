"""Pinned signals to target exposure; scoring and mapping are fixed by the preregistration."""

from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench import screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings

SIGNALS_DIR = screen.SIGNALS_DIR
SOURCE_PREFIX = "source:"


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value):
        raise ValueError("mapping parameters must be finite numbers")
    return float(value)


def _parameters(params: Mapping[str, Any]) -> None:
    signals = params.get("signals")
    if not isinstance(signals, list) or not signals:
        raise ValueError("signals must be a nonempty list")
    ids = set()
    for ref in signals:
        if not isinstance(ref, Mapping) or set(ref) != {
            "id",
            "module_sha256",
            "spec_sha256",
            "score",
        }:
            raise ValueError("each signal needs exactly id, module_sha256, spec_sha256 and score")
        if not isinstance(ref["id"], str) or ref["id"] in ids:
            raise ValueError("signal ids must be unique strings")
        ids.add(ref["id"])
        if ref["score"] not in ("percentile", "sign"):
            raise ValueError("signal score must be percentile or sign")
        if not all(isinstance(ref[key], str) for key in ("module_sha256", "spec_sha256")):
            raise ValueError("signal hashes must be strings")
    mapping = params.get("mapping")
    required: set[str]
    optional: set[str]
    if mapping == "threshold":
        required, optional = {"low", "high"}, {"cut"}
    elif mapping == "linear":
        required, optional = {"low", "high"}, {"p_low", "p_high"}
    elif mapping == "steps":
        required, optional = {"edges", "levels"}, set()
    else:
        raise ValueError("mapping must be threshold, linear or steps")
    keys = set(params) - {"signals", "mapping"}
    if not required <= keys or keys - required - optional:
        raise ValueError(f"{mapping}: missing or extra mapping parameters")
    if mapping == "steps":
        edges, levels = params["edges"], params["levels"]
        if not isinstance(edges, list) or not isinstance(levels, list):
            raise ValueError("steps edges and levels must be lists")
        edge_values = [_number(value) for value in edges]
        for value in levels:
            _number(value)
        if len(levels) != len(edges) + 1 or any(
            left >= right for left, right in zip(edge_values, edge_values[1:], strict=False)
        ):
            raise ValueError("steps need increasing edges and one more level than edges")
    else:
        for key in keys:
            _number(params[key])
        if mapping == "linear" and params.get("p_low", 0.0) >= params.get("p_high", 1.0):
            raise ValueError("linear requires p_low < p_high")
    # Do not bound scores or exposures here: parameter-consumption checks perturb numbers.
    # The bench's simulation, not this adapter, enforces executable exposure bounds.


def signal_exposure(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """Average oriented signal scores, map to exposure, then hold through missing observations."""

    _parameters(params)
    scores = []
    for ref in params["signals"]:
        loaded = screen.load_signal(ref["id"], directory=SIGNALS_DIR)
        if (loaded.module_sha256, loaded.spec_sha256) != (ref["module_sha256"], ref["spec_sha256"]):
            raise ValueError(f"signal {ref['id']} changed since the preregistration")
        sources = {}
        for source in loaded.spec.sources:
            prefix = f"{SOURCE_PREFIX}{source}:"
            columns = [column for column in inputs.columns if column.startswith(prefix)]
            if not columns:
                raise ValueError(f"source {source} is missing; set data.signal_snapshot_id")
            sources[source] = inputs[columns].rename(
                columns={column: column.removeprefix(prefix) for column in columns}
            )
        values = loaded.function(inputs["ndx_return"], sources, dict(loaded.spec.params))
        if not isinstance(values, pd.Series) or not values.index.equals(inputs.index):
            raise ValueError(f"signal {ref['id']} must return a Series on inputs.index")
        favourable = loaded.spec.expected_sign * (1 if loaded.spec.target == "return" else -1)
        if ref["score"] == "percentile":
            warmup = load_screen_settings().settings.screen.warmup_days
            score = screen.expanding_percentile(values, warmup)
            if favourable < 0:
                score = 1 - score
        else:
            oriented = favourable * values
            score = pd.Series(
                np.where(oriented > 0, 1.0, np.where(oriented < 0, 0.0, 0.5)),
                index=inputs.index,
            ).mask(values.isna())
        scores.append(score)
    score = pd.concat(scores, axis=1).mean(axis=1, skipna=False)
    mapping = params["mapping"]
    if mapping == "threshold":
        targets = pd.Series(
            np.where(score >= params.get("cut", 0.5), params["high"], params["low"]),
            index=inputs.index,
        )
    elif mapping == "linear":
        p_low, p_high = params.get("p_low", 0.0), params.get("p_high", 1.0)
        targets = params["low"] + (params["high"] - params["low"]) * (
            (score - p_low) / (p_high - p_low)
        ).clip(0, 1)
    else:
        levels = np.asarray(params["levels"], dtype=float)
        targets = pd.Series(
            levels[np.searchsorted(params["edges"], score, side="right")], index=inputs.index
        )
    return targets.astype(float).mask(score.isna()).ffill()
