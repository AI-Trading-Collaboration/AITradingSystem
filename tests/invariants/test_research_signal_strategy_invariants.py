"""Invariants of the bench's generic signal-to-exposure strategy (TRADING-2565 S4d).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-13:research_throughput``). All data is synthetic.

41. The expanding percentile is the one the screen uses, and it never reads later values.
42. Scores and mappings: percentile and sign scores, orientation by expected sign and target, the
    three mappings (a score on a step edge goes up), the average of several signals, warm-up and
    holding the last target; each signal gets the parameters of its own spec.
43. The preregistration pins each signal: a changed module or spec is refused, line endings are
    not a change.
44. The bench's own checks see through the generic strategy: a signal that looks ahead is caught,
    every numeric mapping parameter is read.
45. Signal sources enter the bench inputs only as known at each close, and only when the
    preregistration names a signal snapshot.
46. Preregistration fields: the signal snapshot is fixed, screens are cited by number, and QQQ-Q1
    reads exactly as before.
47. The generic strategy gives the same run as the same rule written by hand.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import (
    BenchData,
    Rebalance,
    RunSpec,
    from_daily_frame,
    load_settings,
    run_strategy,
)
from ai_trading_system.research_bench import signal_strategy as gs
from ai_trading_system.research_bench.checks import (
    check_no_lookahead,
    check_parameters_consumed,
    cut_dates,
    strategy_targets,
)
from ai_trading_system.research_bench.data import with_signal_sources
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_bench.prereg import DataSpec, Prereg, load_prereg
from ai_trading_system.research_bench.screen import expanding_percentile
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.settings import resolve_window
from ai_trading_system.research_bench.sources import known_at

BENCH_SETTINGS_SHA256 = "69351836d2e2bacb8b2888fe783a0949f485af9bf3e0294ffdb78ba50d0c46c2"
LOADED = load_settings()
DEV = resolve_window(LOADED, "qqq_direction", "development")
WARMUP = load_screen_settings().settings.screen.warmup_days
NAN = float("nan")


def _spec(signal_id: str, **spec: Any) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "mechanism": "test signal",
        "expected_sign": 1,
        "target": "return",
        "params": {},
        "sources": [],
        "markets": "all",
        "literature": "none",
    } | spec


def _write(
    directory: Path, signal_id: str, body: str, *, newline: str = "\n", **spec: Any
) -> dict[str, Any]:
    module = body.replace("\n", newline)
    text = json.dumps(_spec(signal_id, **spec))
    (directory / f"{signal_id}.py").write_bytes(module.encode("utf-8"))
    (directory / f"{signal_id}.yaml").write_bytes(text.encode("utf-8"))
    return {
        "id": signal_id,
        "module_sha256": chain_digest(body.encode("utf-8")),
        "spec_sha256": chain_digest(text.encode("utf-8")),
    }


IDENTITY = """
def signal(returns, sources, params):
    return returns.copy()
"""

MOMENTUM = """
import numpy as np


def signal(returns, sources, params):
    return np.log1p(returns).rolling(63).sum()
"""

PEEKING = """
def signal(returns, sources, params):
    return returns.shift(-3).rolling(3).sum()
"""

MA_GAP = """
import numpy as np


def signal(returns, sources, params):
    price = np.exp(np.log1p(returns).cumsum())
    return price / price.rolling(210).mean() - 1
"""

SCALED = """
def signal(returns, sources, params):
    return returns * params["scale"]
"""

SPREAD = """
def signal(returns, sources, params):
    return -sources["spread"]["value"].diff(21)
"""

SPREAD_PEEKING = """
def signal(returns, sources, params):
    return -sources["spread"]["value"].shift(-1)
"""


@pytest.fixture
def signals_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(gs, "SIGNALS_DIR", tmp_path)
    return tmp_path


def _data(seed: int = 7) -> BenchData:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(seed)
    regime = np.where((index.year >= 2000) & (index.year <= 2002), 0.03, 0.015)
    r = rng.normal(0.0004, 1.0, len(index)) * regime
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r - 0.002 / 252,
            "etf_reference_return": r,
            "cash_return": np.full(len(index), 0.03 / 360 * 1.4),
            "lev2_return": 2 * r - 0.04 / 252,
            "lev3_return": 3 * r - 0.07 / 252,
            "ndx_price": level * 0.98,
            "ndx_tr": level,
            "vix": 20.0 + 100 * np.abs(r),
            "vxn": 25.0 + 100 * np.abs(r),
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    daily.iloc[0, :5] = np.nan
    return from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": f"synthetic-{seed}"})


def _probe_inputs() -> pd.DataFrame:
    """ndx_return: 252 distinct warm-up values, then 1.0, -1.0, 0.1255, NaN, 0.0."""

    values = [*(np.arange(WARMUP) / 1000.0), 1.0, -1.0, 0.1255, NAN, 0.0]
    index = pd.bdate_range("2000-01-03", periods=len(values), name="date")
    return pd.DataFrame({"ndx_return": values}, index=index)


def _tail(targets: pd.Series) -> list[float]:
    return [float(v) for v in targets.to_numpy()[WARMUP:]]


# 41 ----------------------------------------------------------------------------------------------


def test_the_expanding_percentile_is_the_screen_one() -> None:
    values = pd.Series([3.0, 1.0, 2.0, 2.0, 5.0, NAN, 0.0])
    got = expanding_percentile(values, 2).to_numpy()
    np.testing.assert_allclose(got, [NAN, NAN, 0.5, 0.5, 1.0, NAN, 0.0], equal_nan=True)
    later = values.copy()
    later.iloc[5:] = [9.0, -9.0]
    np.testing.assert_array_equal(expanding_percentile(later, 2).to_numpy()[:5], got[:5])


# 42 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mapping", "expected"),
    [
        (
            {"mapping": "threshold", "cut": 0.5, "low": 0.25, "high": 1.0},
            [1.0, 0.25, 1.0, 1.0, 0.25],
        ),
        ({"mapping": "threshold", "low": 0.25, "high": 1.0}, [1.0, 0.25, 1.0, 1.0, 0.25]),
        (
            {"mapping": "linear", "p_low": 0.2, "p_high": 0.8, "low": 0.0, "high": 2.0},
            [2.0, 0.0, 1.0, 1.0, 0.0],
        ),
        ({"mapping": "linear", "low": 0.0, "high": 2.0}, [2.0, 0.0, 1.0, 1.0, 3.0 / 255]),
        (
            {"mapping": "steps", "edges": [0.4, 0.6], "levels": [0.0, 0.5, 1.5]},
            [1.5, 0.0, 0.5, 0.5, 0.0],
        ),
    ],
)
def test_percentile_scores_and_mappings(
    signals_dir: Path, mapping: dict[str, Any], expected: list[float]
) -> None:
    ref = _write(signals_dir, "ident", IDENTITY) | {"score": "percentile"}
    targets = gs.signal_exposure(_probe_inputs(), {"signals": [ref], **mapping})
    assert targets.index.equals(_probe_inputs().index)
    assert targets.iloc[:WARMUP].isna().all()
    # scores 1, 0, 1/2, none (the last target is held), 1.5/255
    assert _tail(targets) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ({"expected_sign": -1}, [0.25, 1.0, 1.0]),
        ({"target": "risk"}, [0.25, 1.0, 1.0]),
        ({"target": "risk", "expected_sign": -1}, [1.0, 0.25, 1.0]),
    ],
)
def test_orientation_follows_expected_sign_and_target(
    signals_dir: Path, spec: dict[str, Any], expected: list[float]
) -> None:
    ref = _write(signals_dir, "ident", IDENTITY, **spec) | {"score": "percentile"}
    params = {"signals": [ref], "mapping": "threshold", "low": 0.25, "high": 1.0}
    assert _tail(gs.signal_exposure(_probe_inputs(), params))[:3] == pytest.approx(expected)


def test_sign_scores_need_no_warm_up_and_average_across_signals(signals_dir: Path) -> None:
    inputs = _probe_inputs()
    up = _write(signals_dir, "ident", IDENTITY) | {"score": "sign"}
    down = _write(signals_dir, "neg", IDENTITY, expected_sign=-1) | {"score": "sign"}
    linear = {"mapping": "linear", "low": 0.0, "high": 1.0}
    alone = gs.signal_exposure(inputs, {"signals": [up], **linear})
    assert alone.iloc[0] == pytest.approx(0.5)  # a return of exactly 0 scores one half
    assert alone.iloc[1:WARMUP].tolist() == pytest.approx([1.0] * (WARMUP - 1))
    assert _tail(alone) == pytest.approx([1.0, 0.0, 1.0, 1.0, 0.5])
    both = gs.signal_exposure(inputs, {"signals": [up, down], **linear})
    assert _tail(both)[:3] == pytest.approx([0.5, 0.5, 0.5])
    mixed = gs.signal_exposure(inputs, {"signals": [up, down | {"score": "percentile"}], **linear})
    assert mixed.iloc[:WARMUP].isna().all()  # every signal must have a score
    assert _tail(mixed)[:3] == pytest.approx([0.5, 0.5, 0.75])


def test_a_score_on_a_step_edge_goes_up_and_signals_get_their_spec_parameters(
    signals_dir: Path,
) -> None:
    inputs = _probe_inputs()
    up = _write(signals_dir, "ident", IDENTITY) | {"score": "sign"}
    steps = {"mapping": "steps", "edges": [0.5], "levels": [0.0, 1.0]}
    on_edge = gs.signal_exposure(inputs, {"signals": [up], **steps})
    assert on_edge.iloc[0] == pytest.approx(1.0)  # score 1/2 sits on the edge
    assert _tail(on_edge) == pytest.approx([1.0, 0.0, 1.0, 1.0, 1.0])
    flipped = _write(signals_dir, "scaled", SCALED, params={"scale": -2.0}) | {"score": "sign"}
    linear = {"mapping": "linear", "low": 0.0, "high": 1.0}
    assert _tail(gs.signal_exposure(inputs, {"signals": [flipped], **linear})) == pytest.approx(
        [0.0, 1.0, 0.0, 0.0, 0.5]
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"mapping": "threshold", "low": 0.0},
        {"mapping": "threshold", "low": 0.0, "high": 1.0, "p_low": 0.1},
        {"mapping": "linear", "low": 0.0, "high": 1.0, "p_low": 0.6, "p_high": 0.4},
        {"mapping": "steps", "edges": [0.6, 0.4], "levels": [0.0, 0.5, 1.0]},
        {"mapping": "steps", "edges": [0.4, 0.6], "levels": [0.0, 1.0]},
        {"mapping": "spline", "low": 0.0, "high": 1.0},
        {"mapping": "threshold", "low": 0.0, "high": 1.0, "signals": []},
    ],
)
def test_malformed_parameters_are_refused(signals_dir: Path, bad: dict[str, Any]) -> None:
    ref = _write(signals_dir, "ident", IDENTITY) | {"score": "percentile"}
    with pytest.raises(ValueError):
        gs.signal_exposure(_probe_inputs(), {"signals": [ref]} | bad)


# 43 ----------------------------------------------------------------------------------------------


def test_the_preregistration_pins_each_signal(signals_dir: Path) -> None:
    ref = _write(signals_dir, "ident", IDENTITY) | {"score": "percentile"}
    params = {"signals": [ref], "mapping": "threshold", "low": 0.0, "high": 1.0}
    base = gs.signal_exposure(_probe_inputs(), params)

    _write(signals_dir, "ident", IDENTITY, newline="\r\n")
    pd.testing.assert_series_equal(gs.signal_exposure(_probe_inputs(), params), base)

    _write(signals_dir, "ident", IDENTITY.replace("copy()", "copy() * 1.0"))
    with pytest.raises(ValueError, match="ident"):
        gs.signal_exposure(_probe_inputs(), params)

    _write(signals_dir, "ident", IDENTITY, literature="changed")
    with pytest.raises(ValueError, match="ident"):
        gs.signal_exposure(_probe_inputs(), params)

    with pytest.raises(ValueError):
        gs.signal_exposure(_probe_inputs(), params | {"signals": [ref | {"id": "missing"}]})


# 44 ----------------------------------------------------------------------------------------------


def _cuts(data: BenchData) -> list[pd.Timestamp]:
    index = pd.DatetimeIndex(data.inputs.index)
    return cut_dates(index, pd.Timestamp(DEV.start), pd.Timestamp(DEV.end), 10)


def test_the_bench_check_catches_a_signal_that_looks_ahead(signals_dir: Path) -> None:
    data = _data()
    honest = _write(signals_dir, "mom", MOMENTUM) | {"score": "percentile"}
    peeking = _write(signals_dir, "peek", PEEKING) | {"score": "percentile"}
    mapping = {"mapping": "threshold", "cut": 0.5, "low": 0.25, "high": 1.0}
    with np.errstate(all="ignore"):
        clean = check_no_lookahead(
            gs.signal_exposure, data.inputs, {"signals": [honest], **mapping}, _cuts(data)
        )
        caught = check_no_lookahead(
            gs.signal_exposure, data.inputs, {"signals": [peeking], **mapping}, _cuts(data)
        )
    assert clean == []
    assert caught


def test_every_numeric_mapping_parameter_is_read(signals_dir: Path) -> None:
    data = _data()
    ref = _write(signals_dir, "mom", MOMENTUM) | {"score": "percentile"}
    params = {"signals": [ref], "mapping": "linear", "p_low": 0.2, "p_high": 0.8}
    params |= {"low": 0.25, "high": 1.0}
    problems, unchecked = check_parameters_consumed(
        gs.signal_exposure,
        data.inputs,
        params,
        pd.Timestamp(DEV.start),
        pd.Timestamp(DEV.end),
    )
    assert problems == []
    assert set(unchecked) == {"signals", "mapping"}


# 45 ----------------------------------------------------------------------------------------------


def _spread(index: pd.DatetimeIndex) -> pd.DataFrame:
    days = index[index.month != 7]  # its own calendar: nothing in July, stale after 5 days
    rng = np.random.default_rng(11)
    values = 2.0 + np.cumsum(rng.normal(0, 0.02, len(days)))
    return pd.DataFrame({"value": values}, index=days)


def test_sources_enter_the_inputs_as_known_at_each_close(signals_dir: Path) -> None:
    data = _data()
    frame = _spread(pd.DatetimeIndex(data.inputs.index))
    extended = with_signal_sources(
        data, {"spread": frame}, {"spread": 1}, snapshot_id="sig-0123456789ab"
    )
    column = "source:spread:value"
    expected = known_at(frame["value"], pd.DatetimeIndex(data.inputs.index), 1)
    np.testing.assert_array_equal(extended.inputs[column].to_numpy(), expected.to_numpy())
    pd.testing.assert_frame_equal(extended.inputs[list(data.inputs.columns)], data.inputs)
    pd.testing.assert_frame_equal(extended.returns, data.returns)
    assert extended.source == {**data.source, "signal_snapshot_id": "sig-0123456789ab"}
    assert "signal_snapshot_id" not in data.source

    ref = _write(signals_dir, "spread_fall", SPREAD, sources=["spread"]) | {"score": "percentile"}
    params = {"signals": [ref], "mapping": "threshold", "low": 0.25, "high": 1.0}
    targets = strategy_targets(gs.signal_exposure, extended.inputs, params)
    assert targets.loc[pd.Timestamp(DEV.start) :].notna().all()
    with pytest.raises(ValueError, match="spread"):
        gs.signal_exposure(data.inputs, params)

    peeking = _write(signals_dir, "spread_peek", SPREAD_PEEKING, sources=["spread"])
    with np.errstate(all="ignore"):
        caught = check_no_lookahead(
            gs.signal_exposure,
            extended.inputs,
            {"signals": [peeking | {"score": "percentile"}], "mapping": "threshold"}
            | {"low": 0.25, "high": 1.0},
            _cuts(data),
        )
    assert caught


# 46 ----------------------------------------------------------------------------------------------


def test_the_signal_snapshot_is_fixed() -> None:
    assert DataSpec(snapshot_id="qlh-x", qqq_series="actual").signal_snapshot_id is None
    spec = DataSpec(snapshot_id="qlh-x", qqq_series="actual", signal_snapshot_id="sig-0123456789ab")
    assert spec.signal_snapshot_id == "sig-0123456789ab"
    for bad in ("LATEST", "latest", "sig-xyz", "0123456789ab"):
        with pytest.raises(ValidationError):
            DataSpec(snapshot_id="qlh-x", qqq_series="actual", signal_snapshot_id=bad)


def test_screens_are_cited_by_number() -> None:
    raw = load_prereg(PROJECT_ROOT / "research" / "QQQ-Q1" / "prereg.yaml").model_dump()
    assert Prereg.model_validate(raw | {"screens": ["S0001", "S0012"]}).screens == (
        "S0001",
        "S0012",
    )
    assert Prereg.model_validate(raw | {"screen_family_size": 7}).screen_family_size == 7
    for bad in ({"screens": ["1"]}, {"screens": ["S01"]}, {"screen_family_size": 0}):
        with pytest.raises(ValidationError):
            Prereg.model_validate(raw | bad)


def test_qqq_q1_reads_as_before() -> None:
    prereg = load_prereg(PROJECT_ROOT / "research" / "QQQ-Q1" / "prereg.yaml")
    assert prereg.data.signal_snapshot_id is None
    assert prereg.screens == () and prereg.screen_family_size is None
    assert set(_data().source) == {"snapshot_id", "qqq_series"}


# 47 ----------------------------------------------------------------------------------------------


def above_ma(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """The same rule written by hand: fully in when the price is at or above its 210-day mean."""

    price = np.exp(np.log1p(inputs["ndx_return"]).cumsum())
    gap = price / price.rolling(210).mean() - 1
    return pd.Series(np.where(gap.isna(), np.nan, (gap >= 0).astype(float)), index=inputs.index)


def test_the_generic_strategy_equals_the_rule_written_by_hand(signals_dir: Path) -> None:
    data = _data()
    ref = _write(signals_dir, "ma_gap", MA_GAP) | {"score": "sign"}
    params = {"signals": [ref], "mapping": "threshold", "low": 0.0, "high": 1.0}
    generic = strategy_targets(gs.signal_exposure, data.inputs, params)
    by_hand = strategy_targets(above_ma, data.inputs, {})
    np.testing.assert_array_equal(generic.to_numpy(), by_hand.to_numpy())

    def run(strategy: Any, run_params: Mapping[str, Any]) -> Any:
        spec = RunSpec(
            question_type="qqq_direction",
            strategy_id="s",
            params=run_params,
            window_role="development",
            rebalance=Rebalance(schedule="month_end"),
        )
        with np.errstate(all="ignore"):
            return run_strategy(data, strategy, spec, LOADED)

    left, right = run(gs.signal_exposure, params), run(above_ma, {})
    assert left.strategy == right.strategy
    assert left.comparison == right.comparison
    assert left.checks["problems"] == []
    assert set(left.checks["parameters_unchecked"]) == {"signals", "mapping"}


def test_the_bench_settings_file_is_unchanged() -> None:
    path = PROJECT_ROOT / "config" / "research" / "research_bench.yaml"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == BENCH_SETTINGS_SHA256
