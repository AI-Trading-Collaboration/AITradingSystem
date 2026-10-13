"""QQQ-Q1 (TRADING-2567): the preregistration is complete, every strategy runs on the development
window with every parameter read, and σ* uses only data from before that window."""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, run_and_record
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.prereg import rule_problems

LOADED = load_settings()
QUESTION_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q1")
QUESTION = lg.load_question(QUESTION_DIR)
QQQ_START = pd.Timestamp("1999-03-10")


def _data() -> BenchData:
    """Price index from 1986, the QQQ series from its first trading day, like the snapshot."""

    index = pd.bdate_range("1986-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(7)
    regime = np.where((index.year >= 2000) & (index.year <= 2002), 0.03, 0.012)
    r = rng.normal(0.0004, 1.0, len(index)) * regime
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "etf_reference_return": np.where(index > QQQ_START, r, np.nan),
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    source = {"snapshot_id": QUESTION.prereg.data.snapshot_id}
    return from_daily_frame(daily, qqq_series="actual", source=source)


DATA = _data()


def test_the_preregistration_is_complete_and_its_rules_can_be_met() -> None:
    prereg = QUESTION.prereg
    assert prereg.owner_decision == "owner_decision:TRADING-2567:2026-10-13:qqq_q1_prereg"
    assert prereg.data.qqq_series == "actual"
    assert prereg.validation.strategies == ("q1a", "q1b")
    assert prereg.validation.confidence == 0.975
    assert rule_problems(prereg, LOADED.settings) == []
    assert len(prereg.strategies) <= prereg.trial_cap
    for entry in prereg.strategies:
        assert callable(lg.load_strategy(QUESTION, entry.id))


def test_every_strategy_runs_on_the_development_window_with_every_parameter_read(
    tmp_path: Path,
) -> None:
    directory = tmp_path.joinpath("QQQ-Q1")
    shutil.copytree(QUESTION_DIR, directory)
    question = lg.load_question(directory)
    for entry in question.prereg.strategies:
        recorded = run_and_record(question, DATA, entry.id, loaded=LOADED)
        assert recorded.record["status"] == "ok", entry.id
        assert recorded.record["result"]["checks"]["parameters_unchecked"] == []
        exposure = recorded.record["result"]["strategy"]["average_exposure"]
        assert 0.0 < exposure <= (3.0 if entry.params["leveraged"] else 1.0)


@pytest.mark.parametrize("strategy_id", ["q1a", "q1b", "q1a_ewma"])
def test_target_vol_reads_only_dates_before_the_development_window(strategy_id: str) -> None:
    entry = QUESTION.prereg.strategy(strategy_id)
    strategy = lg.load_strategy(QUESTION, strategy_id)
    cap = 3.0 if entry.params["leveraged"] else 1.0
    base = strategy(DATA.inputs, entry.params)
    before = base.index < QQQ_START

    later = DATA.inputs.copy()
    later.loc[~before, "ndx_return"] *= 2.0
    moved = strategy(later, entry.params)
    assert np.allclose(base[before], moved[before], equal_nan=True)
    # Same σ*, twice the σ̂ once the estimate holds only later returns: uncapped targets halve.
    settled = (base.index >= QQQ_START + pd.DateOffset(years=1)) & (base < cap)
    assert settled.sum() > 1000
    assert np.allclose(moved[settled], base[settled] / 2.0)

    earlier = DATA.inputs.copy()
    earlier.loc[before, "ndx_return"] *= 2.0
    assert not np.allclose(strategy(earlier, entry.params)[~before], base[~before], equal_nan=True)
