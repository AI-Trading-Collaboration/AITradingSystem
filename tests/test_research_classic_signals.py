"""Classic trend signals for QQQ-Q2 (TRADING-2565-11): preregistered specs, formulas, no look-ahead,
the exposures the Q2 mapping gives, and the append-only record lines. Synthetic data only."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import checks, screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.signal_strategy import signal_exposure

MOMENTUM = ("tsmom_21", "tsmom_63", "tsmom_126", "tsmom_252")
SIGNALS = (*MOMENTUM, "ma_distance_210")
# The specification text was fixed before any result existed; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "tsmom_21": "b3a7d3e36f3a418f614ce08ace10d8e7cbf800ebad026ea9d40249289e757934",
    "tsmom_63": "23ac5e338bb5265af00d78c633c1a4ae32f861e4963dff58f82ea5a281f70992",
    "tsmom_126": "0309640809301bd11d6e38e107085501aa0acc14f7577847b9e05be97db20938",
    "tsmom_252": "e1e85a47a3cf93dd72ba8d27232d3d8cc5c0fb53545b667cd577a3751cf4e62a",
    "ma_distance_210": "f8213b4edf41cd51ac13d548e08c5e277ca6cf94eacebe4ce07cbf6e2e0b917a",
}
EXPECTED = {
    "tsmom_21": {"window": 21},
    "tsmom_63": {"window": 63},
    "tsmom_126": {"window": 126},
    "tsmom_252": {"window": 252},
    "ma_distance_210": {"window": 210},
}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _run(signal_id: str, returns: pd.Series) -> pd.Series:
    loaded = _load(signal_id)
    function: Signal = loaded.function
    return function(returns.copy(), {}, dict(loaded.spec.params))


def _returns(n: int, seed: int, start: str = "1999-01-04") -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(0.0004, 0.012, n), index=pd.bdate_range(start, periods=n))


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_specification_is_the_preregistered_text(signal_id: str) -> None:
    loaded = _load(signal_id)
    assert loaded.spec.signal_id == signal_id
    assert (loaded.spec.expected_sign, loaded.spec.target) == (1, "return")
    assert loaded.spec.params == EXPECTED[signal_id] and loaded.spec.sources == ()
    assert loaded.spec.markets == "all"
    assert loaded.spec.mechanism.strip() and loaded.spec.literature.strip() != "none"
    assert loaded.spec_sha256 == SPEC_SHA256[signal_id]
    raw = yaml.safe_load((screen.SIGNALS_DIR / f"{signal_id}.yaml").read_text(encoding="utf-8"))
    assert set(raw) == {
        "signal_id",
        "mechanism",
        "expected_sign",
        "target",
        "params",
        "sources",
        "markets",
        "literature",
    }


def test_the_four_momentum_modules_are_one_file_under_four_names() -> None:
    texts = {
        signal_id: (screen.SIGNALS_DIR / f"{signal_id}.py").read_bytes() for signal_id in MOMENTUM
    }
    assert len(set(texts.values())) == 1
    assert len({_load(signal_id).module_sha256 for signal_id in MOMENTUM}) == 1


def test_momentum_is_the_compounded_return_over_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    function: Signal = _load("tsmom_21").function
    returns = pd.Series([0.10, -0.10, 0.20, -0.05, 0.0], index=index)
    got = function(returns, {}, {"window": 2})
    expected = [np.nan, 1.1 * 0.9 - 1, 0.9 * 1.2 - 1, 1.2 * 0.95 - 1, 0.95 * 1.0 - 1]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.iloc[1] < 0 < got.iloc[2] and got.iloc[4] < 0
    got = function(returns, {}, {"window": 3})
    expected = [np.nan, np.nan, 1.1 * 0.9 * 1.2 - 1, 0.9 * 1.2 * 0.95 - 1, 1.2 * 0.95 * 1.0 - 1]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)


def test_momentum_is_missing_while_a_missing_return_is_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    function: Signal = _load("tsmom_21").function
    returns = pd.Series([0.1, np.nan, 0.1, 0.1, 0.0, -0.1], index=index)
    got = function(returns, {}, {"window": 2})
    expected = [np.nan, np.nan, np.nan, 1.1 * 1.1 - 1, 1.1 * 1.0 - 1, 1.0 * 0.9 - 1]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)


@pytest.mark.parametrize("signal_id", MOMENTUM)
def test_momentum_equals_a_naive_product_over_the_declared_window(signal_id: str) -> None:
    returns = _returns(500, seed=17)
    window = EXPECTED[signal_id]["window"]
    values = returns.to_numpy()
    naive = np.full(len(values), np.nan)
    for t in range(window - 1, len(values)):
        product = 1.0
        for r in values[t - window + 1 : t + 1]:
            product *= 1.0 + r
        naive[t] = product - 1.0
    got = _run(signal_id, returns)
    np.testing.assert_allclose(got.to_numpy(), naive, atol=1e-10, equal_nan=True)
    assert got.isna().sum() == window - 1


def test_distance_is_price_over_its_trailing_average_minus_one() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    function: Signal = _load("ma_distance_210").function
    returns = pd.Series([0.0, 0.10, -0.10, 0.20], index=index)
    got = function(returns, {}, {"window": 3})
    expected = [
        np.nan,
        np.nan,
        0.99 / ((1.0 + 1.1 + 0.99) / 3) - 1,
        1.188 / ((1.1 + 0.99 + 1.188) / 3) - 1,
    ]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.iloc[2] < 0 < got.iloc[3]


def test_distance_is_missing_while_a_missing_return_is_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    function: Signal = _load("ma_distance_210").function
    returns = pd.Series([0.0, 0.1, np.nan, 0.1, 0.0, 0.0], index=index)
    got = function(returns, {}, {"window": 2})
    np.testing.assert_allclose(
        got.to_numpy(),
        [np.nan, 1.1 / 1.05 - 1, np.nan, np.nan, 0.0, 0.0],
        atol=1e-12,
        equal_nan=True,
    )


def test_distance_is_the_screening_baseline_ma_10m_distance() -> None:
    returns = _returns(900, seed=29)
    got = _run("ma_distance_210", returns)
    baseline = screen.baseline_signals(returns)["ma_10m_distance"]
    np.testing.assert_allclose(got.to_numpy(), baseline.to_numpy(), atol=1e-12, equal_nan=True)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_a_value_never_depends_on_later_data(signal_id: str) -> None:
    returns = _returns(700, seed=11)
    full = _run(signal_id, returns)
    for cut in (80, 260, 411):
        part = _run(signal_id, returns.iloc[:cut])
        pd.testing.assert_series_equal(part, full.iloc[:cut], check_exact=True)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_output_is_a_finite_float_series_on_the_input_index(signal_id: str) -> None:
    returns = _returns(900, seed=5)
    got = _run(signal_id, returns)
    assert isinstance(got, pd.Series) and got.index.equals(returns.index)
    assert got.dtype == float and not np.isinf(got.to_numpy()).any()
    assert got.notna().sum() > 600


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_inputs_are_not_modified(signal_id: str) -> None:
    returns = _returns(400, seed=3)
    before = returns.copy()
    loaded = _load(signal_id)
    function: Signal = loaded.function
    function(returns, {}, dict(loaded.spec.params))
    pd.testing.assert_series_equal(returns, before)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_window_parameter_changes_the_output(signal_id: str) -> None:
    returns = _returns(1500, seed=21)
    loaded = _load(signal_id)
    function: Signal = loaded.function
    window = loaded.spec.params["window"]
    base = function(returns, {}, {"window": window})
    other = function(returns, {}, {"window": window + 1})
    assert not np.allclose(base.to_numpy(), other.to_numpy(), equal_nan=True, atol=1e-12)


# The mappings QQQ-Q2 preregisters (research/QQQ-Q2/prereg.yaml): what they give on synthetic data.
def _ref(signal_id: str, score: str) -> dict[str, str]:
    loaded = _load(signal_id)
    return {
        "id": signal_id,
        "module_sha256": loaded.module_sha256,
        "spec_sha256": loaded.spec_sha256,
        "score": score,
    }


def _trend4() -> dict[str, Any]:
    signals = [_ref(signal_id, "sign") for signal_id in MOMENTUM]
    return {"signals": signals, "mapping": "linear", "low": 0, "high": 1}


def _ma10() -> dict[str, Any]:
    signals = [_ref("ma_distance_210", "sign")]
    return {"signals": signals, "mapping": "threshold", "low": 0, "high": 1}


def _gate() -> dict[str, Any]:
    signals = [_ref("credit_spread_level", "percentile")]
    return {"signals": signals, "mapping": "threshold", "low": 0.5, "high": 1.0, "cut": 0.2}


def _inputs(returns: pd.Series, spread: pd.Series | None = None) -> pd.DataFrame:
    frame = pd.DataFrame({"ndx_return": returns})
    if spread is not None:
        frame["source:baa10y:value"] = spread
    return frame


def _trailing(values: np.ndarray, window: int, t: int) -> float:
    product = 1.0
    for r in values[t - window + 1 : t + 1]:
        product *= 1.0 + r
    return product - 1.0


def test_trend4_is_the_share_of_positive_lookbacks() -> None:
    returns = _returns(700, seed=8)
    values = returns.to_numpy()
    got = signal_exposure(_inputs(returns), _trend4())
    assert got.isna().sum() == 251
    for t in range(251, len(values), 37):
        positive = sum(_trailing(values, w, t) > 0 for w in (21, 63, 126, 252))
        assert got.iloc[t] == pytest.approx(positive / 4, abs=1e-12), t
    assert set(np.unique(got.dropna())) <= {0.0, 0.25, 0.5, 0.75, 1.0}


def test_trend4_by_hand_a_fall_after_a_long_rise() -> None:
    values = np.array([0.002] * 300 + [-0.01] * 30)
    returns = pd.Series(values, index=pd.bdate_range("2000-01-03", periods=len(values)))
    got = signal_exposure(_inputs(returns), _trend4())
    # 21 days: all down. 63: 33 up days at 0.2% against 30 down days at 1% is a loss. 126: also a
    # loss. 252: 222 up days at 0.2% outweigh the fall. One lookback in four is positive.
    assert 1.002**33 * 0.99**30 < 1 and 1.002**96 * 0.99**30 < 1 and 1.002**222 * 0.99**30 > 1
    assert got.iloc[-1] == pytest.approx(0.25, abs=1e-12)
    assert got.iloc[299] == pytest.approx(1.0, abs=1e-12)
    falling = pd.Series(-0.001, index=returns.index)
    assert signal_exposure(_inputs(falling), _trend4()).iloc[-1] == 0.0


def test_ma10_is_in_above_the_average_and_out_below_it() -> None:
    returns = _returns(700, seed=4)
    price = (1.0 + returns).cumprod().to_numpy()
    got = signal_exposure(_inputs(returns), _ma10())
    assert got.isna().sum() == 209
    for t in range(209, len(price)):
        average = price[t - 209 : t + 1].mean()
        assert got.iloc[t] == (1.0 if price[t] > average else 0.0), t


def test_the_credit_gate_halves_exposure_in_the_top_fifth_of_its_own_history() -> None:
    rng = np.random.default_rng(13)
    returns = _returns(1500, seed=6)
    spread = pd.Series(2.5 + np.cumsum(rng.normal(0.0, 0.05, 1500)), index=returns.index)
    got = signal_exposure(_inputs(returns, spread), _gate())
    warmup = load_screen_settings().settings.screen.warmup_days
    percentile = screen.expanding_percentile(spread, warmup)
    assert got.isna().sum() == int(percentile.isna().sum())
    valid = percentile.notna() & ((percentile - 0.8).abs() > 1e-9)
    assert set(np.unique(got[valid])) == {0.5, 1.0}
    assert (got[valid & (percentile > 0.8)] == 0.5).all()
    assert (got[valid & (percentile < 0.8)] == 1.0).all()


@pytest.mark.parametrize("name", ["trend4", "ma10", "gate"])
def test_the_preregistered_mappings_read_every_numeric_parameter_and_look_ahead_of_nothing(
    name: str,
) -> None:
    params = {"trend4": _trend4, "ma10": _ma10, "gate": _gate}[name]()
    rng = np.random.default_rng(31)
    returns = _returns(1800, seed=9)
    spread = pd.Series(2.5 + np.cumsum(rng.normal(0.0, 0.05, 1800)), index=returns.index)
    inputs = _inputs(returns, spread)
    start, end = pd.Timestamp("2002-01-02"), pd.Timestamp("2005-12-30")
    problems, unchecked = checks.check_parameters_consumed(
        signal_exposure, inputs, params, start, end
    )
    assert problems == [] and set(unchecked) == {"mapping", "signals"}
    dates = checks.cut_dates(pd.DatetimeIndex(inputs.index), start, end, 20)
    assert checks.check_no_lookahead(signal_exposure, inputs, params, dates) == []


def test_the_signals_readme_names_the_classic_signals() -> None:
    text = (screen.SIGNALS_DIR / "README.md").read_text(encoding="utf-8")
    assert all(signal_id in text for signal_id in SIGNALS)
    assert "TRADING-2565-11" in text and "dataclass" in text


# The record lines are appended, never edited: earlier sentences stay where they are.
DOC = PROJECT_ROOT / "docs/requirements/TRADING-2565_Research_Bench.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2565_RESEARCH_TRIAL_LEDGER_MULTIPLE_TESTING.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
DOC_LINE = (
    "- 2026-10-15：S4e 的第一批真实筛选已跑完，四条记录 S0001 到 S0004 已发布到 main 71f00bc1："
    "credit_spread_level 是候选，credit_spread_change_63 淘汰，term_spread_level 和 "
    "high_distance_252 证据不足。S4f（TRADING-2565-11）：经典趋势信号 tsmom_21、tsmom_63、"
    "tsmom_126、tsmom_252 和 ma_distance_210 的模块与规格，供第二题（QQQ-Q2）的预注册按哈希"
    "钉住；它们是固定基准，不进筛选账。只用合成数据验收，不读真实快照。"
    "production_effect=none；broker_action=none。"
)
NOTES_TAIL = (
    "；2026-10-15：S4e 第一批真实筛选已跑完并发布为记录 S0001 到 S0004（main 71f00bc1）；"
    "S4f（TRADING-2565-11）第二题用的经典趋势信号"
)
# The card's current owner and step (TRADING-2565-28), checked against the register.
NEXT_OWNER = "Claude（研究自动循环）"
NEXT_STEP = (
    "研究自动循环去掉产量上限、改成定期复盘"
    "（owner_decision:TRADING-2565:2026-10-19:no_output_caps，章程第 14 节）；"
    "第三批筛选信号 absorption_shift_63、industry_dispersion_21、defensive_lead_63"
    "（TRADING-2565-28）上 main 后筛选，候选写试跑计划，主配置过门才叫 Baker"
)


def test_the_requirement_progress_gets_exactly_one_new_line_after_the_s4e_line() -> None:
    lines = DOC.read_text(encoding="utf-8").rstrip("\n").splitlines()
    assert lines.count(DOC_LINE) == 1
    previous = lines[lines.index(DOC_LINE) - 1]
    assert "S4e（TRADING-2565-10）：第一批真实信号" in previous


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    assert card["notes"].count(NOTES_TAIL) == 1
    before = card["notes"].split(NOTES_TAIL)[0]
    assert before.endswith("S4e（TRADING-2565-10）第一批真实信号")
    assert "发布阻塞待核验" in before
    assert (card["next_owner"], card["next_step"]) == (NEXT_OWNER, NEXT_STEP)
    assert card["status"] == "IN_PROGRESS"
    register = REGISTER.read_text(encoding="utf-8")
    assert NOTES_TAIL in register and NEXT_STEP in register
