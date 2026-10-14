"""Invariants of signal screening and the global screening ledger (TRADING-2565 S4b).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-13:signal_screen_layer``). All data is synthetic.

34. The screen never reads past the screen end: data after 2010-12-31 can be anything and the
    result does not move, for every market and source; a source enters at its known time.
35. Forward alignment: a signal equal to the coming return (or, for a risk target, the coming
    volatility) has rank correlation 1 and the smallest possible p-value; independent noise has a
    correlation far inside the minimum detectable effect; the expanding percentile, the group
    table, the leave-out and the partial correlation agree with hand computations.
36. A signal module that looks ahead is refused, and the baselines themselves do not look ahead
    and follow their formulas.
37. A copy of a baseline is seen as one.
38. The ledger is a hash chain that shows edits, deletions and reordering and survives Windows
    line ends; the same signal on the same data is refused; a changed spec is a new family
    member; Benjamini-Hochberg is applied over the whole family.
39. The verdict follows the approved rules at their boundaries, and a QQQ-only signal is never a
    candidate.
40. The minimum detectable effect is the closed form; results hold summaries only;
    ``config/research/research_bench.yaml`` is unchanged.
"""

from __future__ import annotations

import functools
import hashlib
import json
import tempfile
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.screen import (
    ScreenCheckError,
    ScreenMarket,
    baseline_signals,
    bh_adjust,
    expanding_percentile,
    load_signal,
    market_statistics,
    minimum_detectable_ic,
    screen_signal,
    screen_verdict,
)
from ai_trading_system.research_bench.screen_ledger import (
    append_screen_record,
    family_key,
    screen_ledger_summary,
    screen_records,
    verify_screen_ledger,
)
from ai_trading_system.research_bench.screen_settings import load_screen_settings

BENCH_SETTINGS_SHA256 = "69351836d2e2bacb8b2888fe783a0949f485af9bf3e0294ffdb78ba50d0c46c2"
APPROVED = load_screen_settings().settings.screen
FAST = APPROVED.model_copy(update={"bootstrap_draws": 200})
END = pd.Timestamp("2010-12-31")


def _markets(seed: int = 3) -> dict[str, ScreenMarket]:
    rng = np.random.default_rng(seed)
    us_index = pd.bdate_range("1926-07-01", "2015-12-31")
    market = pd.Series(rng.normal(0.0003, 0.01, len(us_index)), index=us_index)
    industries = {
        f"industry_{name}": market + pd.Series(rng.normal(0, 0.006, len(us_index)), index=us_index)
        for name in ("agric", "food", "soda")
    }
    qqq_index = pd.bdate_range("1986-01-02", "2015-12-31")
    qqq = 1.3 * market.reindex(qqq_index) + pd.Series(
        rng.normal(0, 0.008, len(qqq_index)), index=qqq_index
    )
    out = {
        "qqq": ScreenMarket("qqq", qqq, date(1999, 3, 10)),
        "us_market": ScreenMarket("us_market", market, date(1927, 7, 1)),
    }
    out.update({k: ScreenMarket(k, v, date(1927, 7, 1)) for k, v in industries.items()})
    return out


def _write_signal(directory: Path, signal_id: str, body: str, **spec: Any) -> None:
    yaml_lines = {
        "signal_id": signal_id,
        "mechanism": "test signal",
        "expected_sign": 1,
        "target": "return",
        "params": {},
        "sources": [],
        "markets": "all",
        "literature": "none",
    } | spec
    (directory / f"{signal_id}.py").write_text(body, encoding="utf-8")
    (directory / f"{signal_id}.yaml").write_text(json.dumps(yaml_lines), encoding="utf-8")


MOMENTUM = """
import numpy as np
import pandas as pd


def signal(returns, sources, params):
    return np.log1p(returns).rolling(63).sum()
"""

PEEKING = """
def signal(returns, sources, params):
    return returns.shift(-5).rolling(5).sum()
"""

COPY = """
from ai_trading_system.research_bench.screen import baseline_signals


def signal(returns, sources, params):
    return baseline_signals(returns)["mom_12_1"]
"""

SOURCE_READER = """
def signal(returns, sources, params):
    return sources["spread"]["value"]
"""


def _forward(returns: pd.Series, h: int) -> pd.Series:
    logs = np.log1p(returns.to_numpy())
    cum = np.concatenate([[0.0], np.cumsum(logs)])
    out = np.full(len(logs), np.nan)
    out[: len(logs) - h] = cum[1 + h :] - cum[1 : len(logs) - h + 1]
    return pd.Series(out, index=returns.index)


def _forward_vol(returns: pd.Series, h: int) -> pd.Series:
    return returns.rolling(h).std().shift(-h) * np.sqrt(252)


def _forward_drawdown(returns: pd.Series, h: int) -> pd.Series:
    logs = np.log1p(returns.to_numpy())
    out = np.full(len(logs), np.nan)
    for t in range(len(logs) - h):
        path = np.concatenate([[0.0], np.cumsum(logs[t + 1 : t + 1 + h])])
        out[t] = np.expm1((path - np.maximum.accumulate(path)).min())
    return pd.Series(out, index=returns.index)


def _ranks(values: Any) -> np.ndarray:
    return pd.Series(np.asarray(values, dtype=float)).rank(method="average").to_numpy()


def _rank_ic(x: Any, y: Any) -> float:
    return float(np.corrcoef(_ranks(x), _ranks(y))[0, 1])


@functools.cache
def _mom63_result() -> dict[str, Any]:
    directory = Path(tempfile.mkdtemp())
    _write_signal(directory, "mom63", MOMENTUM)
    return screen_signal(load_signal("mom63", directory=directory), _markets(), {}, {}, FAST)


# 34 ----------------------------------------------------------------------------------------------


def test_the_screen_never_reads_past_the_screen_end(tmp_path: Path) -> None:
    _write_signal(tmp_path, "mom63", MOMENTUM)
    loaded = load_signal("mom63", directory=tmp_path)
    base = screen_signal(loaded, _markets(), {}, {}, FAST)
    poisoned = _markets()
    for market in poisoned.values():
        market.returns[market.returns.index > END] = 0.5
    moved = screen_signal(loaded, poisoned, {}, {}, FAST)
    assert json.dumps(moved, sort_keys=True) == json.dumps(base, sort_keys=True)
    cut = {
        key: ScreenMarket(key, market.returns[market.returns.index <= END], market.eval_start)
        for key, market in _markets().items()
    }
    shorter = screen_signal(loaded, cut, {}, {}, FAST)
    assert json.dumps(shorter, sort_keys=True) == json.dumps(base, sort_keys=True)


def test_market_statistics_cut_at_the_screen_end() -> None:
    returns, signal = _noise()
    base = _stats(signal, returns)
    returns[returns.index > END] = 0.5
    signal[signal.index > END] = 9.0
    assert _stats(signal, returns) == base


def test_a_source_enters_at_its_known_time(tmp_path: Path) -> None:
    _write_signal(tmp_path, "spread", SOURCE_READER, sources=["spread"])
    loaded = load_signal("spread", directory=tmp_path)
    market = _markets()["us_market"].returns
    market = market[market.index >= pd.Timestamp("1990-01-01")]
    published = pd.DataFrame({"value": market.to_numpy()}, index=market.index)
    lagged = screen_signal(loaded, _markets(), {"spread": published}, {"spread": 1}, FAST)
    by_hand = published.shift(1)
    by_hand[by_hand.index > END] = 0.5
    same = screen_signal(loaded, _markets(), {"spread": by_hand}, {"spread": 0}, FAST)
    assert json.dumps(lagged["markets"], sort_keys=True) == json.dumps(
        same["markets"], sort_keys=True
    )
    with pytest.raises(ScreenCheckError):
        screen_signal(loaded, _markets(), {}, {}, FAST)


# 35 ----------------------------------------------------------------------------------------------


def _noise(seed: int = 0) -> tuple[pd.Series, pd.Series]:
    index = pd.bdate_range("1995-01-02", "2015-12-31")
    rng = np.random.default_rng(seed)
    returns = pd.Series(rng.normal(0.0003, 0.012, len(index)), index=index)
    signal = pd.Series(rng.normal(size=len(index)), index=index)
    return returns, signal


def _stats(signal: pd.Series, returns: pd.Series) -> dict[str, Any]:
    return market_statistics(
        signal,
        returns,
        eval_start=date(1999, 3, 10),
        screen_end=date(2010, 12, 31),
        settings=FAST,
        expected_sign=1,
        target="return",
        bootstrap=True,
    )


def test_a_signal_equal_to_the_coming_return_is_found() -> None:
    returns, _ = _noise()
    oracle = _forward(returns[returns.index <= END], 21).reindex(returns.index)
    stats = _stats(oracle, returns)["horizons"]["21"]
    assert stats["ic"] == pytest.approx(1.0, abs=1e-9)
    assert stats["quintiles"][4]["fwd_return"] > stats["quintiles"][0]["fwd_return"]
    assert stats["p_value"] == pytest.approx(1 / (FAST.bootstrap_draws + 1))
    assert stats["monotonicity"] == pytest.approx(1.0)


def test_a_signal_equal_to_the_coming_volatility_is_found_for_a_risk_target() -> None:
    returns, _ = _noise()
    oracle = _forward_vol(returns[returns.index <= END], 21).reindex(returns.index)
    stats = market_statistics(
        oracle,
        returns,
        eval_start=date(1999, 3, 10),
        screen_end=date(2010, 12, 31),
        settings=FAST,
        expected_sign=1,
        target="risk",
        bootstrap=False,
    )["horizons"]["21"]
    assert stats["ic"] == pytest.approx(1.0, abs=1e-9)
    assert stats["monotonicity"] == pytest.approx(1.0)


def test_the_expanding_percentile_by_hand() -> None:
    values = pd.Series([3.0, 1.0, 2.0, 2.0, np.nan, 5.0, 2.0])
    expected = [np.nan, np.nan, 0.5, 0.5, np.nan, 1.0, 0.4]
    np.testing.assert_allclose(expanding_percentile(values, 2).to_numpy(), expected)
    later = values.copy()
    later.iloc[6] = -4.0
    np.testing.assert_allclose(expanding_percentile(later, 2).to_numpy()[:6], expected[:6])


def test_the_group_table_by_hand() -> None:
    returns, _ = _noise()
    returns = returns * 2  # wide enough swings that some 21-day drawdowns pass -10%
    rising = pd.Series(np.arange(len(returns), dtype=float), index=returns.index)
    stats = _stats(rising, returns)["horizons"]["21"]
    cut = returns[returns.index <= END]
    days = cut.index >= pd.Timestamp("1999-03-10")
    forward = _forward(cut, 21)[days].dropna()
    vol = _forward_vol(cut, 21)[days].dropna()
    drawdown = _forward_drawdown(cut, 21)[days].dropna()
    groups = stats["quintiles"]
    assert [g["group"] for g in groups] == [1, 2, 3, 4, 5]
    assert [g["days"] for g in groups] == [0, 0, 0, 0, len(forward)]
    assert groups[4]["fwd_return"] == pytest.approx(forward.mean() * 252 / 21)
    assert groups[4]["fwd_vol"] == pytest.approx(vol.mean())
    assert groups[4]["drawdown_share"] == pytest.approx((drawdown <= -0.10).mean())
    assert 0.02 < groups[4]["drawdown_share"] < 0.98
    assert groups[0]["fwd_return"] is None and groups[0]["drawdown_share"] is None
    assert stats["monotonicity"] is None


def test_too_few_days_give_no_statistics() -> None:
    returns, signal = _noise()
    stats = market_statistics(
        signal,
        returns,
        eval_start=date(2010, 3, 1),
        screen_end=date(2010, 12, 31),
        settings=FAST,
        expected_sign=1,
        target="return",
        bootstrap=True,
    )
    for item in stats["horizons"].values():
        assert 0 < item["days"] < FAST.min_days
        for key in ("ic", "mde", "ic_low", "ic_high", "p_value", "monotonicity"):
            assert item[key] is None
        assert sum(g["days"] for g in item["quintiles"]) == item["days"]


def test_leave_out_and_partial_correlation_by_hand() -> None:
    result = _mom63_result()
    for name, unit in (("us_market", "decade"), ("qqq", "year")):
        market = _markets()[name]
        returns = market.returns[market.returns.index <= END]
        signal = np.log1p(returns).rolling(63).sum()
        forward = _forward(returns, 21)
        keep = (returns.index >= pd.Timestamp(market.eval_start)) & signal.notna() & forward.notna()
        x, y = signal[keep].to_numpy(), forward[keep].to_numpy()
        index = returns.index[keep]
        out = result["markets"][name]
        assert out["horizons"]["21"]["ic"] == pytest.approx(_rank_ic(x, y), abs=1e-9)
        assert out["first_day"] == index[0].date().isoformat()
        assert out["last_day"] == index[-1].date().isoformat()

        units = index.year if unit == "year" else index.year // 10 * 10
        left_out = [_rank_ic(x[units != u], y[units != u]) for u in sorted(set(units))]
        assert out["leave_out"]["unit"] == unit
        assert out["leave_out"]["count"] == len(left_out) == (12 if unit == "year" else 10)
        assert out["leave_out"]["min_ic"] == pytest.approx(min(left_out), abs=1e-9)
        assert out["leave_out"]["max_ic"] == pytest.approx(max(left_out), abs=1e-9)
        assert out["leave_out"]["stable"] is all(v > 0 for v in left_out)

        base = baseline_signals(returns)
        controls = [_ranks(base[k][keep]) for k in ("mom_12_1", "ma_10m_distance", "rv_21")]
        for k, b in base.items():
            assert out["baseline_corr"][k] == pytest.approx(_rank_ic(x, b[keep]), abs=1e-9)
        design = np.column_stack([np.ones(len(x)), *controls])

        def residual(v: np.ndarray, design: np.ndarray = design) -> np.ndarray:
            return v - design @ np.linalg.lstsq(design, v, rcond=None)[0]

        partial = np.corrcoef(residual(_ranks(x)), residual(_ranks(y)))[0, 1]
        assert out["partial_ic"] == pytest.approx(partial, abs=1e-9)
    assert result["markets"]["industry_food"]["partial_ic"] is None


def test_independent_noise_stays_inside_the_minimum_detectable_effect() -> None:
    returns, signal = _noise()
    horizons = _stats(signal, returns)["horizons"]
    assert set(horizons) == {"21", "63", "126"}
    for item in horizons.values():
        assert abs(item["ic"]) < item["mde"] / 2
        assert item["ic_low"] <= item["ic"] <= item["ic_high"]
        assert item["n_eff"] == pytest.approx(item["days"] / int(item["horizon"]))
        assert len(item["quintiles"]) == 5


def test_bootstrap_is_only_where_asked() -> None:
    returns, signal = _noise()
    stats = market_statistics(
        signal,
        returns,
        eval_start=date(1999, 3, 10),
        screen_end=date(2010, 12, 31),
        settings=FAST,
        expected_sign=1,
        target="return",
        bootstrap=False,
    )
    for item in stats["horizons"].values():
        assert item.get("ic_low") is None and item.get("p_value") is None


# 36 ----------------------------------------------------------------------------------------------


def test_a_signal_that_looks_ahead_is_refused(tmp_path: Path) -> None:
    _write_signal(tmp_path, "peek", PEEKING)
    with pytest.raises(ScreenCheckError):
        screen_signal(load_signal("peek", directory=tmp_path), _markets(), {}, {}, FAST)


def test_baselines_do_not_look_ahead() -> None:
    returns, _ = _noise()
    base = baseline_signals(returns)
    assert set(base) == {"mom_12_1", "ma_10m_distance", "rv_21"}
    cut = pd.Timestamp("2005-06-30")
    changed = returns.copy()
    changed[changed.index > cut] = 0.3
    moved = baseline_signals(changed)
    for name, series in base.items():
        np.testing.assert_array_equal(
            moved[name][moved[name].index <= cut].to_numpy(),
            series[series.index <= cut].to_numpy(),
        )


def test_the_baselines_follow_their_formulas() -> None:
    returns, _ = _noise()
    base = baseline_signals(returns)
    r = returns.to_numpy()
    logs = np.log1p(r)
    price = np.exp(np.cumsum(logs))
    assert np.isnan(base["mom_12_1"].iloc[250]) and not np.isnan(base["mom_12_1"].iloc[251])
    for t in (400, 1500, 3000):
        assert base["mom_12_1"].iloc[t] == pytest.approx(logs[t - 251 : t - 20].sum(), abs=1e-9)
        assert base["ma_10m_distance"].iloc[t] == pytest.approx(
            price[t] / price[t - 209 : t + 1].mean() - 1, abs=1e-9
        )
        assert base["rv_21"].iloc[t] == pytest.approx(
            r[t - 20 : t + 1].std(ddof=1) * np.sqrt(252), abs=1e-9
        )


# 37 ----------------------------------------------------------------------------------------------


def test_a_copy_of_a_baseline_is_seen_as_one(tmp_path: Path) -> None:
    _write_signal(tmp_path, "copy", COPY)
    result = screen_signal(load_signal("copy", directory=tmp_path), _markets(), {}, {}, FAST)
    assert result["baseline_max_abs_corr"] == pytest.approx(1.0, abs=1e-9)
    assert result["markets"]["us_market"]["partial_ic"] is None


# 38 ----------------------------------------------------------------------------------------------


def _body(
    signal_id: str, spec: str, p_value: float, data: str = "sig-aaaaaaaaaaaa"
) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "module_sha256": hashlib.sha256(signal_id.encode()).hexdigest(),
        "spec_sha256": hashlib.sha256(spec.encode()).hexdigest(),
        "params": {},
        "markets": ["qqq", "us_market"],
        "data": {"signal_snapshot_id": data, "bench_snapshot_id": "qlh-test"},
        "code": {"commit": "0" * 40, "modified": False},
        "settings_sha256": "1" * 64,
        "results": {},
        "primary_p_value": p_value,
        "verdict": "inconclusive",
        "family_size": 0,
    }


def _ledger_dir(root: Path) -> Path:
    return root / "research" / "_screens" / "ledger"


def test_the_ledger_shows_edits_deletions_and_reordering(tmp_path: Path) -> None:
    for i, p in enumerate((0.01, 0.04, 0.03)):
        append_screen_record(tmp_path, _body(f"s{i}", "v1", p))
    assert [r["number"] for r in screen_records(tmp_path)] == [1, 2, 3]
    assert verify_screen_ledger(tmp_path) == []
    files = sorted(_ledger_dir(tmp_path).glob("*.json"))

    original = files[1].read_bytes()
    files[1].write_bytes(original.replace(b"0.04", b"0.05"))
    assert verify_screen_ledger(tmp_path)
    files[1].write_bytes(original)
    assert verify_screen_ledger(tmp_path) == []

    third = files[2].read_bytes()
    files[1].unlink()
    assert verify_screen_ledger(tmp_path)
    files[1].write_bytes(original)

    first = files[0].read_bytes()
    files[0].write_bytes(third)
    files[2].write_bytes(first)
    assert verify_screen_ledger(tmp_path)


def test_windows_line_ends_keep_the_chain(tmp_path: Path) -> None:
    append_screen_record(tmp_path, _body("s0", "v1", 0.01))
    first = _ledger_dir(tmp_path) / "0001.json"
    first.write_bytes(first.read_bytes().replace(b"\n", b"\r\n"))
    append_screen_record(tmp_path, _body("s1", "v1", 0.02))
    assert verify_screen_ledger(tmp_path) == []
    second = _ledger_dir(tmp_path) / "0002.json"
    second.write_bytes(second.read_bytes().replace(b"\n", b"\r\n"))
    assert verify_screen_ledger(tmp_path) == []


def test_the_same_signal_on_the_same_data_is_refused(tmp_path: Path) -> None:
    append_screen_record(tmp_path, _body("s", "v1", 0.2))
    with pytest.raises(ValueError):
        append_screen_record(tmp_path, _body("s", "v1", 0.2))
    append_screen_record(tmp_path, _body("s", "v1", 0.2, data="sig-bbbbbbbbbbbb"))
    append_screen_record(tmp_path, _body("s", "v2", 0.2))
    records = screen_records(tmp_path)
    assert len({family_key(r) for r in records}) == 2
    assert screen_ledger_summary(tmp_path, APPROVED)["family_size"] == 2


def test_benjamini_hochberg_runs_over_the_whole_family(tmp_path: Path) -> None:
    assert bh_adjust([0.01, 0.04, 0.03, 0.20]) == pytest.approx([0.04, 0.16 / 3, 0.16 / 3, 0.20])
    for i, p in enumerate((0.01, 0.04, 0.03, 0.20)):
        append_screen_record(tmp_path, _body(f"s{i}", "v1", p))
    summary = screen_ledger_summary(tmp_path, APPROVED)
    adjusted = {m["signal_id"]: m["adjusted_p"] for m in summary["members"]}
    assert adjusted == pytest.approx({"s0": 0.04, "s1": 0.16 / 3, "s2": 0.16 / 3, "s3": 0.20})


# 39 ----------------------------------------------------------------------------------------------

GOOD: dict[str, Any] = {
    "expected_sign": 1,
    "qqq_ic": 0.05,
    "qqq_interval": (-0.01, 0.11),
    "primary_interval": (0.01, 0.09),
    "breadth": 0.8,
    "primary_adjusted_p": 0.05,
    "leave_out_stable": True,
    "baseline_max_abs_corr": 0.3,
    "qqq_only": False,
}


@pytest.mark.parametrize(
    ("change", "verdict"),
    [
        ({}, "candidate"),
        ({"primary_interval": (-0.10, -0.02)}, "eliminated"),
        ({"breadth": 0.4}, "eliminated"),
        ({"baseline_max_abs_corr": 0.95}, "eliminated"),
        ({"primary_adjusted_p": 0.2}, "inconclusive"),
        ({"leave_out_stable": False}, "inconclusive"),
        ({"breadth": 0.6}, "inconclusive"),
        ({"qqq_ic": -0.01}, "inconclusive"),
        ({"qqq_ic": 0.0}, "inconclusive"),
        ({"breadth": 2 / 3}, "candidate"),
        ({"breadth": 0.5}, "inconclusive"),
        ({"baseline_max_abs_corr": 0.9}, "eliminated"),
        ({"primary_adjusted_p": 0.10}, "candidate"),
        ({"primary_interval": (-0.10, 0.0)}, "candidate"),
        ({"qqq_only": True}, "inconclusive"),
        ({"expected_sign": -1, "qqq_ic": -0.05, "primary_interval": (0.02, 0.10)}, "eliminated"),
        (
            {
                "expected_sign": -1,
                "qqq_ic": -0.05,
                "qqq_interval": (-0.11, 0.01),
                "primary_interval": (-0.09, -0.01),
            },
            "candidate",
        ),
    ],
)
def test_the_verdict_follows_the_approved_rules(change: dict[str, Any], verdict: str) -> None:
    assert screen_verdict(**(GOOD | change), settings=APPROVED) == verdict


def test_a_qqq_only_signal_is_never_a_candidate() -> None:
    only: dict[str, Any] = GOOD | {
        "qqq_only": True,
        "primary_interval": None,
        "breadth": None,
        "primary_adjusted_p": None,
        "leave_out_stable": None,
    }
    assert screen_verdict(**only, settings=APPROVED) == "inconclusive"
    wrong = only | {"qqq_interval": (-0.12, -0.02), "qqq_ic": -0.07}
    assert screen_verdict(**wrong, settings=APPROVED) == "eliminated"


def test_the_approved_rule_values() -> None:
    assert APPROVED.screen_end == date(2010, 12, 31)
    assert APPROVED.bh_q == 0.10
    assert APPROVED.primary_market == "us_market"
    assert APPROVED.primary_horizon == 21
    assert APPROVED.candidate_breadth == pytest.approx(2 / 3, abs=1e-6)
    assert APPROVED.eliminate_breadth == 0.5
    assert APPROVED.copy_corr == 0.9
    assert tuple(APPROVED.horizons) == (21, 63, 126)


# 40 ----------------------------------------------------------------------------------------------


def test_the_minimum_detectable_effect_is_the_closed_form() -> None:
    assert minimum_detectable_ic(100) == pytest.approx(0.2801585218, abs=1e-9)
    assert minimum_detectable_ic(400) == pytest.approx(0.2801585218 / 2, abs=1e-9)


def _lists(value: Any) -> list[int]:
    if isinstance(value, dict):
        return [n for item in value.values() for n in _lists(item)]
    if isinstance(value, list | tuple):
        return [len(value)] + [n for item in value for n in _lists(item)]
    return []


def test_results_hold_summaries_only(tmp_path: Path) -> None:
    _write_signal(tmp_path, "mom63", MOMENTUM)
    result = screen_signal(load_signal("mom63", directory=tmp_path), _markets(), {}, {}, FAST)
    text = json.dumps(result)  # plain JSON values only
    assert max(_lists(json.loads(text)), default=0) <= 60
    assert set(result["markets"]) == set(_markets())
    for market in ("qqq", "us_market"):
        assert result["markets"][market]["horizons"]["21"]["ic_low"] is not None
    assert result["markets"]["industry_food"]["horizons"]["21"].get("ic_low") is None


def test_the_bench_settings_file_is_unchanged() -> None:
    path = PROJECT_ROOT / "config" / "research" / "research_bench.yaml"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == BENCH_SETTINGS_SHA256
