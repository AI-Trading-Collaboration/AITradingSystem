"""TRADING-2565-56: the period-5 state table (3.2) on synthetic monthly data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from test_research_policy_table import GATE, LOADED, PROVENANCE, ROOT_DIR
from typer.testing import CliRunner

from ai_trading_system.research_bench.settings import EpisodeSpec
from ai_trading_system.research_bench.sources import MONTHLY_MAX_MISSING_MONTHS, monthly_known_at
from ai_trading_system.research_probes import state_table as st

MONTHS = st.months()


def _series(values: Any, start: str = st.FIRST_MONTH) -> pd.Series:
    array = np.asarray(values, dtype=float)
    return pd.Series(array, index=pd.period_range(start, periods=len(array), freq="M"))


def _frames(seed: int = 1, end: str = "2026-09") -> dict[str, pd.DataFrame]:
    """Five monthly sources shaped like the snapshot: month-start dates, a value column."""

    rng = np.random.default_rng(seed)

    def frame(start: str, base: float, step: float, vol: float) -> pd.DataFrame:
        dates = pd.date_range(start, end, freq="MS")
        values = base * np.exp(np.cumsum(rng.normal(step, vol, len(dates))))
        return pd.DataFrame({"value": values}, index=dates)

    baa = frame("1919-01-01", 6.0, 0.0, 0.02)
    aaa = baa.copy()
    aaa["value"] = baa["value"] - np.abs(rng.normal(1.0, 0.5, len(baa)))
    return {
        st.PRICE: frame("1871-01-01", 5.0, 0.003, 0.04),
        st.EARNINGS: frame("1871-01-01", 0.4, 0.002, 0.02),
        st.CPI: frame("1871-01-01", 12.0, 0.002, 0.004),
        st.BAA: baa,
        st.AAA: aaa,
    }


class _Spec:
    def __init__(self, release: int, lag: int) -> None:
        self.release_lag_months = release
        self.known_lag_trading_days = lag


SPECS = {source: _Spec(*lags) for source, lags in st.LAGS.items()}


# ---------------------------------------------------------------- alignment


@pytest.mark.parametrize(("release", "lag", "offset"), [(0, 0, 1), (15, 0, 16), (0, 5, 1)])
def test_the_month_end_value_is_the_bench_alignment_on_the_last_business_day(
    release: int, lag: int, offset: int
) -> None:
    values = _series(np.arange(1.0, 61.0), "1999-01")
    found = st.known_month_end(values, release, lag)
    calendar = pd.bdate_range("1999-01-01", "2003-12-31")
    bench = monthly_known_at(
        pd.Series(values.to_numpy(), index=values.index.to_timestamp(how="start")),
        calendar,
        release,
        lag,
    )
    ends = bench.groupby(calendar.to_period("M")).tail(1)
    assert np.array_equal(found.to_numpy(), ends.to_numpy(), equal_nan=True)
    assert np.array_equal(found.to_numpy(), values.shift(offset).to_numpy(), equal_nan=True)
    assert st.known_offsets(release, lag) == [offset]


def test_a_missing_month_is_bridged_once_then_expires() -> None:
    assert MONTHLY_MAX_MISSING_MONTHS == 1
    values = _series(np.arange(1.0, 25.0), "2001-01")
    values.iloc[5] = np.nan
    found = st.known_month_end(values, 0, 0)
    # 2001-06 is missing: July's month end still uses May's value.
    assert found.loc[pd.Period("2001-07", "M")] == 5.0
    values.iloc[6] = np.nan
    found = st.known_month_end(values, 0, 0)
    assert np.isnan(found.loc[pd.Period("2001-08", "M")])


def test_the_percentile_counts_ties_and_waits_for_its_history() -> None:
    found = st.expanding_percentile(_series([1, 3, 2, np.nan, 5, 3]), min_history=3)
    expected = [np.nan, np.nan, 2 / 3, np.nan, 1.0, 0.8]
    assert np.allclose(found.to_numpy(), expected, equal_nan=True)
    assert st.expanding_percentile(_series(np.arange(119.0))).isna().all()
    assert st.expanding_percentile(_series(np.arange(120.0))).iloc[-1] == 1.0


# ---------------------------------------------------------------- the three states


@pytest.fixture(scope="module")
def frame() -> pd.DataFrame:
    return st.monthly_frame(_frames())


@pytest.fixture(scope="module")
def states(frame: pd.DataFrame) -> pd.DataFrame:
    return st.state_frame(frame)


def test_the_frame_stops_at_2010_and_refuses_bad_sources() -> None:
    frames = _frames()
    frame = st.monthly_frame(frames)
    assert list(frame.columns) == list(st.LAGS)
    assert frame.index[0] == pd.Period("1871-01", "M") and frame.index[-1] == pd.Period(
        "2010-12", "M"
    )
    assert frame[st.BAA].first_valid_index() == pd.Period("1919-01", "M")
    with pytest.raises(ValueError, match="no shiller_cpi"):
        st.monthly_frame({k: v for k, v in frames.items() if k != st.CPI})
    with pytest.raises(ValueError, match="value column"):
        st.monthly_frame({**frames, st.BAA: frames[st.BAA].rename(columns={"value": "x"})})
    twice = pd.concat([frames[st.AAA].iloc[:3], frames[st.AAA]])
    with pytest.raises(ValueError, match="repeats a month"):
        st.monthly_frame({**frames, st.AAA: twice})


def test_values_after_2010_change_nothing(frame: pd.DataFrame) -> None:
    later = _frames()
    for source in later:
        values = later[source]["value"].to_numpy(copy=True)
        dates = later[source].index
        values[dates > pd.Timestamp("2010-12-31")] *= 3.0
        later[source] = pd.DataFrame({"value": values}, index=dates)
    assert st.monthly_frame(later).equals(frame)


def test_cape_is_the_known_price_over_the_known_real_earnings_average(
    frame: pd.DataFrame, states: pd.DataFrame
) -> None:
    month = pd.Period("1950-06", "M")
    earnings = month - 16
    window = pd.period_range(earnings - 119, earnings, freq="M")
    real = (frame[st.EARNINGS] / frame[st.CPI]).loc[window].mean()
    expected = frame[st.PRICE].loc[month - 1] / frame[st.CPI].loc[earnings] / real
    assert states.loc[month, "cape_value"] == pytest.approx(expected, rel=1e-12)
    first = states["cape_value"].first_valid_index()
    assert first == pd.Period("1871-01", "M") + 119 + 16


def test_the_spread_and_its_change(frame: pd.DataFrame, states: pd.DataFrame) -> None:
    month = pd.Period("1975-03", "M")
    spread = frame[st.BAA] - frame[st.AAA]
    assert states.loc[month, "spread_value"] == pytest.approx(spread.loc[month - 1])
    assert states.loc[month, "spread_change"] == pytest.approx(
        spread.loc[month - 1] - spread.loc[month - 7]
    )
    assert states["spread_value"].first_valid_index() == pd.Period("1919-02", "M")
    assert states["spread_pct"].first_valid_index() == pd.Period("1929-01", "M")
    assert states["widening"].first_valid_index() == pd.Period("1919-08", "M")
    assert states["cape_pct"].first_valid_index() == pd.Period("1871-01", "M") + 135 + 119


def test_the_states_follow_their_lines(states: pd.DataFrame) -> None:
    for name, pct in (("cape", "cape_pct"), ("spread", "spread_pct")):
        defined = states[pct].notna()
        assert states[name].isna().equals(~defined)
        assert (states.loc[defined, name] == (states.loc[defined, pct] >= 0.8)).all()
    change = states["spread_change"]
    assert states["widening"].isna().equals(change.isna())
    assert (states.loc[change.notna(), "widening"] == (change[change.notna()] > 0)).all()


def test_a_zero_change_is_not_widening() -> None:
    frame = pd.DataFrame(
        {
            st.PRICE: 1.0,
            st.EARNINGS: 1.0,
            st.CPI: 1.0,
            st.BAA: 5.0,
            st.AAA: 4.0,
        },
        index=MONTHS,
    )
    found = st.state_frame(frame)
    assert (found["widening"].dropna() == 0.0).all()
    # Flat values: every month ties with all its history, so the percentile is 1.
    assert (found["spread"].dropna() == 1.0).all() and (found["cape"].dropna() == 1.0).all()


def test_a_percentile_of_exactly_0_8_is_on() -> None:
    # Each cycle of five ends on a 4 for both series, so every fifth month ranks at 0.8.
    pattern = np.resize([1.0, 2.0, 3.0, 4.0, 5.0], len(MONTHS))
    frame = pd.DataFrame(
        {
            st.PRICE: pattern,
            st.EARNINGS: 1.0,
            st.CPI: 1.0,
            st.BAA: np.resize([1.0, 2.0, 3.0, 5.0, 4.0], len(MONTHS)) + 4.0,
            st.AAA: 4.0,
        },
        index=MONTHS,
    )
    found = st.state_frame(frame)
    for name, pct in (("cape", "cape_pct"), ("spread", "spread_pct")):
        exact = found[pct] == 0.8
        assert exact.sum() > 100
        assert (found.loc[exact, name] == 1.0).all()


def test_earnings_and_cpi_must_share_their_lags(frame: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match="same publication lags"):
        st.state_frame(frame, {**st.LAGS, st.CPI: (14, 0)})


# ---------------------------------------------------------------- segments


def _state(on: dict[str, float], start: str = "1960-01", end: str = "1965-12") -> pd.Series:
    index = pd.period_range(start, end, freq="M")
    series = pd.Series(0.0, index=index)
    for month, value in on.items():
        series.loc[pd.Period(month, "M")] = value
    return series


def test_marks_show_on_off_and_undefined() -> None:
    state = _state({"1962-02": 1.0, "1962-03": np.nan})
    assert st.marks(state, pd.Period("1962-01", "M"), pd.Period("1962-04", "M")) == "○●–○"


@pytest.mark.parametrize(
    ("on", "expected"),
    [
        ({"1963-03": 1.0, "1963-04": 1.0, "1963-08": 1.0}, 2),  # -3 and +2: +2 is nearer
        ({"1963-04": 1.0, "1963-08": 1.0}, -2),  # -2 and +2: the earlier wins
        ({"1961-05": 1.0}, None),  # 25 months before: outside the search
        ({"1961-06": 1.0}, -24),
        ({}, None),
        ({"1964-01": 1.0}, None),  # after the segment's end
    ],
)
def test_the_nearest_switch_on(on: dict[str, float], expected: int | None) -> None:
    state = _state(on)
    assert (
        st.nearest_switch(state, pd.Period("1963-06", "M"), pd.Period("1963-12", "M")) == expected
    )


def test_a_switch_from_undefined_counts_and_a_long_run_does_not() -> None:
    state = _state({"1963-05": np.nan, "1963-06": 1.0})
    assert st.nearest_switch(state, pd.Period("1963-06", "M"), pd.Period("1963-12", "M")) == 0
    always = pd.Series(1.0, index=pd.period_range("1960-01", "1965-12", freq="M"))
    assert st.nearest_switch(always, pd.Period("1963-06", "M"), pd.Period("1963-12", "M")) is None


def _frame_of(**states: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({name: states.get(name, states["cape"]) for name in st.STATES})


def test_slow_rows_look_six_months_back_including_the_start() -> None:
    state = _state({"1963-01": 1.0, "1963-07": 1.0, "1963-08": np.nan})
    rows = st.slow_rows(
        _frame_of(cape=state),
        [
            EpisodeSpec(id="a", start="1963-06", end="1963-09"),
            EpisodeSpec(id="b", start="1963-07", end="1963-09"),
            EpisodeSpec(id="c", start="1963-08", end="1963-08"),
        ],
    )
    a, b, c = (r["cape"] for r in rows)
    assert rows[0]["months"] == 4 and rows[2]["months"] == 1
    assert a == {
        "before": "●○○○○○",
        "before_on": True,
        "inside": "○●–○",
        "inside_on": 1,
        "start_on": False,
        "switch": 1,
    }
    # b's six months start in 1963-02: January's month is one too early.
    assert b["before"] == "○○○○○●" and b["before_on"] and b["start_on"]
    assert c["before"] == "○○○○●–" and c["before_on"] and not c["start_on"]
    assert c["inside"] == "–" and c["inside_on"] == 0


def test_bull_rows_and_the_line() -> None:
    state = _state({m: 1.0 for m in ("1961-01", "1961-02", "1961-03")})
    bull = st.bull_rows(
        _frame_of(cape=state),
        [
            EpisodeSpec(id="x", start="1961-01", end="1961-04"),
            EpisodeSpec(id="y", start="1962-01", end="1962-02"),
        ],
    )
    assert [r["cape"] for r in bull["rows"]] == [{"on": 3, "share": 0.75}, {"on": 0, "share": 0.0}]
    assert bull["pooled"]["months"] == 6
    assert bull["pooled"]["cape"] == {"on": 3, "share": 0.5}

    def slow(count: int) -> list[dict[str, Any]]:
        return [{n: {"before_on": i < count} for n in st.STATES} for i in range(10)]

    def pooled(share: float | None) -> dict[str, Any]:
        return {"pooled": {n: {"on": 0, "share": share} for n in st.STATES}}

    assert st.line(slow(8), pooled(0.49))["cape"] == {
        "slow_on": 8,
        "slow_count": 10,
        "bull_share": 0.49,
        "passes": True,
    }
    assert not st.line(slow(7), pooled(0.49))["cape"]["passes"]
    assert not st.line(slow(10), pooled(0.5))["cape"]["passes"]
    assert not st.line(slow(10), pooled(None))["cape"]["passes"]


def test_the_decline_table_counts_the_short_segments() -> None:
    state = _state({"1963-02": 1.0, "1963-03": 1.0})
    specs = [
        EpisodeSpec(id="d1", start="1963-03", end="1963-08"),
        EpisodeSpec(id="d2", start="1963-10", end="1964-06"),
        EpisodeSpec(id="d3", start="1964-08", end="1964-09"),
        EpisodeSpec(id="d4", start="1965-01", end="1965-10"),
    ]
    found = st.decline_rows(_frame_of(cape=state, widening=state), specs, ["d4"])
    assert [r["months"] for r in found["rows"]] == [6, 9, 2, 10]
    assert [r["counted"] for r in found["rows"]] == [True, True, True, False]
    assert found["rows"][0] == {
        "id": "d1",
        "start": "1963-03",
        "end": "1963-08",
        "months": 6,
        "counted": True,
        "inside_on": 1,
        "start_on": True,
        "switch": -1,
    }
    assert found["counted"] == 3 and found["short"] == 2 and found["median_months"] == 6


# ---------------------------------------------------------------- whole table


@pytest.fixture(scope="module")
def table(frame: pd.DataFrame) -> dict[str, Any]:
    return st.run_table(frame, GATE, LOADED, ROOT_DIR, provenance=PROVENANCE)


def test_the_table_covers_the_gate_segments(table: dict[str, Any], states: pd.DataFrame) -> None:
    assert table["run_id"] == st.RUN_ID and table["owner_decision"] == st.OWNER_DECISION
    assert table["snapshot_id"] == "sig-ed9e988ffe7c"
    assert table["production_effect"] == "none" and table["broker_action"] == "none"
    for market in st.MARKETS:
        window_id = table["markets"][market]["window_id"]
        slow, bull = GATE.markets[market].segment_lists(window_id)
        found = table["markets"][market]
        assert [r["id"] for r in found["slow_bear"]] == [s.id for s in slow]
        assert [r["id"] for r in found["long_bull"]["rows"]] == [s.id for s in bull]
        assert found["slow_bear"] == st.slow_rows(states, slow)
    us = table["markets"]["us_market"]
    assert us["line"] == st.line(us["slow_bear"], us["long_bull"])
    declines = GATE.markets["us_market"].decline_list(us["window_id"])
    assert [r["id"] for r in us["declines"]["rows"]] == [s.id for s in declines]
    assert "line" not in table["markets"]["qqq"] and "declines" not in table["markets"]["qqq"]


def test_the_series_lines(table: dict[str, Any]) -> None:
    sources = table["series"]["sources"]
    assert {
        s: (v["release_lag_months"], v["known_lag_trading_days"]) for s, v in sources.items()
    } == st.LAGS
    assert {s: v["offsets"] for s, v in sources.items()} == {
        st.PRICE: [1],
        st.EARNINGS: [16],
        st.CPI: [16],
        st.BAA: [1],
        st.AAA: [1],
    }
    assert (
        sources[st.BAA]["first_month"] == "1919-01" and sources[st.BAA]["last_month"] == "2010-12"
    )
    found = table["series"]["states"]
    assert found["spread"]["first_value"] == "1919-02"
    assert found["spread"]["first_defined"] == "1929-01"
    assert all(0.0 <= s["share_on"] <= 1.0 for s in found.values())


def test_the_report_prints_marks_and_no_data_rows(table: dict[str, Any]) -> None:
    text = st.render(table)
    for line in (
        "# 第 5 期状态表（只印，不判、不计、不开题）",
        "## 跑前写下的线（第 5 期章程第 3.2 节，只给续期卡用，不判）",
        "## 利差 6 个月变化对组合尺子的下跌 10% 段（教训 2）",
        "| shiller_earnings | 15 | 0 | 1871-01..2010-12 | 0 | M−16 |",
        "- 分位含当月（当月是到当月为止的最大值时为 1.0），第 120 个有值的月份起有定义；"
        "CAPE 的价按评估台对齐是 M−1 的月均价，比章程「当月月均价」晚一个月，方向保守；"
        "价折实用的是 M−16 的 CPI，最近 15 个月的通胀没折进去："
        "通胀高的年份 CAPE 偏高，通缩年份偏低。",
        "production_effect=none；broker_action=none。",
    ):
        assert line in text
    assert "计入的 24 段里月数 ≤ 6 的 13 段，月数中位 6" in text
    tables = [line for line in text.splitlines() if line.startswith("| ")]
    assert not any(line.startswith(("| 18", "| 19", "| 20")) for line in tables)


def test_the_results_are_plain_json(table: dict[str, Any], tmp_path: Path) -> None:
    st.write_json(table, tmp_path / st.RUN_JSON)
    assert json.loads((tmp_path / st.RUN_JSON).read_text(encoding="utf-8"))["run_id"] == st.RUN_ID


# ---------------------------------------------------------------- checks and commands


def test_the_check_finds_wrong_lags_gaps_and_short_series(frame: pd.DataFrame) -> None:
    assert st.check_problems(frame, SPECS, GATE, LOADED) == []
    wrong = {**SPECS, st.CPI: _Spec(1, 0)}
    assert st.check_problems(frame, wrong, GATE, LOADED) == ["shiller_cpi: 滞后 1/0，应是 15/0"]
    missing = {k: v for k, v in SPECS.items() if k != st.AAA}
    assert st.check_problems(frame, missing, GATE, LOADED) == ["aaa_monthly: 快照里没有"]
    gap = frame.copy()
    gap.loc[pd.Period("1950-03", "M"), st.BAA] = np.nan
    assert st.check_problems(gap, SPECS, GATE, LOADED) == ["baa_monthly: 中间缺月"]
    short = frame.copy()
    short.loc[pd.Period("2010-12", "M"), st.PRICE] = np.nan
    assert st.check_problems(short, SPECS, GATE, LOADED) == ["shiller_price: 只到 2010-11"]


def test_the_module_reads_no_research_record_or_validation_window() -> None:
    source = Path(st.__file__).read_text(encoding="utf-8")
    for forbidden in ("window_reads", "runs/", "_screens", "data/raw", "validation_", "2011"):
        assert forbidden not in source


def _synthetic_load(monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame) -> None:
    monkeypatch.setattr(st, "load_inputs", lambda: (frame, SPECS, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(st, "git_provenance", lambda root: PROVENANCE)


def test_the_check_command(frame: pd.DataFrame, monkeypatch: pytest.MonkeyPatch) -> None:
    _synthetic_load(monkeypatch, frame)
    found = CliRunner().invoke(st.app, ["check"])
    assert found.exit_code == 0, found.output
    lines = found.output.splitlines()
    assert "state_table: shiller_earnings 1871-01..2010-12；月底用的是 M−16" in lines
    assert lines[-1] == "state_table: 可以"
    bad = frame.copy()
    bad.loc[pd.Period("1950-03", "M"), st.BAA] = np.nan
    _synthetic_load(monkeypatch, bad)
    found = CliRunner().invoke(st.app, ["check"])
    assert found.exit_code == 1 and "state_table: 不行：baa_monthly: 中间缺月" in found.output


def test_the_run_command_writes_once(
    frame: pd.DataFrame, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _synthetic_load(monkeypatch, frame)
    out = tmp_path / "states"
    found = CliRunner().invoke(st.app, ["run", "--out", str(out)])
    assert found.exit_code == 0, found.output
    written = json.loads((out / st.RUN_JSON).read_text(encoding="utf-8"))
    assert written["provenance"] == PROVENANCE
    assert (out / st.RUN_MD).read_text(encoding="utf-8").startswith("# 第 5 期状态表")
    assert sum("慢熊段起点前为真" in line for line in found.output.splitlines()) == 3
    again = CliRunner().invoke(st.app, ["run", "--out", str(out)])
    assert again.exit_code == 1 and "拒绝覆盖" in again.output
    (out / st.RUN_MD).unlink()
    again = CliRunner().invoke(st.app, ["run", "--out", str(out)])
    assert again.exit_code == 1 and "拒绝覆盖" in again.output
    assert not (out / st.RUN_MD).exists()
