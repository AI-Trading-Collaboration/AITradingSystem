"""TRADING-2571: the QQQ 200-day pullback signal's forward record, on synthetic Yahoo files."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_probes import ma200_forward as mf
from ai_trading_system.trading_calendar import is_us_equity_trading_day

COMMIT = "0123456789abcdef0123456789abcdef01234567"
PROVENANCE = {"commit": COMMIT, "branch": "main", "code_modified": False}
GROWTH = 1.0004
LAST_DAY = date(2028, 3, 31)
RECORD_DIR = PROJECT_ROOT / "research" / "_forward" / "ma200"


def _trading_days(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


DAYS = _trading_days(date(2000, 1, 3), LAST_DAY)


def at(day: date) -> int:
    return DAYS.index(day)


@dataclass
class Series:
    """Adjusted bars: a smooth uptrend whose lows stay well above the 200-day average."""

    close: list[float] = field(default_factory=lambda: [100 * GROWTH**k for k in range(len(DAYS))])
    open: list[float] = field(default_factory=list)
    low: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.open = list(self.close)
        self.low = [value * 0.995 for value in self.close]

    def sma(self, position: int) -> float:
        return math.fsum(self.close[position - 199 : position + 1]) / 200

    def touch(self, day: date, depth: float = 0.99) -> int:
        position = at(day)
        self.low[position] = self.sma(position) * depth
        return position

    def payload(
        self,
        *,
        factor: float = 1.0,
        symbol: str = "QQQ",
        end: date = LAST_DAY,
        hour: int = 14,
        minute: int = 30,
    ) -> dict[str, Any]:
        count = sum(1 for day in DAYS if day <= end)
        stamps = [
            int(datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC).timestamp())
            for day in DAYS[:count]
        ]
        return {
            "chart": {
                "result": [
                    {
                        "meta": {"symbol": symbol, "dataGranularity": "1d"},
                        "timestamp": stamps,
                        "indicators": {
                            "quote": [
                                {
                                    "open": [v / factor for v in self.open[:count]],
                                    "low": [v / factor for v in self.low[:count]],
                                    "high": [v / factor for v in self.close[:count]],
                                    "close": [v / factor for v in self.close[:count]],
                                    "volume": [1] * count,
                                }
                            ],
                            "adjclose": [{"adjclose": list(self.close[:count])}],
                        },
                    }
                ],
                "error": None,
            }
        }

    def bars(self, end: date = LAST_DAY) -> mf.Bars:
        return mf.parse_chart(self.payload(end=end), file="qqq.json", sha256="0" * 64)

    def write(self, path: Path, end: date = LAST_DAY) -> Path:
        path.write_text(json.dumps(self.payload(end=end)), encoding="utf-8")
        return path


def _signal_series() -> tuple[Series, int, int]:
    """One counted signal: touch 2026-11-04, entry two days later, no stop."""

    series = Series()
    position = series.touch(date(2026, 11, 4))
    return series, position, position + 2


@pytest.fixture
def no_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """Synthetic bars cannot reproduce the real backtest; append's guard is tested on its own."""

    monkeypatch.setattr(mf, "reproduce", lambda bars: {"values": {}, "problems": [], "ok": True})


def _rows(directory: Path) -> list[dict[str, Any]]:
    path = directory / mf.EVENTS_FILE
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _content(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keep = {"kind", "decided_on", *mf.TOUCH_FIELDS, *mf.SETTLE_FIELDS}
    return [{k: v for k, v in row.items() if k in keep} for row in rows if row["kind"] != "checked"]


# Constants and texts ----------------------------------------------------------------------------


def test_the_rules_are_the_frozen_ones() -> None:
    assert mf.RUN_ID == "TRADING-2571-01"
    assert mf.SCHEMA == "ma200_forward.v1"
    assert mf.OWNER_DECISION == "owner_decision:TRADING-2571:2026-11-01:ma200_forward_record"
    assert mf.VERDICT_DECISION == "owner_decision:TRADING-2571:2026-11-02:return_only_verdict"
    assert mf.TICKER == "QQQ"
    assert mf.EASTERN == "America/New_York"
    assert (mf.WINDOW_START, mf.WINDOW_END) == (date(2026, 11, 2), date(2027, 10, 29))
    assert (mf.SMA_DAYS, mf.SLOPE_LAG, mf.CONFIRM_DAYS, mf.COOLDOWN, mf.HOLD_DAYS) == (
        200,
        20,
        3,
        20,
        60,
    )
    assert mf.STOP == 0.08
    assert (mf.LINE_UP, mf.LINE_MEAN) == (0.10, 0.03)
    assert (mf.BACKTEST_START, mf.BACKTEST_END) == (date(2000, 1, 3), date(2026, 10, 30))
    assert mf.GAP_CHECK_FROM == date(2025, 10, 1)
    assert (mf.REVISION_TOLERANCE, mf.REPRODUCE_TOLERANCE) == (1e-5, 5e-4)
    assert mf.RESCALE_FLOOR == 0.9
    assert (mf.EVENTS_FILE, mf.SUMMARY_FILE) == ("events.jsonl", "summary.md")
    assert mf.DEFAULT_DIR == RECORD_DIR
    assert mf.KINDS == ("touch", "settle", "checked")


def test_the_backtest_to_reproduce_is_the_one_the_rules_were_set_on() -> None:
    assert mf.BACKTEST == {
        "touches": 84,
        "signals": 21,
        "failed": 63,
        "counted": 13,
        "entries": (
            "2004-10-21",
            "2007-08-20",
            "2010-05-10",
            "2010-06-15",
            "2010-08-02",
            "2010-09-09",
            "2011-10-24",
            "2012-11-01",
            "2014-10-17",
            "2016-07-07",
            "2019-02-20",
            "2020-04-14",
            "2024-08-07",
        ),
        "up60": 0.8461538461538461,
        "mean60": 0.08367354908251776,
        "mean60_stop": 0.06397331931128644,
        "worst60": -0.013577064042907816,
        "base_days": 6489,
        "base_up60": 0.68777931884728,
        "base_mean60": 0.029483887039537075,
        "base_mean60_stop": 0.023918070521552356,
    }


def test_the_row_fields() -> None:
    assert mf.COMMON_KEYS == frozenset(
        {
            "schema_version",
            "seq",
            "kind",
            "decided_on",
            "prev_sha256",
            "code_commit",
            "price_file",
            "price_sha256",
            "price_last_date",
            "through",
        }
    )
    assert mf.TOUCH_FIELDS == (
        "touch_date",
        "prev_date",
        "prev_close",
        "prev_sma",
        "low",
        "sma",
        "sma_lag",
        "filter",
        "closes",
        "outcome",
        "entry_date",
        "entry_price",
        "counted",
    )
    assert mf.SETTLE_FIELDS == (
        "touch_date",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "r60",
        "stopped",
        "stop_date",
        "stop_open",
        "stop_low",
        "stop_fill",
        "r60_stop",
        "counted",
    )
    assert mf.CHECKED_FIELDS == ("touches", "signals", "counted", "settled", "open")


def test_the_texts() -> None:
    assert mf.NOTE_TEXT == "只用于学习检验，不是交易指令。"
    assert mf.HOLDS_TEXT == "合格：信号带止损的 60 日均值比任意一天高 3 个百分点以上"
    assert mf.NOT_HOLDS_TEXT == "不合格：信号带止损的 60 日均值没有比任意一天高 3 个百分点"
    assert mf.NO_SIGNAL_TEXT == "窗口内没有信号，不判"
    assert mf.IN_PROGRESS_TEXT == "进行中：只描述，不判"
    assert mf.PREFIX == "ma200_forward: "


def test_the_module_does_not_reach_the_network_or_other_records() -> None:
    source = Path(mf.__file__).read_text(encoding="utf-8")
    for word in (
        "requests",
        "urllib",
        "http",
        "socket",
        "yfinance",
        "subprocess",
        "ledger.jsonl",
        "window_reads",
        "daily_root",
    ):
        assert word not in source, word


# Reading the Yahoo file ---------------------------------------------------------------------------


def test_parse_scales_open_and_low_by_adjclose_over_close() -> None:
    series = Series()
    series.low[300] = 50.0
    series.open[300] = 60.0
    bars = mf.parse_chart(series.payload(factor=1.25), file="x.json", sha256="ab")
    assert bars.file == "x.json" and bars.sha256 == "ab" and bars.skipped == 0
    assert bars.dates == tuple(DAYS)
    assert bars.close[300] == pytest.approx(series.close[300], rel=1e-12)
    assert bars.low[300] == pytest.approx(50.0, rel=1e-12)
    assert bars.open[300] == pytest.approx(60.0, rel=1e-12)
    assert len(bars) == len(DAYS)


def test_parse_reads_dates_in_new_york() -> None:
    late = mf.parse_chart(Series().payload(hour=3, minute=0), file="x", sha256="")
    assert late.dates[:3] == tuple(day - timedelta(days=1) for day in DAYS[1:4])
    for hour in (13, 14, 20):
        assert mf.parse_chart(Series().payload(hour=hour), file="x", sha256="").dates[1] == DAYS[1]


def test_parse_skips_and_counts_incomplete_rows() -> None:
    payload = Series().payload()
    quote = payload["chart"]["result"][0]["indicators"]["quote"][0]
    quote["low"][500] = None
    quote["open"][501] = float("nan")
    payload["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"][502] = None
    bars = mf.parse_chart(payload, file="x", sha256="")
    assert bars.skipped == 3
    assert len(bars) == len(DAYS) - 3
    assert DAYS[500] not in bars.dates and DAYS[503] in bars.dates


def test_parse_drops_bars_before_the_backtest_start() -> None:
    payload = Series().payload()
    result = payload["chart"]["result"][0]
    early = int(datetime(1999, 12, 30, 14, 30, tzinfo=UTC).timestamp())
    result["timestamp"].insert(0, early)
    for name in ("open", "low", "high", "close", "volume"):
        result["indicators"]["quote"][0][name].insert(0, 90.0)
    result["indicators"]["adjclose"][0]["adjclose"].insert(0, 90.0)
    bars = mf.parse_chart(payload, file="x", sha256="")
    assert bars.dates[0] == date(2000, 1, 3)
    assert bars.skipped == 0


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("symbol", "QQQ"),
        ("granularity", "daily"),
        ("length", "length"),
        ("order", "increasing"),
        ("start", "2000-01-03"),
        ("short", "220"),
        ("shape", "Yahoo"),
    ],
)
def test_parse_refuses_a_file_it_cannot_trust(change: str, message: str) -> None:
    payload = Series().payload()
    result = payload["chart"]["result"][0]
    if change == "symbol":
        result["meta"]["symbol"] = "SPY"
    elif change == "granularity":
        result["meta"]["dataGranularity"] = "1wk"
    elif change == "length":
        result["indicators"]["quote"][0]["low"].pop()
    elif change == "order":
        result["timestamp"][400], result["timestamp"][401] = (
            result["timestamp"][401],
            result["timestamp"][400],
        )
    elif change == "start":
        payload = Series().payload()
        result = payload["chart"]["result"][0]
        result["timestamp"] = result["timestamp"][1:] + [result["timestamp"][-1] + 86400]
    elif change == "short":
        payload = Series().payload(end=DAYS[218])
    else:
        payload = {"chart": {"result": []}}
    with pytest.raises(mf.Ma200Error, match=message):
        mf.parse_chart(payload, file="x", sha256="")


def test_load_chart_hashes_the_file_bytes(tmp_path: Path) -> None:
    path = Series().write(tmp_path / "qqq-2026-11-06.json")
    bars = mf.load_chart(path)
    assert bars.file == "qqq-2026-11-06.json"
    assert bars.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "bad.json").write_text("[1, 2]", encoding="utf-8")
    with pytest.raises(mf.Ma200Error):
        mf.load_chart(tmp_path / "bad.json")
    with pytest.raises(mf.Ma200Error):
        mf.load_chart(tmp_path / "missing.json")


def test_cut_keeps_bars_through_the_day() -> None:
    bars = Series().bars()
    short = mf.cut(bars, date(2026, 11, 7))
    assert short.dates[-1] == date(2026, 11, 6)
    assert len(short.open) == len(short.low) == len(short.close) == len(short)
    assert (short.file, short.sha256) == (bars.file, bars.sha256)


def test_moving_average_is_the_mean_of_the_last_200_closes() -> None:
    series = Series()
    values = mf.moving_average(series.close)
    assert len(values) == len(series.close)
    assert values[198] is None and values[0] is None
    for position in (199, 1000, 6700):
        assert values[position] == pytest.approx(series.sma(position), rel=1e-12)


# The scan ---------------------------------------------------------------------------------------


def test_a_touch_that_holds_three_closes_is_a_signal_bought_on_the_third_close() -> None:
    series, touch, entry = _signal_series()
    found = [t for t in mf.scan(series.bars()) if t.position >= at(date(2026, 1, 2))]
    assert found == [mf.Touch(touch, "signal", entry, True, True)]


def test_low_equal_to_the_average_touches_and_a_close_equal_to_it_holds() -> None:
    series = Series()
    position = at(date(2026, 11, 4))
    sma = series.sma(position)
    series.low[position] = sma
    series.close[position + 1] = sma
    found = [t for t in mf.scan(series.bars()) if t.position >= at(date(2026, 1, 2))]
    assert [(t.position, t.outcome, t.entry) for t in found] == [(position, "signal", position + 2)]


@pytest.mark.parametrize(("last", "touches"), [(1500.0, False), (1500.5, True)])
def test_the_previous_close_must_be_strictly_above_the_previous_average(
    last: float, touches: bool
) -> None:
    """200 flat closes of 1500.0 make the previous average exactly 1500.0."""

    series = Series()
    position = at(date(2026, 11, 4))
    for k in range(position - 200, position):
        series.close[k] = 1500.0
        series.low[k] = 1501.5
    series.close[position - 1] = last
    series.low[position] = series.sma(position) * 0.98
    found = [t.position for t in mf.scan(series.bars())]
    assert (position in found) is touches


def test_a_failed_touch_resumes_the_search_three_days_later() -> None:
    """The touch day's own close breaks; a touch on day 3 is skipped, one on day 4 counts."""

    series = Series()
    first = at(date(2026, 11, 4))
    series.low[first] = series.sma(first) * 0.99
    series.close[first] = series.sma(first) * 0.999
    assert series.close[first] < series.sma(first)
    for offset in (2, 3):
        series.low[first + offset] = series.sma(first + offset) * 0.99
    found = [t for t in mf.scan(series.bars()) if t.position >= at(date(2026, 1, 2))]
    assert [(t.position, t.outcome) for t in found] == [(first, "failed"), (first + 3, "signal")]
    assert found[0].entry is None and found[0].counted is False


def test_touches_within_20_days_of_an_entry_do_not_count() -> None:
    series, touch, entry = _signal_series()
    for offset in (5, 19, 20):
        series.low[entry + offset] = series.sma(entry + offset) * 0.99
    found = [t for t in mf.scan(series.bars()) if t.position >= at(date(2026, 1, 2))]
    assert [(t.position, t.outcome) for t in found] == [
        (touch, "signal"),
        (entry + 20, "signal"),
    ]


def test_the_slope_filter_is_applied_after_the_scan() -> None:
    """A signal whose average is not above the one 20 days earlier is kept but not counted."""

    series = Series()
    position = at(date(2026, 11, 4))
    for k in range(position - 219, position - 199):
        series.close[k] *= 1.6
    series.low[position] = series.sma(position) * 0.99
    assert series.sma(position) < series.sma(position - 20)
    series.low[position + 2 + 10] = series.sma(position + 12) * 0.99
    found = [t for t in mf.scan(series.bars()) if t.position >= at(date(2026, 6, 1))]
    assert found == [mf.Touch(position, "signal", position + 2, False, False)]


def test_an_average_equal_to_the_one_20_days_earlier_does_not_pass_the_filter() -> None:
    """Copying the last 20 closes to 200 days earlier makes the two averages exactly equal."""

    series = Series()
    position = at(date(2026, 11, 4))
    for k in range(20):
        series.close[position - 219 + k] = series.close[position - 19 + k]
    series.low[position] = series.sma(position) * 0.99
    assert series.sma(position) == series.sma(position - 20)
    found = [t for t in mf.scan(series.bars()) if t.position >= position - 5]
    assert found == [mf.Touch(position, "signal", position + 2, False, False)]


def test_a_touch_without_three_closes_yet_is_pending_and_ends_the_scan() -> None:
    series = Series()
    position = series.touch(date(2026, 11, 4))
    bars = series.bars(end=DAYS[position + 1])
    found = [t for t in mf.scan(bars) if t.position >= at(date(2026, 1, 2))]
    assert found == [mf.Touch(position, "pending", None, True, False)]


# Settling a signal --------------------------------------------------------------------------------


def test_settle_without_a_stop_returns_the_60_day_close() -> None:
    series, _, entry = _signal_series()
    outcome = mf.settle(series.bars(), entry)
    assert outcome is not None
    assert outcome.exit == entry + 60
    assert outcome.r60 == pytest.approx(GROWTH**60 - 1, rel=1e-9)
    assert (outcome.stopped, outcome.stop, outcome.fill) == (False, None, None)
    assert outcome.r60_stop == outcome.r60


def test_a_gap_below_the_stop_fills_at_the_open() -> None:
    series, _, entry = _signal_series()
    price = series.close[entry]
    series.open[entry + 10] = price * 0.90
    series.low[entry + 10] = price * 0.89
    outcome = mf.settle(series.bars(), entry)
    assert outcome is not None and outcome.stopped and outcome.stop == entry + 10
    assert outcome.fill == pytest.approx(price * 0.90, rel=1e-12)
    assert outcome.r60_stop == pytest.approx(-0.10, abs=1e-12)
    assert outcome.r60 == pytest.approx(GROWTH**60 - 1, rel=1e-9)


def test_an_intraday_break_fills_at_entry_times_0_92() -> None:
    series, _, entry = _signal_series()
    price = series.close[entry]
    series.low[entry + 30] = price * 0.92
    outcome = mf.settle(series.bars(), entry)
    assert outcome is not None and outcome.stopped and outcome.stop == entry + 30
    assert outcome.fill == pytest.approx(price * 0.92, rel=1e-12)
    assert outcome.r60_stop == pytest.approx(-0.08, abs=1e-12)


def test_the_stop_watches_days_1_to_60_after_entry_only() -> None:
    series, _, entry = _signal_series()
    price = series.close[entry]
    series.low[entry] = price * 0.5
    series.low[entry + 61] = price * 0.5
    outcome = mf.settle(series.bars(), entry)
    assert outcome is not None and not outcome.stopped
    series.low[entry + 60] = price * 0.5
    outcome = mf.settle(series.bars(), entry)
    assert outcome is not None and outcome.stop == entry + 60


def test_settle_is_none_until_the_60th_day_is_in() -> None:
    series, _, entry = _signal_series()
    assert mf.settle(series.bars(end=DAYS[entry + 59]), entry) is None
    assert mf.settle(series.bars(end=DAYS[entry + 60]), entry) is not None


def test_holding_reports_an_open_position() -> None:
    series, _, entry = _signal_series()
    series.low[entry + 3] = series.close[entry] * 0.9
    state = mf.holding(series.bars(end=DAYS[entry + 5]), entry)
    assert state["days"] == 5
    assert state["last_return"] == pytest.approx(GROWTH**5 - 1, rel=1e-9)
    assert state["stop_date"] == DAYS[entry + 3].isoformat()
    assert state["stop_return"] == pytest.approx(-0.08, abs=1e-12)


def test_base_rows_cover_window_days_with_60_days_after_them() -> None:
    series = Series()
    bars = series.bars(end=date(2027, 3, 1))
    rows = mf.base_rows(bars, mf.WINDOW_START, mf.WINDOW_END)
    assert rows[0][0] == date(2026, 11, 2)
    assert rows[-1][0] == DAYS[at(date(2027, 3, 1)) - 60]
    assert all(r60 == pytest.approx(GROWTH**60 - 1, rel=1e-9) for _, r60, _ in rows)
    assert all(r60 == stop for _, r60, stop in rows)


def test_describe() -> None:
    assert mf.describe([], []) == {
        "n": 0,
        "up60": None,
        "mean60": None,
        "mean60_stop": None,
        "worst60": None,
    }
    assert mf.describe([0.1, 0.02, 0.0, 0.04], [0.1, -0.08, 0.0, 0.04]) == pytest.approx(
        {"n": 4, "up60": 0.75, "mean60": 0.04, "mean60_stop": 0.015, "worst60": 0.0}
    )


# Reproducing the backtest -------------------------------------------------------------------------


def _backtest_series() -> Series:
    series = Series()
    for day in (date(2010, 6, 1), date(2015, 3, 2), date(2015, 3, 9)):
        series.touch(day)
    return series


def test_reproduce_counts_the_history_through_the_backtest_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    series = _backtest_series()
    series.touch(date(2026, 11, 4))
    expected = {
        "touches": 2,
        "signals": 2,
        "failed": 0,
        "counted": 2,
        "entries": (DAYS[at(date(2010, 6, 1)) + 2].isoformat(), "2015-03-04"),
        "up60": 1.0,
        "mean60": GROWTH**60 - 1,
        "mean60_stop": GROWTH**60 - 1,
        "worst60": GROWTH**60 - 1,
        "base_days": at(date(2026, 10, 30)) - 60 - 199 + 1,
        "base_up60": 1.0,
        "base_mean60": GROWTH**60 - 1,
        "base_mean60_stop": GROWTH**60 - 1,
    }
    monkeypatch.setattr(mf, "BACKTEST", expected)
    replay = mf.reproduce(series.bars())
    assert replay["ok"] is True and replay["problems"] == []
    assert replay["values"]["entries"] == expected["entries"]
    assert replay["values"]["base_days"] == expected["base_days"]


def test_reproduce_names_what_it_cannot_reproduce(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = dict(mf.reproduce(_backtest_series().bars())["values"])
    expected["counted"] = 3
    expected["mean60"] = expected["mean60"] + 4e-4
    expected["base_up60"] = 0.9
    monkeypatch.setattr(mf, "BACKTEST", expected)
    replay = mf.reproduce(_backtest_series().bars())
    assert replay["ok"] is False
    assert [problem.split()[0] for problem in replay["problems"]] == ["counted", "base_up60"]


# Events -----------------------------------------------------------------------------------------


def test_build_events_records_a_window_signal_and_its_settlement() -> None:
    series, touch, entry = _signal_series()
    series.touch(date(2026, 9, 1))
    bars = series.bars()
    events = mf.build_events(bars, date(2027, 3, 1))
    assert [e["kind"] for e in events] == ["touch", "settle"]
    first, second = events
    assert first == {
        "kind": "touch",
        "decided_on": DAYS[entry].isoformat(),
        "touch_date": "2026-11-04",
        "prev_date": "2026-11-03",
        "prev_close": pytest.approx(series.close[touch - 1], rel=1e-12),
        "prev_sma": pytest.approx(series.sma(touch - 1), rel=1e-12),
        "low": pytest.approx(series.low[touch], rel=1e-12),
        "sma": pytest.approx(series.sma(touch), rel=1e-12),
        "sma_lag": pytest.approx(series.sma(touch - 20), rel=1e-12),
        "filter": True,
        "closes": pytest.approx([series.close[touch + k] for k in range(3)], rel=1e-12),
        "outcome": "signal",
        "entry_date": DAYS[entry].isoformat(),
        "entry_price": pytest.approx(series.close[entry], rel=1e-12),
        "counted": True,
    }
    assert second == {
        "kind": "settle",
        "decided_on": DAYS[entry + 60].isoformat(),
        "touch_date": "2026-11-04",
        "entry_date": DAYS[entry].isoformat(),
        "entry_price": pytest.approx(series.close[entry], rel=1e-12),
        "exit_date": DAYS[entry + 60].isoformat(),
        "exit_price": pytest.approx(series.close[entry + 60], rel=1e-12),
        "r60": pytest.approx(GROWTH**60 - 1, rel=1e-9),
        "stopped": False,
        "stop_date": None,
        "stop_open": None,
        "stop_low": None,
        "stop_fill": None,
        "r60_stop": pytest.approx(GROWTH**60 - 1, rel=1e-9),
        "counted": True,
    }


def test_build_events_orders_by_decision_day_and_skips_pending_touches() -> None:
    series, touch, entry = _signal_series()
    failed = at(date(2027, 1, 5))
    series.low[failed] = series.sma(failed) * 0.99
    series.close[failed + 2] = series.sma(failed) * 0.99
    late = series.touch(DAYS[entry + 59])
    events = mf.build_events(series.bars(), DAYS[entry + 60])
    assert [(e["kind"], e["touch_date"]) for e in events] == [
        ("touch", "2026-11-04"),
        ("touch", DAYS[failed].isoformat()),
        ("settle", "2026-11-04"),
    ]
    assert events[1]["outcome"] == "failed" and events[1]["entry_date"] is None
    assert events[1]["entry_price"] is None and events[1]["counted"] is False
    assert mf.pending(series.bars(), DAYS[entry + 60]) == [DAYS[late].isoformat()]


def test_build_events_ignores_touches_outside_the_window() -> None:
    series = Series()
    series.touch(date(2026, 10, 27))
    series.touch(date(2027, 11, 2))
    assert mf.build_events(series.bars(), LAST_DAY) == []


def test_a_settle_with_a_stop_records_the_fill() -> None:
    series, _, entry = _signal_series()
    price = series.close[entry]
    series.open[entry + 4] = price * 0.95
    series.low[entry + 4] = price * 0.91
    settle = mf.build_events(series.bars(), LAST_DAY)[1]
    assert settle["stopped"] is True
    assert settle["stop_date"] == DAYS[entry + 4].isoformat()
    assert settle["stop_open"] == pytest.approx(price * 0.95, rel=1e-12)
    assert settle["stop_low"] == pytest.approx(price * 0.91, rel=1e-12)
    assert settle["stop_fill"] == pytest.approx(price * 0.92, rel=1e-12)
    assert settle["r60_stop"] == pytest.approx(-0.08, abs=1e-12)


# Appending ----------------------------------------------------------------------------------------


def test_append_writes_new_events_and_a_checked_row(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    bars = series.bars()
    first = mf.append(tmp_path, bars, date(2026, 11, 6), provenance=PROVENANCE)
    assert [row["kind"] for row in first] == ["touch", "checked"]
    second = mf.append(tmp_path, bars, DAYS[entry + 61], provenance=PROVENANCE)
    assert [row["kind"] for row in second] == ["settle", "checked"]
    rows = _rows(tmp_path)
    assert rows == first + second
    assert [row["seq"] for row in rows] == [1, 2, 3, 4]
    assert rows[0]["prev_sha256"] is None
    raw = (tmp_path / mf.EVENTS_FILE).read_bytes().split(b"\n")
    for row, previous in zip(rows[1:], raw, strict=False):
        assert row["prev_sha256"] == hashlib.sha256(previous).hexdigest()
    for row in rows:
        assert row["schema_version"] == mf.SCHEMA
        assert row["code_commit"] == COMMIT
        assert row["price_file"] == "qqq.json" and row["price_sha256"] == "0" * 64
        assert row["price_last_date"] == LAST_DAY.isoformat()
    assert rows[1] | {} == rows[1]
    assert {k: rows[1][k] for k in ("decided_on", "through", *mf.CHECKED_FIELDS)} == {
        "decided_on": "2026-11-06",
        "through": "2026-11-06",
        "touches": 1,
        "signals": 1,
        "counted": 1,
        "settled": 0,
        "open": 1,
    }
    assert {k: rows[3][k] for k in mf.CHECKED_FIELDS} == {
        "touches": 1,
        "signals": 1,
        "counted": 1,
        "settled": 1,
        "open": 0,
    }
    assert mf.verify(tmp_path) == []


def test_the_rows_are_compact_sorted_json(tmp_path: Path, no_replay: None) -> None:
    series, _, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    for line in (tmp_path / mf.EVENTS_FILE).read_bytes().splitlines():
        row = json.loads(line)
        assert line == json.dumps(
            row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")


def test_stepwise_and_single_appends_record_the_same_events(
    tmp_path: Path, no_replay: None
) -> None:
    series, _, entry = _signal_series()
    bars = series.bars()
    weekly = tmp_path / "weekly"
    for day in DAYS[at(date(2026, 11, 2)) : entry + 70 : 5]:
        mf.append(weekly, bars, day, provenance=PROVENANCE)
    once = tmp_path / "once"
    mf.append(once, bars, DAYS[entry + 66], provenance=PROVENANCE)
    assert _content(_rows(weekly)) == _content(_rows(once))
    assert len(_content(_rows(once))) == 2


@pytest.mark.parametrize(
    ("provenance", "message"),
    [
        ({"commit": COMMIT, "code_modified": True}, "code_modified"),
        ({"commit": COMMIT, "code_modified": "unavailable"}, "code_modified"),
        ({"commit": "abc", "code_modified": False}, "40"),
    ],
)
def test_append_refuses_unclean_code(
    tmp_path: Path, no_replay: None, provenance: dict[str, Any], message: str
) -> None:
    with pytest.raises(mf.Ma200Error, match=message):
        mf.append(tmp_path, Series().bars(), date(2026, 11, 6), provenance=provenance)
    assert not (tmp_path / mf.EVENTS_FILE).exists()


def test_append_refuses_bad_through_days(tmp_path: Path, no_replay: None) -> None:
    bars = Series().bars(end=date(2026, 11, 13))
    with pytest.raises(mf.Ma200Error, match="window"):
        mf.append(tmp_path, bars, date(2026, 10, 30), provenance=PROVENANCE)
    with pytest.raises(mf.Ma200Error, match="2026-11-07"):
        mf.append(tmp_path, bars, date(2026, 11, 7), provenance=PROVENANCE)
    with pytest.raises(mf.Ma200Error, match="2026-11-16"):
        mf.append(tmp_path, bars, date(2026, 11, 16), provenance=PROVENANCE)
    mf.append(tmp_path, bars, date(2026, 11, 6), provenance=PROVENANCE)
    for day in (date(2026, 11, 6), date(2026, 11, 5)):
        with pytest.raises(mf.Ma200Error, match="already checked"):
            mf.append(tmp_path, bars, day, provenance=PROVENANCE)
    assert len(_rows(tmp_path)) == 1


def test_append_refuses_a_file_with_a_missing_trading_day(tmp_path: Path, no_replay: None) -> None:
    payload = Series().payload(end=date(2026, 11, 13))
    result = payload["chart"]["result"][0]
    gap = at(date(2026, 3, 10))
    del result["timestamp"][gap]
    for name in ("open", "low", "high", "close", "volume"):
        del result["indicators"]["quote"][0][name][gap]
    del result["indicators"]["adjclose"][0]["adjclose"][gap]
    bars = mf.parse_chart(payload, file="x", sha256="")
    with pytest.raises(mf.Ma200Error, match="2026-03-10"):
        mf.append(tmp_path, bars, date(2026, 11, 6), provenance=PROVENANCE)
    assert mf.trading_gaps(bars, date(2026, 3, 1), date(2026, 3, 31)) == [
        "no bar for trading day 2026-03-10"
    ]
    assert mf.trading_gaps(Series().bars(), mf.GAP_CHECK_FROM, LAST_DAY) == []


def test_append_refuses_when_the_backtest_does_not_reproduce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mf,
        "reproduce",
        lambda bars: {"values": {}, "problems": ["counted 12 is not 13"], "ok": False},
    )
    with pytest.raises(mf.Ma200Error, match="counted 12 is not 13"):
        mf.append(tmp_path, Series().bars(), date(2026, 11, 6), provenance=PROVENANCE)
    assert not (tmp_path / mf.EVENTS_FILE).exists()


def test_append_calls_the_real_backtest_check(tmp_path: Path) -> None:
    with pytest.raises(mf.Ma200Error, match="backtest"):
        mf.append(tmp_path, Series().bars(), date(2026, 11, 6), provenance=PROVENANCE)


def test_append_refuses_a_file_that_changes_what_is_recorded(
    tmp_path: Path, no_replay: None
) -> None:
    series, touch, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    before = (tmp_path / mf.EVENTS_FILE).read_bytes()
    series.close[touch + 1] *= 1.001
    with pytest.raises(mf.Ma200Error, match="line 1 differs in closes"):
        mf.append(tmp_path, series.bars(), date(2026, 11, 13), provenance=PROVENANCE)
    series.close[touch + 1] /= 1.001
    series.close[touch + 1] *= 1 + 1e-8
    mf.append(tmp_path, series.bars(), date(2026, 11, 13), provenance=PROVENANCE)
    assert (tmp_path / mf.EVENTS_FILE).read_bytes().startswith(before)
    series.low[touch] = series.close[touch]
    with pytest.raises(mf.Ma200Error, match="1 recorded events but the price file gives 0"):
        mf.append(tmp_path, series.bars(), date(2026, 11, 20), provenance=PROVENANCE)


def test_append_keeps_existing_bytes(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    path = tmp_path / mf.EVENTS_FILE
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n").rstrip(b"\r\n"))
    mf.append(tmp_path, series.bars(), DAYS[entry + 61], provenance=PROVENANCE)
    assert path.read_bytes().count(b"\r\n") == 1
    assert mf.verify(tmp_path) == []
    assert len(_rows(tmp_path)) == 4


# Dividends: Yahoo rewrites every earlier adjusted price by one factor (TRADING-2571-02) ----------


def _rewritten(series: Series, factor: float, noise: float = 0.0) -> Series:
    """The same bars in a file downloaded after a dividend, with Yahoo-like rounding noise."""

    copy = Series()
    for name in ("close", "open", "low"):
        values = getattr(series, name)
        setattr(copy, name, [v * factor * (1 + noise * math.cos(k)) for k, v in enumerate(values)])
    return copy


def test_the_price_and_return_fields() -> None:
    assert mf.PRICE_FIELDS == {
        "touch": ("prev_close", "prev_sma", "low", "sma", "sma_lag", "closes", "entry_price"),
        "settle": ("entry_price", "exit_price", "stop_open", "stop_low", "stop_fill"),
        "checked": (),
    }
    assert mf.RETURN_FIELDS == ("r60", "r60_stop")
    assert mf.ANCHORS == {"touch": "sma", "settle": "entry_price"}


@pytest.mark.parametrize(
    ("new", "old", "factor"),
    [
        (99.85, 100.0, 0.9985),
        (90.01, 100.0, 0.9001),
        (100.0, 100.0, 1.0),
        (100.0005, 100.0, 1.000005),
        (89.99, 100.0, None),
        (100.002, 100.0, None),
        (50.0, 100.0, None),
        (None, 100.0, None),
        (100.0, 0.0, None),
        (True, 1.0, None),
    ],
)
def test_rescale_factor_accepts_one_dividend_factor_between_0_9_and_1(
    new: Any, old: Any, factor: float | None
) -> None:
    result = mf.rescale_factor(new, old)
    assert result == (None if factor is None else pytest.approx(factor, rel=1e-12))


def test_rescale_takes_the_factor_from_the_rows_anchor() -> None:
    touch = {"kind": "touch", "sma": 500.0, "low": 499.0}
    assert mf.rescale(touch, {"kind": "touch", "sma": 499.5, "low": 1.0}) == pytest.approx(0.999)
    assert mf.rescale(touch, {"kind": "touch", "sma": 400.0}) == 1.0
    assert mf.rescale(touch, {"kind": "settle", "entry_price": 499.5}) == 1.0
    settle = {"kind": "settle", "entry_price": 600.0, "exit_price": 700.0}
    assert mf.rescale(settle, {"kind": "settle", "entry_price": 597.0}) == pytest.approx(0.995)
    assert mf.rescale(settle, {"kind": "touch", "entry_price": 597.0}) == 1.0
    assert mf.rescale({"kind": "checked"}, {"kind": "checked"}) == 1.0


def test_a_dividend_rewrite_of_earlier_prices_is_not_a_revision(
    tmp_path: Path, no_replay: None
) -> None:
    series, _, entry = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    before = (tmp_path / mf.EVENTS_FILE).read_bytes()
    once = _rewritten(series, 0.9985, noise=2e-6)
    assert mf.check(tmp_path, once.bars()) == []
    mf.append(tmp_path, once.bars(), date(2026, 11, 13), provenance=PROVENANCE)
    twice = _rewritten(once, 0.9984)
    mf.append(tmp_path, twice.bars(), DAYS[entry + 61], provenance=PROVENANCE)
    assert (tmp_path / mf.EVENTS_FILE).read_bytes().startswith(before)
    assert mf.verify(tmp_path) == []
    touch, settle = _content(_rows(tmp_path))
    assert settle["entry_price"] == pytest.approx(touch["entry_price"] * 0.9985 * 0.9984, rel=1e-5)
    assert settle["r60"] == pytest.approx(GROWTH**60 - 1, abs=1e-5)
    thrice = _rewritten(twice, 0.9986, noise=2e-6)
    assert mf.check(tmp_path, thrice.bars()) == []
    mf.append(tmp_path, thrice.bars(), DAYS[entry + 66], provenance=PROVENANCE)
    assert mf.check(tmp_path, thrice.bars()) == []


@pytest.mark.parametrize("factor", [0.8999, 0.5, 1.0001, 1.01])
def test_a_rewrite_outside_the_dividend_band_is_a_revision(
    tmp_path: Path, no_replay: None, factor: float
) -> None:
    series, _, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    moved = _rewritten(series, factor).bars()
    message = "line 1 differs in prev_close, prev_sma, low, sma, sma_lag, closes, entry_price"
    assert mf.check(tmp_path, moved) == [message]
    with pytest.raises(mf.Ma200Error, match=message):
        mf.append(tmp_path, moved, date(2026, 11, 13), provenance=PROVENANCE)


@pytest.mark.parametrize("factor", [0.9001, 0.9985, 1.0])
def test_a_rewrite_inside_the_dividend_band_is_accepted(
    tmp_path: Path, no_replay: None, factor: float
) -> None:
    series, _, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    moved = _rewritten(series, factor).bars()
    mf.append(tmp_path, moved, date(2026, 11, 13), provenance=PROVENANCE)
    assert mf.check(tmp_path, moved) == []


def test_a_rewrite_that_also_moves_one_price_is_a_revision(tmp_path: Path, no_replay: None) -> None:
    series, touch, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    moved = _rewritten(series, 0.9985)
    moved.close[touch + 1] *= 1 + 5e-5
    with pytest.raises(mf.Ma200Error, match=r"line 1 differs in closes$"):
        mf.append(tmp_path, moved.bars(), date(2026, 11, 13), provenance=PROVENANCE)


def test_rounding_noise_beyond_the_tolerance_is_a_revision(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    mf.append(tmp_path, series.bars(), DAYS[entry + 61], provenance=PROVENANCE)
    assert mf.check(tmp_path, _rewritten(series, 0.9985, noise=2e-6).bars()) == []
    problems = mf.check(tmp_path, _rewritten(series, 0.9985, noise=3e-5).bars())
    assert problems[0].startswith("line 1 differs in"), problems
    assert any(p.startswith("line 2 differs in") for p in problems), problems


def test_a_settled_return_that_moved_is_a_revision(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    mf.append(tmp_path, series.bars(), DAYS[entry + 61], provenance=PROVENANCE)
    moved = _rewritten(series, 0.9985)
    moved.close[entry + 60] *= 1 + 2e-5
    assert mf.check(tmp_path, moved.bars()) == ["line 2 differs in exit_price, r60, r60_stop"]


def test_a_settlement_may_carry_a_dividend_rewritten_entry_price(
    tmp_path: Path, no_replay: None
) -> None:
    path = _recorded(tmp_path, no_replay)
    price = _rows(tmp_path)[0]["entry_price"]
    for factor, message in (
        (1.01, "entry_price does not match its touch"),
        (0.85, "entry_price does not match its touch"),
        (0.95, "r60 is not"),
    ):
        _rewrite(path, 2, {"entry_price": price * factor})
        problems = mf.verify(tmp_path)
        assert problems and message in problems[0], (factor, problems)


# Verifying the record -----------------------------------------------------------------------------


def _recorded(tmp_path: Path, no_replay: None) -> Path:
    series, _, entry = _signal_series()
    price = series.close[entry]
    series.open[entry + 4] = price * 0.95
    series.low[entry + 4] = price * 0.91
    bars = series.bars()
    mf.append(tmp_path, bars, date(2026, 11, 6), provenance=PROVENANCE)
    mf.append(tmp_path, bars, DAYS[entry + 61], provenance=PROVENANCE)
    return tmp_path / mf.EVENTS_FILE


def _rewrite(path: Path, position: int, change: dict[str, Any], *, rechain: bool = True) -> None:
    rows = [json.loads(line) for line in path.read_bytes().splitlines()]
    rows[position].update(change)
    previous = None
    lines = []
    for row in rows:
        if rechain:
            row["prev_sha256"] = previous
        data = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        lines.append(data)
        previous = hashlib.sha256(data.encode("utf-8")).hexdigest()
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_verify_accepts_an_empty_or_missing_record(tmp_path: Path) -> None:
    assert mf.verify(tmp_path) == []
    assert mf.read_events(tmp_path) == []


@pytest.mark.parametrize(
    ("position", "change", "rechain", "message"),
    [
        (1, {"price_file": "other.json"}, False, "line 3: prev_sha256"),
        (0, {"seq": 5}, True, "line 1: seq is not 1"),
        (0, {"schema_version": "v0"}, True, "line 1: schema_version"),
        (0, {"extra": 1}, True, "line 1: keys"),
        (0, {"kind": "note"}, True, "line 1: kind"),
        (0, {"filter": False}, True, "filter does not match"),
        (0, {"counted": False}, True, "counted is not signal and filter"),
        (0, {"outcome": "failed"}, True, "outcome does not match"),
        (0, {"touch_date": "2026-10-30"}, True, "outside the window"),
        (0, {"low": 10_000.0}, True, "low is not at or below sma"),
        (1, {"touches": 2}, True, "touches is not 1"),
        (1, {"through": "2026-11-09"}, True, "decided_on is not through"),
        (2, {"r60": 0.5}, True, "r60 is not"),
        (2, {"stop_fill": 1.0}, True, "stop_fill"),
        (2, {"entry_price": 1.0}, True, "entry_price does not match its touch"),
        (2, {"entry_date": "2026-11-09"}, True, "entry_date does not match its touch"),
        (2, {"touch_date": "2026-11-05"}, True, "no earlier signal touch"),
        (2, {"decided_on": "2026-11-05"}, True, "goes backwards"),
    ],
)
def test_verify_finds_broken_rows(
    tmp_path: Path,
    no_replay: None,
    position: int,
    change: dict[str, Any],
    rechain: bool,
    message: str,
) -> None:
    path = _recorded(tmp_path, no_replay)
    _rewrite(path, position, change, rechain=rechain)
    problems = mf.verify(tmp_path)
    assert problems and message in problems[0], problems
    with pytest.raises(mf.Ma200Error, match="broken"):
        mf.read_events(tmp_path)


def test_verify_needs_a_checked_row_last(tmp_path: Path, no_replay: None) -> None:
    path = _recorded(tmp_path, no_replay)
    lines = path.read_bytes().splitlines()
    path.write_bytes(b"\n".join(lines[:-1]) + b"\n")
    assert "last row is not a checked row" in mf.verify(tmp_path)[0]


def test_check_recomputes_the_record_from_a_price_file(tmp_path: Path, no_replay: None) -> None:
    series, touch, _ = _signal_series()
    mf.append(tmp_path, series.bars(), date(2026, 11, 6), provenance=PROVENANCE)
    assert mf.check(tmp_path) == []
    assert mf.check(tmp_path, series.bars()) == []
    series.close[touch + 2] *= 1.01
    assert mf.check(tmp_path, series.bars()) == ["line 1 differs in closes, entry_price"]


def test_check_reports_a_backtest_that_does_not_reproduce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        mf,
        "reproduce",
        lambda bars: {"values": {}, "problems": ["touches 80 is not 84"], "ok": False},
    )
    assert mf.check(tmp_path, Series().bars()) == ["backtest: touches 80 is not 84"]


# The criteria and the summary ---------------------------------------------------------------------


def _stats(n: int, up60: float, mean60: float, mean60_stop: float) -> dict[str, Any]:
    return {"n": n, "up60": up60, "mean60": mean60, "mean60_stop": mean60_stop, "worst60": 0.0}


def test_judge_uses_only_the_return_line() -> None:
    base = _stats(250, 0.70, 0.03, 0.02)
    holds = mf.judge(_stats(2, 0.80, 0.0, 0.06), base, True)
    assert holds["state"] == "holds" and holds["text"] == mf.HOLDS_TEXT
    assert holds["diff_up"] == pytest.approx(0.10) and holds["diff_mean"] == pytest.approx(0.03)
    assert holds["line_up"] is True and holds["line_mean"] is True
    # VERDICT_DECISION (2026-11-02): the up-ratio line is still computed, for reference only.
    low_up = mf.judge(_stats(2, 0.50, 0.0, 0.06), base, True)
    assert low_up["state"] == "holds" and low_up["text"] == mf.HOLDS_TEXT
    assert low_up["line_up"] is False and low_up["diff_up"] == pytest.approx(-0.20)
    short = mf.judge(_stats(2, 0.80, 0.09, 0.0599), base, True)
    assert short["state"] == "not_holds" and short["line_mean"] is False
    assert short["line_up"] is True and short["text"] == mf.NOT_HOLDS_TEXT
    both = mf.judge(_stats(2, 0.50, 0.09, 0.0), base, True)
    assert both["state"] == "not_holds" and both["diff_mean"] == pytest.approx(-0.03)
    assert mf.judge(_stats(2, 0.79, 0.0, 0.9), base, True)["line_up"] is False
    waiting = mf.judge(_stats(2, 0.80, 0.0, 0.06), base, False)
    assert waiting["state"] == "in_progress" and waiting["text"] == mf.IN_PROGRESS_TEXT
    assert waiting["line_up"] is True
    empty = mf.judge(_stats(0, 0.0, 0.0, 0.0) | {"up60": None}, base, True)
    assert empty["state"] == "no_signal" and empty["diff_up"] is None
    none = mf.judge(_stats(2, 0.80, 0.0, 0.06), None, False)
    assert none["base"] is None and none["diff_mean"] is None


def test_criteria_is_complete_only_after_every_window_day_has_60_days(
    tmp_path: Path, no_replay: None
) -> None:
    series, _, _ = _signal_series()
    bars = series.bars()
    end = DAYS[at(mf.WINDOW_END) + 60]
    mf.append(tmp_path, bars, DAYS[at(end) - 1], provenance=PROVENANCE)
    rows = mf.read_events(tmp_path)
    assert mf.criteria(rows, bars)["complete"] is False
    assert mf.criteria(rows, None)["base"] is None
    mf.append(tmp_path, bars, end, provenance=PROVENANCE)
    result = mf.criteria(mf.read_events(tmp_path), bars)
    assert result["complete"] is True
    assert result["base"]["n"] == sum(1 for day in DAYS if mf.WINDOW_START <= day <= mf.WINDOW_END)
    assert result["signals"]["n"] == 1
    assert result["state"] == "not_holds"


def test_criteria_waits_for_a_signal_from_the_last_window_days(
    tmp_path: Path, no_replay: None
) -> None:
    series = Series()
    touch = series.touch(date(2027, 10, 28))
    bars = series.bars()
    end = DAYS[at(mf.WINDOW_END) + 60]
    mf.append(tmp_path, bars, end, provenance=PROVENANCE)
    result = mf.criteria(mf.read_events(tmp_path), bars)
    assert result["base"]["n"] == sum(1 for day in DAYS if mf.WINDOW_START <= day <= mf.WINDOW_END)
    assert result["complete"] is False and result["state"] == "in_progress"
    mf.append(tmp_path, bars, DAYS[touch + 2 + 60], provenance=PROVENANCE)
    assert mf.criteria(mf.read_events(tmp_path), bars)["complete"] is True


def test_the_summary(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    failed = at(date(2027, 1, 5))
    series.low[failed] = series.sma(failed) * 0.99
    series.close[failed + 2] = series.sma(failed) * 0.99
    bars = series.bars()
    mf.append(tmp_path, bars, DAYS[entry + 10], provenance=PROVENANCE)
    text = mf.render_summary(mf.read_events(tmp_path), bars)
    for part in (
        "# QQQ 200 日线回踩信号的前向记录",
        mf.RUN_ID,
        mf.OWNER_DECISION,
        mf.NOTE_TEXT,
        "## 信号",
        "## 没站稳的触线",
        "## 合格标准（Baker 定，2026-11-02 起只看收益）",
        f"依据 `{mf.VERDICT_DECISION}`",
        "## 回测对照（2000-01 至 2026-10，固定）",
        mf.IN_PROGRESS_TEXT,
        "2026-11-04",
        f"持有第 10 天，至今 {(GROWTH**10 - 1) * 100:+.2f}%",
        "60 日上涨 84.6%",
        f"检查到：{DAYS[entry + 10].isoformat()}",
    ):
        assert part in text, part
    assert "没有。" in text
    mean_title = "- 信号带止损均值减任意一天不带止损均值（判定线 +3.00）："
    up_title = "- 上涨比例差（只参考，不进判定；原线 +10.00）："
    assert mean_title in text and up_title in text
    assert text.index(mean_title) < text.index(up_title)
    assert "没给价格文件" in mf.render_summary(mf.read_events(tmp_path), None)
    mf.append(tmp_path, bars, DAYS[entry + 61], provenance=PROVENANCE)
    path = mf.write_summary(tmp_path, bars)
    later = path.read_text(encoding="utf-8")
    assert path == tmp_path / mf.SUMMARY_FILE
    assert DAYS[failed].isoformat() in later.split("## 没站稳的触线")[1]
    assert "已结算" in later and f"{(GROWTH**60 - 1) * 100:+.2f}%" in later
    assert len(later.splitlines()) < 80


def test_event_texts(tmp_path: Path, no_replay: None) -> None:
    series, _, entry = _signal_series()
    rows = mf.append(tmp_path, series.bars(), DAYS[entry + 61], provenance=PROVENANCE)
    texts = [mf.event_text(row) for row in rows]
    assert texts[0] == (
        f"2026-11-04 触线，站稳，{DAYS[entry]} 收盘 {series.close[entry]:.2f} 买入（计入）"
    )
    assert texts[1].startswith(f"{DAYS[entry]} 买入的信号 {DAYS[entry + 60]} 满 60 日：")
    assert texts[2] == (
        f"检查到 {DAYS[entry + 61]}：窗口内触线 1 次，信号 1 次（计入 1），已结算 1，持有中 0"
    )


# Commands ----------------------------------------------------------------------------------


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch, no_replay: None) -> Iterator[CliRunner]:
    monkeypatch.setattr(mf, "git_provenance", lambda root: PROVENANCE)
    yield CliRunner()


def test_the_commands(tmp_path: Path, cli: CliRunner) -> None:
    series, touch, entry = _signal_series()
    prices = series.write(tmp_path / "qqq.json")
    record = tmp_path / "record"
    result = cli.invoke(
        mf.app,
        [
            "append",
            "--prices",
            str(prices),
            "--through",
            DAYS[touch + 1].isoformat(),
            "--dir",
            str(record),
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[-1] == (
        "ma200_forward: 2026-11-04 触线，站没站稳要等之后两天的收盘"
    )
    result = cli.invoke(
        mf.app,
        [
            "append",
            "--prices",
            str(prices),
            "--through",
            DAYS[entry].isoformat(),
            "--dir",
            str(record),
        ],
    )
    assert result.exit_code == 0, result.output
    assert all(line.startswith(mf.PREFIX) for line in result.output.splitlines())
    assert "买入（计入）" in result.output
    result = cli.invoke(
        mf.app,
        [
            "append",
            "--prices",
            str(prices),
            "--through",
            DAYS[entry].isoformat(),
            "--dir",
            str(record),
        ],
    )
    assert result.exit_code == 1 and "拒绝追加" in result.output
    result = cli.invoke(mf.app, ["summary", "--prices", str(prices), "--dir", str(record)])
    assert result.exit_code == 0 and (record / mf.SUMMARY_FILE).exists()
    result = cli.invoke(mf.app, ["check", "--prices", str(prices), "--dir", str(record)])
    assert result.exit_code == 0
    assert "记录完好，3 行" in result.output
    result = cli.invoke(
        mf.app, ["append", "--prices", str(prices), "--through", "2026-11-5", "--dir", str(record)]
    )
    assert result.exit_code != 0


def test_the_reproduce_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    prices = _backtest_series().write(tmp_path / "qqq.json")
    result = CliRunner().invoke(mf.app, ["reproduce", "--prices", str(prices)])
    assert result.exit_code == 1
    assert result.output.startswith("ma200_forward: 回测复现：触线 2，信号 2，计入 2")
    expected = dict(mf.reproduce(_backtest_series().bars())["values"])
    monkeypatch.setattr(mf, "BACKTEST", expected)
    result = CliRunner().invoke(mf.app, ["reproduce", "--prices", str(prices)])
    assert result.exit_code == 0 and len(result.output.splitlines()) == 1


def test_the_check_command_fails_on_a_broken_record(tmp_path: Path, cli: CliRunner) -> None:
    (tmp_path / mf.EVENTS_FILE).write_text("{}\n", encoding="utf-8")
    result = cli.invoke(mf.app, ["check", "--dir", str(tmp_path)])
    assert result.exit_code == 1 and result.output.startswith(mf.PREFIX)


# The committed record and its README (run after every Friday append, before the commit) --


def test_committed_record_passes_check() -> None:
    assert mf.verify(RECORD_DIR) == []


def test_committed_summary_matches_the_record() -> None:
    rows = mf.read_events(RECORD_DIR)
    if not rows:
        assert not (RECORD_DIR / mf.SUMMARY_FILE).exists()
        return
    text = (RECORD_DIR / mf.SUMMARY_FILE).read_text(encoding="utf-8")
    assert f"检查到：{rows[-1]['through']}" in text
    assert f"价格文件：{rows[-1]['price_file']}" in text


def test_committed_readme_quotes_the_frozen_rules() -> None:
    text = (RECORD_DIR / "README.md").read_text(encoding="utf-8")
    for line in (
        "数据：QQQ 日线，Yahoo 复权价。",
        "触线：前一天收盘高于前一天的 200 日均线，当天盘中最低价 ≤ 当天的 200 日均线。",
        "过滤：当天的 200 日均线高于 20 个交易日前。",
        "信号：触线当天和之后 2 天收盘都 ≥ 当天的 200 日均线，第 3 天收盘价买入。",
        "止损 −8%（按当天开盘价和买入价×0.92 中较低的成交），持有 60 个交易日。",
        "对比：同期任意一天买入 QQQ 持有 60 天。",
        "合格标准（Baker 事先定）：60 天上涨比例比任意一天高 10 个百分点以上，"
        "且带止损的 60 天平均收益高 3 个百分点以上。",
        mf.OWNER_DECISION,
        mf.VERDICT_DECISION,
        "2026-11-02 起正式判定只看第二条",
        "topic-ma200-c20261031-2220",
    ):
        assert line in text, line
