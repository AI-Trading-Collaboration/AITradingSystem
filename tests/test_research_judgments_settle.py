"""TRADING-2569-03 acceptance tests: settlement of judgments (manual outcomes and price outcomes).

Written by Claude and copied into the repository unchanged. All data here is synthetic; no real
ledger content appears in this file.
"""

from __future__ import annotations

import copy
import json
from dataclasses import fields
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_judgments import importer, prices, settle
from ai_trading_system.research_judgments.__main__ import app
from ai_trading_system.research_judgments.settle import (
    Outcome,
    SettleParams,
    load_settle_params,
    settle_ledger,
    settle_price,
    settle_rows,
)
from ai_trading_system.research_judgments.settlements import settle as run_settle

KNOWN = {"TSM", "NVDA", "SMH"}
METHODS = {"guidance_base_rate", "capex_cycle", "other"}
SECRET = "SECRET-STATEMENT-4711"
PARAMS = SettleParams(
    market_timezone="America/New_York",
    market_close="16:00",
    max_calendar_gap_days=7,
    max_abs_daily_move=0.5,
)
DAYS = pd.bdate_range("2026-01-05", periods=40)  # Monday 2026-01-05 is index 0
SUMMER = pd.bdate_range("2026-07-01", periods=20)  # Wednesday 2026-07-01 is index 0
ASOF = date(2026, 3, 1)


def frame(
    t: dict[int, float] | None = None,
    b: dict[int, float] | None = None,
    days: pd.DatetimeIndex = DAYS,
    ticker: str = "TSM",
    bench: str = "SMH",
) -> pd.DataFrame:
    """Flat 100 series with the given index -> value overrides."""
    tv = [100.0] * len(days)
    bv = [100.0] * len(days)
    for i, v in (t or {}).items():
        tv[i] = v
    for i, v in (b or {}).items():
        bv[i] = v
    return pd.DataFrame({ticker: tv, bench: bv}, index=days)


def rising(days: pd.DatetimeIndex = DAYS) -> pd.DataFrame:
    """Ticker +1 a day, benchmark flat: any complete window is an outperform hit."""
    return pd.DataFrame({"TSM": [100.0 + i for i in range(len(days))], "SMH": 100.0}, index=days)


def day(i: int, days: pd.DatetimeIndex = DAYS) -> str:
    return days[i].date().isoformat()


def L(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "a1",
        "ticker": "TSM",
        "statement": "gross margin above 66%",
        "prob": 0.65,
        "deadline": "2026-01-19",
        "kind": "基本面",
        "createdAt": "2026-01-02T12:00:00Z",
        "purpose": "research",
        "aiMode": "human",
        "methods": ["guidance_base_rate"],
        "status": "open",
        "addenda": [],
    }
    base.update(over)
    return base


def P(**over: Any) -> dict[str, Any]:
    market = {
        "benchmark": "SMH",
        "horizon": 5,
        "anchor": "event_prev_close",
        "direction": "outperform",
    }
    market.update(over.pop("market", {}))
    fields_ = {
        "kind": "价格",
        "deadline": day(10),
        "statement": "TSM beats SMH after the event",
        "market": market,
        **over,
    }
    return L(**fields_)


# ---------- config ----------


def test_default_config_loads_and_is_a_reviewed_zone_c_file() -> None:
    params = load_settle_params()
    assert params == PARAMS
    path = PROJECT_ROOT / "config" / "research" / "research_judgment_eval.yaml"
    assert path.is_file()
    raw = yaml.safe_load(path.read_text("utf-8"))
    for key in (
        "policy_id", "version", "status", "owner", "rationale", "intended_effect",
        "validation_evidence", "review_condition",
    ):  # fmt: skip
        assert str(raw.get(key, "")).strip(), key
    assert raw["production_effect"] == "none"


@pytest.mark.parametrize(
    "patch",
    [
        {"settlement": {"market_timezone": "Mars/Base"}},
        {"settlement": {"market_close": "4pm"}},
        {"settlement": {"market_close": "25:00"}},
        {"settlement": {"market_close": "16"}},
        {"settlement": {"market_close": "1600"}},
        {"settlement": {"market_close": "16:00:30"}},
        {"settlement": {"max_calendar_gap_days": 0}},
        {"settlement": {"max_abs_daily_move": 0}},
        {"settlement": {"surprise": 1}},
        {"schema_version": 2},
    ],
)
def test_bad_config_is_refused(tmp_path: Path, patch: dict[str, Any]) -> None:
    raw = yaml.safe_load(
        (PROJECT_ROOT / "config" / "research" / "research_judgment_eval.yaml").read_text("utf-8")
    )
    for key, value in patch.items():
        if isinstance(value, dict):
            raw[key].update(value)
        else:
            raw[key] = value
    path = tmp_path / "eval.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError):
        load_settle_params(path)


def test_outcome_fields_are_exactly_the_documented_ones() -> None:
    assert [f.name for f in fields(Outcome)] == [
        "id", "kind", "ticker", "source", "state", "outcome", "reason", "prob_original",
        "prob_scored", "ledger_status", "conflict", "overdue", "anchor_date", "end_date",
        "ticker_return", "bench_return", "rel_return",
    ]  # fmt: skip


# ---------- manual (ledger) outcomes ----------


@pytest.mark.parametrize(
    ("status", "state", "outcome"),
    [
        ("hit", "settled", 1),
        ("miss", "settled", 0),
        ("void", "void", None),
        ("open", "pending", None),
    ],
)
def test_ledger_status_maps_to_state_and_outcome(
    status: str, state: str, outcome: int | None
) -> None:
    o = settle_ledger(L(status=status), ASOF)
    assert (o.state, o.outcome, o.source) == (state, outcome, "ledger")
    assert o.ledger_status == status and o.conflict is False and o.reason == ""


def test_unknown_ledger_status_is_a_visible_data_gap() -> None:
    o = settle_ledger(L(status="done"), ASOF)
    assert (o.state, o.outcome, o.reason) == ("data_gap", None, "unknown_status")


@pytest.mark.parametrize(
    ("asof", "overdue"),
    [(date(2026, 1, 18), False), (date(2026, 1, 19), False), (date(2026, 1, 20), True)],
)
def test_open_judgment_is_overdue_only_after_the_deadline_day(asof: date, overdue: bool) -> None:
    assert settle_ledger(L(deadline="2026-01-19"), asof).overdue is overdue


def test_settled_judgment_is_never_overdue() -> None:
    assert settle_ledger(L(status="hit"), date(2027, 1, 1)).overdue is False


@pytest.mark.parametrize("kind", ["基本面", "波动", "其他"])
def test_non_price_kinds_use_the_ledger(kind: str) -> None:
    o = settle_rows([L(kind=kind, status="miss")], frame(), PARAMS, ASOF)[0]
    assert (o.source, o.state, o.outcome) == ("ledger", "settled", 0)


def test_probabilities_are_original_and_scored() -> None:
    row = L(
        prob=0.65,
        probRevisions=[
            {"at": "2026-01-03T00:00:00Z", "prob": 0.55},
            {"at": "2026-01-04T00:00:00Z", "prob": 0.35},
        ],
    )
    o = settle_ledger(row, ASOF)
    assert (o.prob_original, o.prob_scored) == (0.65, 0.35)
    assert settle_ledger(L(prob=0.4), ASOF).prob_scored == 0.4


# ---------- price outcomes: event_prev_close ----------


def test_outperform_hit_with_hand_computed_returns() -> None:
    o = settle_price(P(), frame({9: 100, 14: 110}, {9: 100, 14: 104}), PARAMS)
    assert (o.state, o.outcome, o.source, o.reason) == ("settled", 1, "price", "")
    assert (o.anchor_date, o.end_date) == (day(9), day(14))
    assert (o.ticker_return, o.bench_return, o.rel_return) == (0.1, 0.04, 0.06)


def test_underperform_direction_flips_the_outcome() -> None:
    row = P(market={"direction": "underperform"})
    assert settle_price(row, frame({14: 110}, {14: 104}), PARAMS).outcome == 0
    assert settle_price(row, frame({14: 102}, {14: 105}), PARAMS).outcome == 1


def test_outperform_miss_when_ticker_lags() -> None:
    o = settle_price(P(), frame({14: 102}, {14: 105}), PARAMS)
    assert (o.state, o.outcome) == ("settled", 0)
    assert o.rel_return == pytest.approx(-0.03)


def test_adj_close_levels_do_not_matter_only_the_ratio() -> None:
    closes = frame({14: 110}, {14: 104})
    closes["TSM"] *= 0.5
    closes["SMH"] *= 4
    o = settle_price(P(), closes, PARAMS)
    assert (o.ticker_return, o.bench_return) == (0.1, 0.04)


def test_deadline_on_a_weekend_uses_the_next_trading_day_as_the_event() -> None:
    # 2026-01-17 is a Saturday: the event day is Monday 01-19 (index 10), the anchor Friday 01-16.
    row = P(deadline="2026-01-17")
    o = settle_price(row, frame({9: 100, 14: 110}, {9: 100, 14: 104}), PARAMS)
    assert (o.anchor_date, o.end_date, o.outcome) == (day(9), day(14), 1)


def test_deadline_on_a_trading_day_anchors_on_the_day_before() -> None:
    o = settle_price(P(deadline=day(11)), frame({10: 100, 15: 120}, {10: 100, 15: 100}), PARAMS)
    assert (o.anchor_date, o.end_date, o.rel_return) == (day(10), day(15), 0.2)


def test_event_on_the_first_price_date_has_no_prior_close() -> None:
    o = settle_price(P(deadline=day(0)), frame(), PARAMS)
    assert (o.state, o.reason, o.outcome) == ("data_gap", "no_prior_close", None)


def test_event_before_the_first_price_date_has_no_prior_close() -> None:
    o = settle_price(P(deadline="2025-12-01"), frame(), PARAMS)
    assert (o.state, o.reason) == ("data_gap", "no_prior_close")


def test_window_ending_on_the_last_price_date_is_settled() -> None:
    days = DAYS[:15]  # indexes 0..14; anchor 9 + horizon 5 = 14 is the last
    o = settle_price(P(), frame({14: 110}, {14: 104}, days=days), PARAMS)
    assert (o.state, o.end_date) == ("settled", day(14))


def test_window_one_day_short_is_pending_and_nothing_is_estimated() -> None:
    o = settle_price(P(), frame({13: 110}, {13: 104}, days=DAYS[:14]), PARAMS)
    assert (o.state, o.reason, o.outcome, o.rel_return) == (
        "pending",
        "window_not_complete",
        None,
        None,
    )
    assert (o.anchor_date, o.end_date) == (None, None)


def test_event_after_the_last_price_date_is_pending() -> None:
    o = settle_price(P(deadline="2026-06-01"), frame(), PARAMS)
    assert (o.state, o.reason) == ("pending", "no_event_day_yet")


def test_horizon_counts_trading_days() -> None:
    for h in (1, 3, 8):
        row = P(market={"horizon": h})
        o = settle_price(row, frame({9 + h: 110}, {9 + h: 100}), PARAMS)
        assert (o.end_date, o.rel_return) == (day(9 + h), 0.1)


# ---------- price outcomes: entry_close ----------


def entry(created: str, days: pd.DatetimeIndex = DAYS, **market: Any) -> Outcome:
    row = P(createdAt=created, deadline="2030-01-01", market={"anchor": "entry_close", **market})
    return settle_price(row, rising(days), PARAMS)


@pytest.mark.parametrize(
    ("created", "anchor_index"),
    [
        ("2026-01-06T15:00:00Z", 1),  # 10:00 EST Tuesday: that day's close is the first one after
        ("2026-01-06T20:59:59Z", 1),  # one second before 16:00 EST
        ("2026-01-06T21:00:00Z", 2),  # exactly at the close: the next close
        ("2026-01-06T22:00:00Z", 2),  # after the close
        ("2026-01-10T15:00:00Z", 5),  # Saturday: Monday's close
        ("2026-01-11T23:30:00Z", 5),  # Sunday evening: Monday's close
        ("2026-01-02T12:00:00Z", 0),  # before the price file starts: the first close
    ],
)
def test_entry_close_winter_time(created: str, anchor_index: int) -> None:
    o = entry(created)
    assert o.state == "settled" and o.anchor_date == day(anchor_index)
    assert o.end_date == day(anchor_index + 5)


@pytest.mark.parametrize(
    ("created", "anchor_index"),
    [
        ("2026-07-07T19:59:59Z", 4),  # 15:59:59 EDT
        ("2026-07-07T20:00:00Z", 5),  # 16:00 EDT exactly
        ("2026-07-07T20:00:01Z", 5),
    ],
)
def test_entry_close_uses_daylight_time_in_summer(created: str, anchor_index: int) -> None:
    o = entry(created, days=SUMMER)
    assert o.anchor_date == day(anchor_index, SUMMER)


def test_entry_close_after_the_last_close_is_pending() -> None:
    o = entry("2026-02-27T22:00:00Z")  # after the last price date's close
    assert (o.state, o.reason) == ("pending", "no_entry_close_yet")


def test_entry_close_window_must_be_complete() -> None:
    o = entry("2026-02-20T15:00:00Z", market={"horizon": 5}) if False else None
    row = P(
        createdAt="2026-02-24T15:00:00Z",
        deadline="2026-03-30",
        market={"anchor": "entry_close", "horizon": 5},
    )
    o = settle_price(row, frame(), PARAMS)
    assert (o.state, o.reason) == ("pending", "window_not_complete")


def test_entry_close_return_is_from_the_entry_close() -> None:
    row = P(
        createdAt="2026-01-06T15:00:00Z",
        deadline=day(30),
        market={"anchor": "entry_close", "horizon": 3},
    )
    o = settle_price(row, frame({1: 100, 4: 90}, {1: 100, 4: 95}), PARAMS)
    assert (o.anchor_date, o.end_date) == (day(1), day(4))
    assert (o.ticker_return, o.bench_return, o.rel_return, o.outcome) == (-0.1, -0.05, -0.05, 0)


# ---------- ties, conflicts, voids ----------


def test_exact_tie_is_void_not_a_hit() -> None:
    o = settle_price(P(), frame({14: 110}, {14: 110}), PARAMS)
    assert (o.state, o.reason, o.outcome, o.rel_return) == ("void", "tie", None, 0.0)


def test_returns_are_rounded_to_ten_places_before_the_direction_is_read() -> None:
    o = settle_price(P(), frame({14: 100 * (1 + 1e-12)}, {14: 100.0}), PARAMS)
    assert o.rel_return == 0.0 and o.state == "void"
    o = settle_price(P(), frame({14: 100 * (1 + 3e-10)}, {14: 100.0}), PARAMS)
    assert o.rel_return == 3e-10 and o.outcome == 1


@pytest.mark.parametrize(
    ("status", "conflict"),
    [("open", False), ("hit", False), ("miss", True)],
)
def test_ledger_result_that_disagrees_with_prices_is_flagged_but_prices_win(
    status: str, conflict: bool
) -> None:
    o = settle_price(P(status=status), frame({14: 110}, {14: 104}), PARAMS)
    assert (o.state, o.outcome, o.conflict) == ("settled", 1, conflict)
    assert o.ledger_status == status


def test_ledger_miss_agrees_with_a_price_miss() -> None:
    o = settle_price(P(status="miss"), frame({14: 102}, {14: 105}), PARAMS)
    assert (o.outcome, o.conflict) == (0, False)


def test_a_voided_judgment_stays_void_whatever_the_prices_say() -> None:
    o = settle_price(P(status="void"), frame({14: 110}, {14: 104}), PARAMS)
    assert (o.state, o.reason, o.outcome, o.rel_return) == ("void", "ledger_void", None, None)


def test_pending_price_judgment_with_a_ledger_hit_is_not_settled_from_the_ledger() -> None:
    o = settle_price(P(status="hit"), frame(days=DAYS[:12]), PARAMS)
    assert (o.state, o.outcome) == ("pending", None)


# ---------- data gaps ----------


def test_missing_ticker_or_benchmark_column_is_a_data_gap() -> None:
    closes = frame()
    assert settle_price(P(ticker="NVDA"), closes, PARAMS).reason == "price_missing"
    assert settle_price(P(market={"benchmark": "SOXX"}), closes, PARAMS).reason == "price_missing"
    assert settle_price(P(ticker="NVDA"), closes, PARAMS).state == "data_gap"


def test_a_ticker_cannot_be_judged_against_itself() -> None:
    o = settle_price(P(ticker="SMH"), frame(), PARAMS)
    assert (o.state, o.reason) == ("data_gap", "ticker_is_benchmark")


def test_days_missing_for_one_leg_are_not_trading_days_of_the_pair() -> None:
    closes = frame({14: 110, 15: 111}, {14: 104, 15: 104})
    closes.loc[DAYS[12], "SMH"] = float("nan")  # a hole on the benchmark leg
    # the common calendar skips index 12, so the window 9..+5 now ends at index 15
    o = settle_price(P(), closes, PARAMS)
    assert (o.anchor_date, o.end_date) == (day(9), day(15))
    assert o.ticker_return == 0.11


def test_empty_common_calendar_is_a_data_gap() -> None:
    closes = frame()
    closes["SMH"] = float("nan")
    assert settle_price(P(), closes, PARAMS).reason == "price_missing"


def test_a_long_calendar_gap_inside_the_window_is_a_data_gap() -> None:
    closes = rising().drop(index=DAYS[11:16])  # Monday 01-19 -> Tuesday 01-27: 8 calendar days
    o = settle_price(P(), closes, PARAMS)
    assert (o.state, o.reason) == ("data_gap", "calendar_gap")


def test_a_long_weekend_inside_the_window_is_not_a_gap() -> None:
    closes = rising().drop(index=[DAYS[10]])  # Friday 01-16 -> Tuesday 01-20 = 4 calendar days
    o = settle_price(P(), closes, PARAMS)
    assert (o.state, o.end_date) == ("settled", day(15))  # the dropped day is not a trading day


def test_a_gap_of_exactly_the_limit_is_allowed() -> None:
    closes = rising().drop(index=DAYS[11:15])  # Monday 01-19 -> Monday 01-26: 7 calendar days
    assert settle_price(P(), closes, PARAMS).state == "settled"


def test_gap_limit_comes_from_the_parameters() -> None:
    closes = rising().drop(index=DAYS[11:14])  # Monday 01-19 -> Friday 01-23: 4 calendar days
    tight = SettleParams(**{**PARAMS.model_dump(), "max_calendar_gap_days": 3})
    assert settle_price(P(), closes, PARAMS).state == "settled"
    assert settle_price(P(), closes, tight).reason == "calendar_gap"


def test_gap_outside_the_window_is_ignored() -> None:
    closes = frame({14: 110}, {14: 104}).drop(index=DAYS[20:30])
    assert settle_price(P(), closes, PARAMS).state == "settled"


def test_jump_on_either_leg_inside_the_window_is_not_settled() -> None:
    assert settle_price(P(), frame({12: 160}), PARAMS).reason == "suspect_jump"  # +60%
    assert settle_price(P(), frame({12: 100}, {11: 100, 12: 40}), PARAMS).reason == "suspect_jump"


def test_jump_limit_is_exclusive_and_comes_from_the_parameters() -> None:
    closes = frame({12: 150, 13: 150, 14: 150})  # exactly +50%
    assert settle_price(P(), closes, PARAMS).state == "settled"
    tight = SettleParams(**{**PARAMS.model_dump(), "max_abs_daily_move": 0.4})
    assert settle_price(P(), closes, tight).reason == "suspect_jump"
    assert (
        settle_price(P(), frame({12: 150.5, 13: 150.5, 14: 150.5}), PARAMS).reason == "suspect_jump"
    )


def test_jump_outside_the_window_is_ignored() -> None:
    assert settle_price(P(), frame({14: 110, 25: 300}), PARAMS).state == "settled"


def test_non_positive_price_inside_the_window_is_a_data_gap() -> None:
    assert settle_price(P(), frame({12: 0.0}), PARAMS).reason == "bad_price"
    assert settle_price(P(), frame({12: -5.0}), PARAMS).reason == "bad_price"


# ---------- the close time comes from the parameters ----------


def test_market_close_parameter_moves_the_entry_anchor() -> None:
    early = SettleParams(**{**PARAMS.model_dump(), "market_close": "10:00"})
    row = P(createdAt="2026-01-06T16:00:00Z", deadline=day(30), market={"anchor": "entry_close"})
    # 16:00Z is 11:00 EST: after a 10:00 close, before a 16:00 close
    assert settle_price(row, frame(), PARAMS).anchor_date == day(1)
    assert settle_price(row, frame(), early).anchor_date == day(2)


def test_timezone_parameter_moves_the_entry_anchor() -> None:
    utc = SettleParams(**{**PARAMS.model_dump(), "market_timezone": "UTC"})
    row = P(createdAt="2026-01-06T17:00:00Z", deadline=day(30), market={"anchor": "entry_close"})
    assert settle_price(row, frame(), PARAMS).anchor_date == day(1)  # 12:00 EST
    assert settle_price(row, frame(), utc).anchor_date == day(2)  # 17:00 UTC is after a 16:00 close


# ---------- settle_rows ----------


def test_rows_are_routed_by_kind_filtered_by_purpose_and_ordered_by_id() -> None:
    rows = [
        P(id="z", status="open"),
        L(id="a", status="hit"),
        L(id="m", purpose="learning", status="hit"),
        L(id="c", kind="波动", status="miss"),
    ]
    out = settle_rows(rows, frame({14: 110}, {14: 104}), PARAMS, ASOF)
    assert [(o.id, o.source, o.state) for o in out] == [
        ("a", "ledger", "settled"),
        ("c", "ledger", "settled"),
        ("z", "price", "settled"),
    ]


def test_settlement_is_deterministic_and_leaves_inputs_alone() -> None:
    rows = [P(id="p"), L(id="l")]
    closes = frame({14: 110}, {14: 104})
    before_rows, before_closes = copy.deepcopy(rows), closes.copy()
    first = settle_rows(rows, closes, PARAMS, ASOF)
    assert first == settle_rows(list(reversed(rows)), closes, PARAMS, ASOF)
    assert rows == before_rows and closes.equals(before_closes)


def test_module_has_no_other_public_surprises() -> None:
    assert {"settle_ledger", "settle_price", "settle_rows", "load_settle_params", "Outcome"} <= set(
        dir(settle)
    )


# ---------- reading prices ----------


def write_prices(path: Path, closes: pd.DataFrame, extra: bool = True) -> Path:
    rows = []
    for stamp, series in closes.iterrows():
        for ticker, value in series.items():
            if pd.isna(value):
                continue
            row = {"date": stamp.date().isoformat(), "ticker": ticker, "adj_close": value}
            if extra:
                row.update({"open": 1, "high": 2, "low": 0.5, "close": value, "volume": 10})
            rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_read_closes_gives_a_dates_by_tickers_table(tmp_path: Path) -> None:
    closes = frame({3: 105}, {3: 99})
    got, issues = prices.read_closes(write_prices(tmp_path / "p.csv", closes))
    assert issues == [] and got is not None
    assert list(got.columns) == ["SMH", "TSM"] and got.index.is_monotonic_increasing
    assert got.loc[DAYS[3], "TSM"] == 105 and got.loc[DAYS[3], "SMH"] == 99


def test_read_closes_ignores_row_order_and_extra_columns(tmp_path: Path) -> None:
    closes = frame()
    path = write_prices(tmp_path / "p.csv", closes)
    shuffled = pd.read_csv(path).sample(frac=1, random_state=3)
    shuffled.to_csv(tmp_path / "q.csv", index=False)
    a, _ = prices.read_closes(path)
    b, _ = prices.read_closes(tmp_path / "q.csv")
    assert a is not None and b is not None and a.equals(b)


def test_read_closes_missing_file(tmp_path: Path) -> None:
    got, issues = prices.read_closes(tmp_path / "nope.csv")
    assert got is None and [i.code for i in issues] == ["prices_missing"]


@pytest.mark.parametrize(
    "text",
    [
        "date,ticker\n2026-01-05,TSM\n",
        "date,ticker,adj_close\n2026-01-05,TSM,100\n2026-01-05,TSM,101\n",
        "date,ticker,adj_close\nyesterday,TSM,100\n",
        "date,ticker,adj_close\n2026-01-05,,100\n",
        "",
    ],
)
def test_read_closes_bad_files_are_schema_issues(tmp_path: Path, text: str) -> None:
    path = tmp_path / "p.csv"
    path.write_text(text, encoding="utf-8")
    got, issues = prices.read_closes(path)
    assert got is None and [i.code for i in issues] == ["prices_schema"]


def test_blank_or_text_adj_close_becomes_a_missing_value_not_a_number(tmp_path: Path) -> None:
    path = tmp_path / "p.csv"
    path.write_text(
        "date,ticker,adj_close\n2026-01-05,TSM,\n2026-01-06,TSM,n/a\n2026-01-07,TSM,5\n",
        encoding="utf-8",
    )
    got, issues = prices.read_closes(path)
    assert issues == [] and got is not None
    assert got["TSM"].isna().tolist() == [True, True, False]


def test_digest_covers_only_the_used_series_and_is_order_free() -> None:
    closes = frame({3: 105})
    closes["NVDA"] = 7.0
    base = prices.digest(closes, {"TSM", "SMH"})
    assert base == prices.digest(closes[["SMH", "TSM", "NVDA"]], ["SMH", "TSM"])
    other = closes.copy()
    other["NVDA"] = 8.0
    assert prices.digest(other, {"TSM", "SMH"}) == base
    moved = closes.copy()
    moved.loc[DAYS[3], "TSM"] = 106
    assert prices.digest(moved, {"TSM", "SMH"}) != base
    assert len(base) == 64


def test_digest_depends_on_the_dates_and_ignores_missing_values() -> None:
    closes = frame({3: 105})
    base = prices.digest(closes, {"TSM"})
    shifted = closes.copy()
    shifted.index = shifted.index + pd.Timedelta(days=7)
    assert prices.digest(shifted, {"TSM"}) != base
    extra = closes.copy()
    extra.loc[pd.Timestamp("2026-03-30"), :] = float("nan")  # a date without any price
    extra = extra.sort_index()
    assert prices.digest(extra, {"TSM"}) == base
    holes = closes.copy()
    holes.loc[DAYS[7], "TSM"] = float("nan")
    assert prices.digest(holes, {"TSM"}) == prices.digest(closes.drop(index=DAYS[7]), {"TSM"})


# ---------- the settlement store ----------

NOW = datetime(2026, 3, 1, 9, 0, 0, tzinfo=UTC)


def make_export(rows: list[dict[str, Any]], exported_at: str) -> dict[str, Any]:
    return {"format": "judgments_export.v1", "exported_at": exported_at, "judgments": rows}


def do_import(tmp_path: Path, rows: list[dict[str, Any]], exported_at: str) -> Path:
    data = tmp_path / "data" / "external" / "judgments"
    src = tmp_path / f"in_{exported_at[:19].replace(':', '')}.json"
    src.write_text(json.dumps(make_export(rows, exported_at), ensure_ascii=False), encoding="utf-8")
    result = importer.import_export(
        src, data_dir=data, known_tickers=KNOWN, method_ids=METHODS,
        now=NOW, dry_run=False,
    )  # fmt: skip
    assert result.status == "imported", result.issues
    return data


def ledger_rows(**status: str) -> list[dict[str, Any]]:
    entry_market = {"anchor": "entry_close"}
    return [
        L(id="L1", statement=SECRET, status=status.get("L1", "open")),
        P(id="P1", ticker="TSM"),  # event index 10: needs prices through index 14
        # entry index 0, end index 5
        P(
            id="P2",
            ticker="NVDA",
            createdAt="2026-01-02T12:00:00Z",
            market=entry_market,
            deadline=day(30),
        ),
        L(id="LR", purpose="learning", status="hit"),
        L(id="V1", status="void"),
    ]


def price_table(n: int) -> pd.DataFrame:
    closes = frame({9: 100, 14: 110}, {9: 100, 14: 104}).iloc[:n].copy()
    closes["NVDA"] = [100.0 + i for i in range(n)]
    return closes


def settle_now(
    data: Path, prices_path: Path, asof: date = ASOF, dry_run: bool = False, now: datetime = NOW
) -> Any:
    return run_settle(
        data_dir=data, prices_path=prices_path, params=PARAMS, asof=asof, now=now, dry_run=dry_run
    )


def snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


@pytest.fixture
def world(tmp_path: Path) -> tuple[Path, Path]:
    data = do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    return data, short


def test_first_settlement_writes_a_file_and_a_chained_log_line(world: tuple[Path, Path]) -> None:
    data, short = world
    result = settle_now(data, short, asof=date(2026, 1, 12))
    assert result.status == "settled" and result.seq == 1
    # P2 settled on prices; L1 and P1 are pending; V1 is void; the learning row is not counted
    assert result.counts == {"settled": 1, "pending": 2, "void": 1, "data_gap": 0, "conflicts": 0}
    lines = (data / "settlements.jsonl").read_bytes().split(b"\n")
    assert lines[-1] == b"" and len(lines) == 2
    record = json.loads(lines[0])
    assert record["seq"] == 1 and record["prev_line_sha256"] == "0" * 64
    assert record["asof"] == "2026-01-12" and record["settled_at"] == "2026-03-01T09:00:00Z"
    assert record["new_settled_ids"] == ["P2"] and record["changed_ids"] == []
    assert record["counts"] == result.counts
    out_file = data / "settlements" / record["file"]
    assert out_file.is_file() and record["file"].startswith("settlement_2026-01-12_")
    import hashlib

    assert hashlib.sha256(out_file.read_bytes()).hexdigest() == record["file_sha256"]
    assert record["file"].endswith(record["file_sha256"][:8] + ".json")
    doc = json.loads(out_file.read_text("utf-8"))
    assert doc["format"] == "judgment_settlements.v1" and doc["asof"] == "2026-01-12"
    assert [o["id"] for o in doc["outcomes"]] == ["L1", "P1", "P2", "V1"]
    imports = json.loads((data / "imports.jsonl").read_text("utf-8").splitlines()[-1])
    assert doc["export_sha256"] == imports["export_sha256"] == record["export_sha256"]
    assert len(doc["prices_sha256"]) == 64 and record["prices_sha256"] == doc["prices_sha256"]


def test_settlement_outputs_hold_no_judgment_text(world: tuple[Path, Path]) -> None:
    data, short = world
    settle_now(data, short)
    for name, raw in snapshot(data / "settlements").items():
        assert SECRET.encode() not in raw, name
    assert SECRET.encode() not in (data / "settlements.jsonl").read_bytes()
    keys = set(json.loads(next((data / "settlements").iterdir()).read_text("utf-8"))["outcomes"][0])
    assert keys == {f.name for f in fields(Outcome)}


def test_same_inputs_again_is_unchanged_and_writes_nothing(world: tuple[Path, Path]) -> None:
    data, short = world
    settle_now(data, short)
    before = snapshot(data)
    result = settle_now(data, short, asof=date(2026, 3, 2))  # later day, nothing new
    assert (result.status, result.seq) == ("unchanged", 1)
    assert snapshot(data) == before


def test_a_new_settlement_appends_to_the_chain_and_names_the_newly_settled(
    tmp_path: Path, world: tuple[Path, Path]
) -> None:
    data, short = world
    settle_now(data, short, asof=date(2026, 1, 12))
    full = write_prices(tmp_path / "full.csv", price_table(40))
    result = settle_now(data, full, asof=date(2026, 3, 1))
    assert result.status == "settled" and result.seq == 2
    lines = (data / "settlements.jsonl").read_bytes().split(b"\n")[:-1]
    first, second = (json.loads(x) for x in lines)
    import hashlib

    assert second["prev_line_sha256"] == hashlib.sha256(lines[0]).hexdigest()
    assert second["new_settled_ids"] == ["P1"] and second["changed_ids"] == []
    assert second["counts"] == {
        "settled": 2,
        "pending": 1,
        "void": 1,
        "data_gap": 0,
        "conflicts": 0,
    }
    assert len(list((data / "settlements").iterdir())) == 2
    assert first["file"] != second["file"]


def test_dry_run_writes_nothing_not_even_directories(tmp_path: Path) -> None:
    data = do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    before = snapshot(data)
    result = settle_now(data, short, dry_run=True)
    assert result.status == "settled" and result.seq == 1
    assert snapshot(data) == before and not (data / "settlements").exists()


def test_a_settled_price_outcome_is_frozen_against_revised_prices(
    tmp_path: Path, world: tuple[Path, Path]
) -> None:
    data, short = world
    settle_now(data, short)
    before = snapshot(data)
    revised = price_table(8)
    revised.loc[DAYS[5], "NVDA"] = 50.0  # P2 ran from index 0 to 5: the ticker now lost money
    result = settle_now(data, write_prices(tmp_path / "rev.csv", revised))
    assert result.status == "rejected"
    assert [(i.code, i.id) for i in result.issues] == [("settlement_changed", "P2")]
    assert snapshot(data) == before


def test_a_revision_that_does_not_move_the_result_or_dates_is_tolerated(
    tmp_path: Path, world: tuple[Path, Path]
) -> None:
    data, short = world
    settle_now(data, short)
    nudged = price_table(8)
    nudged["NVDA"] = [100.0 + i for i in range(8)]
    nudged.loc[DAYS[3], "NVDA"] = 103.5  # inside the window, not at its ends
    result = settle_now(data, write_prices(tmp_path / "n.csv", nudged))
    assert result.status == "unchanged"


def test_a_changed_manual_result_is_recorded_not_rejected(tmp_path: Path) -> None:
    data = do_import(tmp_path, ledger_rows(L1="hit"), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    settle_now(data, short)
    rows = ledger_rows(L1="miss")
    src = tmp_path / "second.json"
    src.write_text(json.dumps(make_export(rows, "2026-03-01T08:30:00Z")), encoding="utf-8")
    res = importer.import_export(
        src, data_dir=data, known_tickers=KNOWN, method_ids=METHODS, now=NOW
    )
    assert res.status == "imported", res.issues
    result = settle_now(data, short)
    assert result.status == "settled" and result.seq == 2
    last = json.loads((data / "settlements.jsonl").read_text("utf-8").splitlines()[-1])
    assert last["changed_ids"] == ["L1"] and last["new_settled_ids"] == []


def test_a_manual_result_arriving_later_is_a_new_settlement(tmp_path: Path) -> None:
    data = do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    settle_now(data, short)
    src = tmp_path / "second.json"
    src.write_text(
        json.dumps(make_export(ledger_rows(L1="hit"), "2026-03-01T08:30:00Z")), encoding="utf-8"
    )
    importer.import_export(src, data_dir=data, known_tickers=KNOWN, method_ids=METHODS, now=NOW)
    settle_now(data, short)
    last = json.loads((data / "settlements.jsonl").read_text("utf-8").splitlines()[-1])
    assert last["new_settled_ids"] == ["L1"]
    doc = json.loads((data / "settlements" / last["file"]).read_text("utf-8"))
    assert {o["id"]: o["outcome"] for o in doc["outcomes"]}["L1"] == 1


def test_conflicts_are_counted(tmp_path: Path) -> None:
    rows = [P(id="P1", status="miss"), L(id="L1", status="hit")]
    data = do_import(tmp_path, rows, "2026-03-01T08:00:00Z")
    result = settle_now(data, write_prices(tmp_path / "p.csv", price_table(40)))
    assert result.counts["conflicts"] == 1 and result.counts["settled"] == 2


def test_asof_cannot_go_backwards(world: tuple[Path, Path]) -> None:
    data, short = world
    settle_now(data, short, asof=date(2026, 2, 1))
    before = snapshot(data)
    result = settle_now(data, short, asof=date(2026, 1, 31))
    assert result.status == "rejected" and [i.code for i in result.issues] == ["asof_backwards"]
    assert snapshot(data) == before


def test_no_import_no_settlement(tmp_path: Path) -> None:
    short = write_prices(tmp_path / "short.csv", price_table(8))
    result = settle_now(tmp_path / "empty", short)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["no_import"]
    assert not (tmp_path / "empty").exists()


def test_missing_or_bad_prices_are_refused_before_anything_is_written(
    tmp_path: Path, world: tuple[Path, Path]
) -> None:
    data, _ = world
    before = snapshot(data)
    r1 = settle_now(data, tmp_path / "nope.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text("date,ticker\n", encoding="utf-8")
    r2 = settle_now(data, bad)
    assert [i.code for i in r1.issues] == ["prices_missing"]
    assert [i.code for i in r2.issues] == ["prices_schema"]
    assert r1.status == r2.status == "rejected" and snapshot(data) == before


def test_tampered_import_copy_is_refused(world: tuple[Path, Path]) -> None:
    data, short = world
    victim = next((data / "exports").iterdir())
    victim.write_bytes(victim.read_bytes() + b" ")
    result = settle_now(data, short)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["baseline_tampered"]


def test_tampered_settlement_file_or_log_breaks_the_chain(world: tuple[Path, Path]) -> None:
    data, short = world
    settle_now(data, short)
    out = next((data / "settlements").iterdir())
    original = out.read_bytes()
    out.write_bytes(original + b" ")
    assert [i.code for i in settle_now(data, short, asof=date(2026, 3, 5)).issues] == [
        "chain_broken"
    ]
    out.write_bytes(original)
    log = data / "settlements.jsonl"
    text = log.read_bytes()
    log.write_bytes(text.replace(b'"seq":1', b'"seq":2'))
    assert [i.code for i in settle_now(data, short, asof=date(2026, 3, 5)).issues] == [
        "chain_broken"
    ]
    log.write_bytes(text.rstrip(b"\n"))
    assert [i.code for i in settle_now(data, short, asof=date(2026, 3, 5)).issues] == [
        "chain_broken"
    ]


def test_only_used_tickers_are_hashed_so_unrelated_price_changes_do_not_matter(
    tmp_path: Path, world: tuple[Path, Path]
) -> None:
    data, short = world
    settle_now(data, short)
    other = price_table(8)
    other["QQQ"] = 5.0  # a ticker no judgment uses
    result = settle_now(data, write_prices(tmp_path / "other.csv", other))
    assert result.status == "unchanged"


def step_table(n: int = 40, drop: list[int] | tuple[int, ...] = (), tsm_end: float = 110.0) -> Any:
    """Ticker steps 100 -> tsm_end at index 12 and stays; benchmark flat. Dropped indexes vanish."""
    closes = frame({i: tsm_end for i in range(12, n)}).iloc[:n]
    return closes.drop(index=[DAYS[i] for i in drop])


def frozen_world(tmp_path: Path, name: str, closes: pd.DataFrame) -> tuple[Path, Path]:
    root = tmp_path / name
    root.mkdir()
    data = do_import(root, [P(id="P1")], "2026-03-01T08:00:00Z")
    return data, write_prices(root / "p.csv", closes)


def test_a_moved_anchor_or_end_date_is_a_change_even_when_the_returns_agree(
    tmp_path: Path,
) -> None:
    for name, drop in (("end", [10]), ("anchor", [9])):
        data, path = frozen_world(tmp_path, name, step_table())
        assert settle_now(data, path).counts["settled"] == 1
        before = snapshot(data)
        revised = write_prices(tmp_path / f"{name}_revised.csv", step_table(drop=drop))
        result = settle_now(data, revised)
        assert result.status == "rejected", name
        assert [(i.code, i.id) for i in result.issues] == [("settlement_changed", "P1")], name
        assert snapshot(data) == before


def test_a_revision_that_flips_the_outcome_inside_the_tolerance_is_a_change(
    tmp_path: Path,
) -> None:
    tiny = 100.0 + 3e-8  # relative return +3e-10: a hit by the smallest margin
    data, path = frozen_world(tmp_path, "flip", step_table(tsm_end=tiny))
    settle_now(data, path)
    flipped = write_prices(tmp_path / "flipped.csv", step_table(tsm_end=100.0 - 3e-8))
    result = settle_now(data, flipped)
    assert [i.code for i in result.issues] == ["settlement_changed"]


def test_the_freeze_tolerance_is_one_billionth_of_a_return(tmp_path: Path) -> None:
    data, path = frozen_world(tmp_path, "tol", step_table())
    settle_now(data, path)
    inside = write_prices(tmp_path / "in.csv", step_table(tsm_end=110.00000005))  # +5e-10
    tolerated = settle_now(data, inside)  # accepted, and recorded as a new settlement
    assert tolerated.status == "settled" and tolerated.seq == 2 and tolerated.issues == []
    outside = write_prices(tmp_path / "out.csv", step_table(tsm_end=110.0000002))  # +2e-9
    assert [i.code for i in settle_now(data, outside).issues] == ["settlement_changed"]
    visible = write_prices(tmp_path / "vis.csv", step_table(tsm_end=111.0))  # +1e-2, same outcome
    assert [i.code for i in settle_now(data, visible).issues] == ["settlement_changed"]


def test_prices_hash_in_the_record_follows_only_the_used_series(tmp_path: Path) -> None:
    def prices_sha(name: str, closes: pd.DataFrame) -> str:
        root = tmp_path / name
        root.mkdir()
        data = do_import(root, [P(id="P1")], "2026-03-01T08:00:00Z")
        settle_now(data, write_prices(root / "p.csv", closes))
        return json.loads((data / "settlements.jsonl").read_text("utf-8"))["prices_sha256"]

    base = step_table()
    with_other = base.copy()
    with_other["QQQ"] = 5.0
    moved = base.copy()
    moved.loc[DAYS[3], "SMH"] = 99.0
    assert prices_sha("a", base) == prices_sha("b", with_other)
    assert prices_sha("a2", base) != prices_sha("c", moved)


def test_settled_at_is_written_in_utc(tmp_path: Path) -> None:
    from datetime import timedelta, timezone

    data, path = frozen_world(tmp_path, "tz", step_table())
    tokyo = datetime(2026, 3, 1, 18, 0, 0, tzinfo=timezone(timedelta(hours=9)))
    settle_now(data, path, now=tokyo)
    record = json.loads((data / "settlements.jsonl").read_text("utf-8"))
    assert record["settled_at"] == "2026-03-01T09:00:00Z"


# ---------- command line ----------


def cli(args: list[str]) -> Any:
    return CliRunner().invoke(app, args)


def test_cli_settle_prints_counts_only(tmp_path: Path) -> None:
    data = do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    root = tmp_path
    short = write_prices(tmp_path / "short.csv", price_table(8))
    out = cli(["settle", "--root", str(root), "--asof", "2026-01-12", "--prices", str(short)])
    assert out.exit_code == 0, out.output
    assert (
        out.output.strip()
        == "settled seq=1 asof=2026-01-12 settled=1 pending=2 void=1 data_gap=0 conflicts=0"
    )
    assert SECRET not in out.output
    again = cli(["settle", "--root", str(root), "--asof", "2026-01-12", "--prices", str(short)])
    assert again.output.strip() == "unchanged seq=1"
    dry = cli(
        ["settle", "--root", str(root), "--asof", "2026-01-13", "--prices", str(short), "--dry-run"]
    )
    assert dry.exit_code == 0 and dry.output.strip() == "unchanged seq=1"
    assert (data / "settlements.jsonl").is_file()


def test_cli_dry_run_line_and_nothing_written(tmp_path: Path) -> None:
    do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    out = cli(
        [
            "settle",
            "--root",
            str(tmp_path),
            "--asof",
            "2026-01-12",
            "--prices",
            str(short),
            "--dry-run",
        ]
    )
    assert out.exit_code == 0
    assert (
        out.output.strip() == "ok asof=2026-01-12 settled=1 pending=2 void=1 data_gap=0 conflicts=0"
    )
    assert not (tmp_path / "data" / "external" / "judgments" / "settlements").exists()


def test_cli_rejection_prints_issue_codes_and_exits_one(tmp_path: Path) -> None:
    short = write_prices(tmp_path / "short.csv", price_table(8))
    out = cli(
        ["settle", "--root", str(tmp_path / "none"), "--asof", "2026-01-12", "--prices", str(short)]
    )
    assert out.exit_code == 1
    assert out.output.splitlines() == [
        "issue no_import id=- field=-",
        "rejected: nothing was written",
    ]


def test_cli_defaults_prices_to_the_repository_state_not_the_judgments_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.research_judgments import __main__ as entry_point

    code = tmp_path / "code"
    (code / "data" / "raw").mkdir(parents=True)
    write_prices(code / "data" / "raw" / "prices_daily.csv", price_table(8))
    monkeypatch.setattr(entry_point, "PROJECT_ROOT", code)
    monkeypatch.delenv("AITS_STATE_ROOT", raising=False)
    judgments = tmp_path / "judgments"
    judgments.mkdir()
    do_import(judgments, ledger_rows(), "2026-03-01T08:00:00Z")
    out = cli(["settle", "--root", str(judgments), "--asof", "2026-01-12", "--dry-run"])
    assert out.exit_code == 0, out.output
    assert out.output.strip().startswith("ok asof=2026-01-12 settled=1")


def test_cli_asof_defaults_to_today_in_utc(tmp_path: Path) -> None:
    do_import(tmp_path, ledger_rows(), "2026-03-01T08:00:00Z")
    short = write_prices(tmp_path / "short.csv", price_table(8))
    before = datetime.now(UTC).date().isoformat()
    out = cli(["settle", "--root", str(tmp_path), "--prices", str(short), "--dry-run"])
    after = datetime.now(UTC).date().isoformat()
    assert out.exit_code == 0, out.output
    assert out.output.split()[1] in {f"asof={before}", f"asof={after}"}


def test_the_module_can_be_run_with_python_dash_m_and_offers_every_command() -> None:
    import os
    import subprocess
    import sys

    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}
    run = subprocess.run(
        [sys.executable, "-m", "ai_trading_system.research_judgments", "--help"],
        capture_output=True, text=True, env=env, cwd=PROJECT_ROOT, timeout=120,
    )  # fmt: skip
    assert run.returncode == 0, run.stderr
    for command in ("check", "import", "settle"):
        assert command in run.stdout


def test_cli_rejects_a_bad_asof(tmp_path: Path) -> None:
    out = cli(["settle", "--root", str(tmp_path), "--asof", "01/12/2026"])
    assert out.exit_code != 0


# ---------- the repository wiring ----------


def test_eval_config_is_zone_c_and_the_code_is_not() -> None:
    zones = yaml.safe_load((PROJECT_ROOT / "config" / "gov008_ship.yaml").read_text("utf-8"))[
        "zones"
    ]
    from fnmatch import fnmatch

    def zone_c(path: str) -> bool:
        return any(fnmatch(path, pattern) for pattern in zones["C"])

    assert zone_c("config/research/research_judgment_eval.yaml")
    for name in ("settle", "settlements", "prices"):
        assert not zone_c(f"src/ai_trading_system/research_judgments/{name}.py")
    assert not zone_c("tests/test_research_judgments_settle.py")


def test_settlement_thresholds_are_not_hardcoded_in_the_code() -> None:
    source = PROJECT_ROOT / "src" / "ai_trading_system" / "research_judgments"
    text = "".join(
        (source / n).read_text("utf-8") for n in ("settle.py", "settlements.py", "prices.py")
    )
    for forbidden in (
        "0.5",
        "America/New_York",
        '"16:00"',
        "max_calendar_gap_days=",
        "> 7",
        ">= 7",
    ):
        assert forbidden not in text, forbidden


def test_system_flow_mentions_settlement() -> None:
    flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text("utf-8")
    lines = [x for x in flow.splitlines() if "research_judgments" in x and "settle" in x]
    assert len(lines) == 1
    for needle in ("settlements.jsonl", "research_judgment_eval.yaml", "check|import|settle"):
        assert needle in lines[0], needle
    assert sum("TRADING-2569" in x for x in flow.splitlines()) == 1  # replaced, not duplicated
