"""TRADING-2565-45: the forward ledger (the daily band, fixed 0.8, F0001) on synthetic files."""

from __future__ import annotations

import ast
import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_probes import forward_ledger as fl

COMMIT = "0123456789abcdef0123456789abcdef01234567"
PROVENANCE = {"commit": COMMIT, "branch": "main", "code_modified": False}
PRICE_START = date(2025, 6, 2)
LAST_DAY = date(2026, 11, 6)
NO_CAPTURE = date(2026, 10, 15)
NO_SNAPSHOT = date(2026, 10, 20)
BANDS = (
    (0.4, 0.6, "中性", 0.4, 0.6),
    (0.6, 0.8, "偏重仓", 0.6, 1.0),
    (0.2, 0.4, "防守", 0.4, 0.6),
)


def _prices() -> pd.Series:
    days = fl.trading_days(PRICE_START, LAST_DAY)
    returns = []
    for k, day in enumerate(days):
        value = 0.0006 + 0.011 * math.sin(k / 6.0 + 3.0)
        if date(2025, 11, 3) <= day <= date(2026, 5, 29):
            value -= 0.0025
        returns.append(value)
    return pd.Series(100 * np.cumprod(1 + np.array(returns)), index=pd.DatetimeIndex(days))


PRICES = _prices()


def _band(day: date) -> tuple[float, float, str, float, float]:
    return BANDS[day.toordinal() % len(BANDS)]


def _write_capture(root: Path, day: date) -> None:
    path = root / fl.PRICES_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    history = PRICES[PRICES.index <= pd.Timestamp(day)]
    rows = []
    for stamp, value in history.items():
        text = stamp.date().isoformat()
        rows.append(f"{text},SPY,1,1,1,1,{value * 1.7:.10f},100")
        rows.append(f"{text},QQQ,1,1,1,1,{value:.10f},100")
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def _write_snapshot(root: Path, day: date, band: tuple[float, float, str, float, float]) -> None:
    low, high, label, model_low, model_high = band
    path = root / fl.SNAPSHOT_REL.format(day=day.isoformat())
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "schema_version": 1,
        "signal_date": day.isoformat(),
        "positions": {
            "final_risk_asset_ai_band": {
                "min_position": low,
                "max_position": high,
                "label": label,
            },
            "model_risk_asset_ai_band": {
                "min_position": model_low,
                "max_position": model_high,
                "label": "模型",
            },
            "final_total_risk_asset_band": {"min_position": 0.9, "max_position": 1.0, "label": "x"},
        },
    }
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")


@pytest.fixture()
def daily_root(tmp_path: Path) -> Path:
    root = tmp_path / "daily"
    for day in fl.trading_days(date(2026, 10, 8), LAST_DAY):
        if day != NO_CAPTURE:
            _write_capture(root, day)
        if day != NO_SNAPSHOT:
            _write_snapshot(root, day, _band(day))
    return root


@pytest.fixture()
def ledger_dir(tmp_path: Path) -> Path:
    return tmp_path / "forward"


def _mid(day: date) -> float:
    low, high, *_ = _band(day)
    return (low + high) / 2.0


def _return(day: date) -> float:
    position = PRICES.index.get_loc(pd.Timestamp(day))
    return float(PRICES.iloc[position] / PRICES.iloc[position - 1] - 1.0)


def _lines(ledger_dir: Path) -> list[bytes]:
    return (ledger_dir / fl.LEDGER_FILE).read_bytes().splitlines()


# Constants ---------------------------------------------------------------------------------------


def test_constants_name_the_owner_decisions_and_the_lines() -> None:
    assert fl.RUN_ID == "TRADING-2565-45"
    assert fl.SCHEMA == "forward_ledger.v1"
    assert fl.DAILY_POLICY == "owner_decision:TRADING-2565:2026-10-22:daily_policy"
    assert fl.READ_DAILY_OUTPUTS == "owner_decision:TRADING-2565:2026-10-22:read_daily_outputs"
    assert (fl.BACKFILL_START, fl.FORWARD_START, fl.HELD_FROM) == (
        date(2026, 10, 12),
        date(2026, 10, 21),
        date(2026, 10, 8),
    )
    assert fl.FIXED_EXPOSURE == 0.8
    assert fl.OVERLAY_ID == "F0001"
    assert (fl.OVERLAY_CONFIG.question, fl.OVERLAY_CONFIG.strategy) == ("QQQ-Q3", "fast_slow")
    assert fl.OVERLAY_CONFIG.schedule is None
    assert fl.SNAPSHOT_REL == "data/processed/decision_snapshots/decision_snapshot_{day}.json"
    assert fl.PRICES_REL == "data/raw/daily_input_capture/{day}/market_macro/prices_daily.csv"
    assert fl.DEFAULT_LEDGER_DIR == PROJECT_ROOT / "research" / "_forward"
    assert (fl.LEDGER_FILE, fl.WEEKLY_FILE) == ("ledger.jsonl", "weekly.md")
    assert fl.ROW_KEYS == {
        "schema_version",
        "seq",
        "date",
        "backfill",
        "qqq_tr",
        "price_file",
        "price_last_date",
        "exp_daily",
        "daily_min",
        "daily_max",
        "daily_label",
        "daily_model_mid",
        "snapshot_file",
        "exp_daily_held",
        "exp_fixed",
        "overlay",
        "exp_overlay",
        "ret_daily",
        "ret_fixed",
        "ret_overlay",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }


def test_trading_days_skip_weekends_and_full_day_holidays() -> None:
    assert fl.trading_days(date(2026, 11, 25), date(2026, 11, 30)) == [
        date(2026, 11, 25),
        date(2026, 11, 27),
        date(2026, 11, 30),
    ]
    assert fl.trading_days(date(2026, 10, 10), date(2026, 10, 11)) == []


# Inputs ------------------------------------------------------------------------------------------


def test_read_band_takes_the_final_band_midpoint_and_the_model_midpoint(daily_root: Path) -> None:
    day = date(2026, 10, 13)
    band = fl.read_band(daily_root, day)
    low, high, label, model_low, model_high = _band(day)
    assert band is not None
    assert (band.min_position, band.max_position, band.label) == (low, high, label)
    assert band.mid == pytest.approx((low + high) / 2.0, abs=1e-15)
    assert band.model_mid == pytest.approx((model_low + model_high) / 2.0, abs=1e-15)
    assert band.file == "data/processed/decision_snapshots/decision_snapshot_2026-10-13.json"
    assert fl.read_band(daily_root, NO_SNAPSHOT) is None


@pytest.mark.parametrize(
    "band",
    [
        (0.6, 0.4, "反", 0.4, 0.6),
        (0.4, 1.2, "超", 0.4, 0.6),
        (-0.1, 0.4, "负", 0.4, 0.6),
        (0.4, 0.6, "中性", 0.7, 0.5),
    ],
)
def test_read_band_rejects_a_band_outside_zero_to_one_or_reversed(
    tmp_path: Path, band: tuple[float, float, str, float, float]
) -> None:
    _write_snapshot(tmp_path, date(2026, 10, 13), band)
    with pytest.raises(fl.ForwardLedgerError):
        fl.read_band(tmp_path, date(2026, 10, 13))


def test_read_band_rejects_a_true_or_false_position(tmp_path: Path) -> None:
    _write_snapshot(tmp_path, date(2026, 10, 13), (False, 0.6, "中性", 0.4, 0.6))
    with pytest.raises(fl.ForwardLedgerError):
        fl.read_band(tmp_path, date(2026, 10, 13))


def test_read_band_rejects_a_snapshot_without_the_model_band(tmp_path: Path) -> None:
    path = tmp_path / fl.SNAPSHOT_REL.format(day="2026-10-13")
    path.parent.mkdir(parents=True)
    positions = {
        "final_risk_asset_ai_band": {"min_position": 0.4, "max_position": 0.6, "label": "中性"}
    }
    path.write_text(json.dumps({"positions": positions}), encoding="utf-8")
    with pytest.raises(fl.ForwardLedgerError):
        fl.read_band(tmp_path, date(2026, 10, 13))


def test_read_prices_keeps_qqq_drops_blank_prices_and_rejects_duplicates(tmp_path: Path) -> None:
    path = tmp_path / fl.PRICES_REL.format(day="2026-10-13")
    path.parent.mkdir(parents=True)
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2026-10-13,QQQ,1,1,1,1,110,1\n"
        "2026-10-09,QQQ,1,1,1,1,100,1\n"
        "2026-10-12,QQQ,1,1,1,1,,1\n"
        "2026-10-12,SPY,1,1,1,1,50,1\n",
        encoding="utf-8",
    )
    prices = fl.read_prices(tmp_path, date(2026, 10, 13))
    assert prices is not None
    assert list(prices.index.date) == [date(2026, 10, 9), date(2026, 10, 13)]
    assert fl.day_return(prices, date(2026, 10, 13)) == pytest.approx(0.1, abs=1e-15)
    assert fl.day_return(prices, date(2026, 10, 9)) is None
    assert fl.day_return(prices, date(2026, 10, 12)) is None
    assert fl.read_prices(tmp_path, date(2026, 10, 14)) is None
    path.write_text(
        "date,ticker,adj_close\n2026-10-13,QQQ,110\n2026-10-13,QQQ,111\n", encoding="utf-8"
    )
    with pytest.raises(fl.ForwardLedgerError):
        fl.read_prices(tmp_path, date(2026, 10, 13))
    path.write_text("date,ticker,adj_close\n2026-10-13,QQQ,0\n", encoding="utf-8")
    with pytest.raises(fl.ForwardLedgerError):
        fl.read_prices(tmp_path, date(2026, 10, 13))


# Append (requirement test a) ---------------------------------------------------------------------


def test_append_writes_one_row_per_trading_day_with_hand_computed_fields(
    daily_root: Path, ledger_dir: Path
) -> None:
    rows = fl.append(ledger_dir, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    days = fl.trading_days(date(2026, 10, 12), date(2026, 10, 23))
    assert [row["date"] for row in rows] == [day.isoformat() for day in days]
    assert [row["seq"] for row in rows] == list(range(1, len(days) + 1))
    # Snapshots made before Baker's tap (2026-10-21 15:06:51Z) are the backfill: as_of 10-20.
    assert [row["backfill"] for row in rows] == [day <= date(2026, 10, 20) for day in days]
    by_day = {row["date"]: row for row in rows}

    first = by_day["2026-10-12"]
    assert set(first) == fl.ROW_KEYS
    assert first["prev_sha256"] is None
    assert first["schema_version"] == fl.SCHEMA
    assert first["exp_daily"] == pytest.approx(_mid(date(2026, 10, 12)), abs=1e-15)
    assert first["exp_daily_held"] == pytest.approx(_mid(date(2026, 10, 8)), abs=1e-15)
    assert first["qqq_tr"] == pytest.approx(_return(date(2026, 10, 12)), abs=1e-12)
    assert first["ret_daily"] == pytest.approx(
        _mid(date(2026, 10, 8)) * _return(date(2026, 10, 12)), abs=1e-12
    )
    assert first["exp_fixed"] == 0.8
    assert first["ret_fixed"] == pytest.approx(0.8 * _return(date(2026, 10, 12)), abs=1e-12)
    assert first["price_file"] == fl.PRICES_REL.format(day="2026-10-12")
    assert first["price_last_date"] == "2026-10-12"
    assert first["snapshot_file"] == fl.SNAPSHOT_REL.format(day="2026-10-12")
    low, high, label, model_low, model_high = _band(date(2026, 10, 12))
    assert (first["daily_min"], first["daily_max"], first["daily_label"]) == (low, high, label)
    assert first["daily_model_mid"] == pytest.approx((model_low + model_high) / 2, abs=1e-15)
    assert (first["code_commit"], first["code_modified"]) == (COMMIT, False)
    assert first["overlay"] == "F0001"
    assert first["notes"] == []

    # The band published at t is held on the second trading day after t.
    assert by_day["2026-10-13"]["exp_daily_held"] == pytest.approx(_mid(date(2026, 10, 9)))
    assert by_day["2026-10-14"]["exp_daily_held"] == pytest.approx(_mid(date(2026, 10, 12)))

    # A day without its own capture takes its price from the next capture; no line breaks.
    missing_price = by_day[NO_CAPTURE.isoformat()]
    assert missing_price["qqq_tr"] == pytest.approx(_return(NO_CAPTURE), abs=1e-12)
    assert missing_price["price_file"] == fl.PRICES_REL.format(day="2026-10-16")
    assert missing_price["price_last_date"] == "2026-10-16"
    assert missing_price["ret_fixed"] == pytest.approx(0.8 * _return(NO_CAPTURE), abs=1e-12)
    assert missing_price["ret_daily"] == pytest.approx(
        _mid(date(2026, 10, 13)) * _return(NO_CAPTURE), abs=1e-12
    )
    assert missing_price["ret_overlay"] == pytest.approx(
        missing_price["exp_overlay"] * _return(NO_CAPTURE), abs=1e-12
    )
    assert missing_price["notes"] == ["没有当天的价格捕获文件，价格取自 2026-10-16 的文件"]

    missing_band = by_day[NO_SNAPSHOT.isoformat()]
    assert missing_band["exp_daily"] is None and missing_band["snapshot_file"] is None
    assert missing_band["daily_label"] is None and missing_band["daily_model_mid"] is None
    assert "没有当天的决策快照" in missing_band["notes"]
    # 10-22 holds the band of 10-20, which is missing: the last published band (10-19) is held.
    assert by_day["2026-10-22"]["exp_daily_held"] == pytest.approx(_mid(date(2026, 10, 19)))
    assert by_day["2026-10-23"]["exp_daily_held"] == pytest.approx(_mid(date(2026, 10, 21)))

    assert rows[1]["prev_sha256"] == fl.chain_digest(_lines(ledger_dir)[0])


def test_each_day_takes_its_return_from_that_days_price_capture(
    daily_root: Path, ledger_dir: Path
) -> None:
    # The capture of 2026-10-13 says QQQ rose 10% that day; later captures keep the synthetic path.
    path = daily_root / fl.PRICES_REL.format(day="2026-10-13")
    before = float(PRICES.loc[pd.Timestamp(date(2026, 10, 12))])
    path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        f"2026-10-12,QQQ,1,1,1,1,{before!r},1\n"
        f"2026-10-13,QQQ,1,1,1,1,{before * 1.1!r},1\n",
        encoding="utf-8",
    )
    rows = fl.append(ledger_dir, daily_root, date(2026, 10, 14), provenance=PROVENANCE)
    by_day = {row["date"]: row for row in rows}
    assert by_day["2026-10-13"]["qqq_tr"] == pytest.approx(0.1, abs=1e-12)
    assert by_day["2026-10-13"]["ret_fixed"] == pytest.approx(0.08, abs=1e-12)
    assert by_day["2026-10-14"]["qqq_tr"] == pytest.approx(_return(date(2026, 10, 14)), abs=1e-12)
    assert by_day["2026-10-12"]["qqq_tr"] == pytest.approx(_return(date(2026, 10, 12)), abs=1e-12)


def _lag(root: Path, day: date) -> None:
    """The capture of this day stops one trading day short."""

    path = root / fl.PRICES_REL.format(day=day.isoformat())
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    kept = "".join(line for line in lines if not line.startswith(day.isoformat()))
    path.write_text(kept, encoding="utf-8")


def test_rows_are_keyed_by_price_date_and_a_lagging_capture_shows_in_the_offsets(
    daily_root: Path, tmp_path: Path
) -> None:
    lagging, missing = date(2026, 10, 21), date(2026, 10, 19)
    _lag(daily_root, lagging)
    (daily_root / fl.PRICES_REL.format(day=missing.isoformat())).unlink()
    # Appending through the lagging day stops at the last day its capture prices.
    twice = tmp_path / "twice"
    first = fl.append(twice, daily_root, lagging, provenance=PROVENANCE)
    assert first[-1]["date"] == "2026-10-20"
    fl.append(twice, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    once = tmp_path / "once"
    rows = fl.append(once, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    assert (once / fl.LEDGER_FILE).read_bytes() == (twice / fl.LEDGER_FILE).read_bytes()
    by_day = {row["date"]: row for row in rows}
    # 10-19 has no capture; 10-20's capture prices it.
    assert by_day["2026-10-19"]["price_file"] == fl.PRICES_REL.format(day="2026-10-20")
    assert by_day["2026-10-19"]["qqq_tr"] == pytest.approx(_return(missing), abs=1e-12)
    # 10-21's own capture stops at 10-20, so 10-21 is priced from 10-22's capture.
    row = by_day["2026-10-21"]
    assert row["price_file"] == fl.PRICES_REL.format(day="2026-10-22")
    assert row["qqq_tr"] == pytest.approx(_return(lagging), abs=1e-12)
    assert row["notes"] == ["当天的价格捕获文件里没有当天的价格，价格取自 2026-10-22 的文件"]
    assert fl.price_offsets(rows) == {0: len(rows)}
    # A day priced only by a lagging capture records the offset.
    (daily_root / fl.PRICES_REL.format(day="2026-10-20")).unlink()
    other = tmp_path / "other"
    rows = fl.append(other, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    by_day = {row["date"]: row for row in rows}
    row = by_day["2026-10-20"]
    assert row["price_file"] == fl.PRICES_REL.format(day="2026-10-21")
    assert row["price_last_date"] == "2026-10-20"
    assert "价格捕获文件 2026-10-21 里最后的价格日是 2026-10-20" in row["notes"]
    assert fl.price_offsets(rows) == {0: len(rows) - 2, 1: 2}
    assert fl.offsets_line(rows).endswith("（不恒定）")
    assert fl.verify(other) == []


def test_rows_are_compact_sorted_json_lines(daily_root: Path, ledger_dir: Path) -> None:
    rows = fl.append(ledger_dir, daily_root, date(2026, 10, 14), provenance=PROVENANCE)
    data = (ledger_dir / fl.LEDGER_FILE).read_bytes()
    assert data.endswith(b"\n") and b"\r" not in data
    for row, line in zip(rows, data.splitlines(), strict=True):
        expected = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        assert line == expected.encode("utf-8")


# Overlay (requirement test c) --------------------------------------------------------------------


def _bench_exposure(through: date) -> dict[str, float]:
    """The F0001 rule worked by hand: half for each positive sign of the 21- and 252-day return
    at each month end, traded at the next close; between trades the QQQ weight drifts (cash 0)."""

    prices = PRICES[PRICES.index <= pd.Timestamp(through)]
    index = prices.index

    def target(position: int) -> float:
        fast = prices.iloc[position] / prices.iloc[position - 21] - 1.0
        slow = prices.iloc[position] / prices.iloc[position - 252] - 1.0
        return 0.5 * (fast > 0) + 0.5 * (slow > 0)

    month_end = [
        position == len(index) - 1 or index[position].month != index[position + 1].month
        for position in range(len(index))
    ]
    start = index.get_loc(pd.Timestamp(date(2026, 10, 9)))
    opening = max(p for p in range(start) if month_end[p])
    weight = target(opening)
    held: dict[str, float] = {}
    for q in range(start + 1, len(index)):
        held[index[q].date().isoformat()] = weight
        r = prices.iloc[q] / prices.iloc[q - 1] - 1.0
        weight = weight * (1 + r) / (1 + weight * r)
        if month_end[q - 1]:
            weight = target(q - 1)
    return held


def test_overlay_exposure_follows_the_f0001_rule_on_real_prices(
    daily_root: Path, ledger_dir: Path
) -> None:
    rows = fl.append(ledger_dir, daily_root, LAST_DAY, provenance=PROVENANCE)
    expected = _bench_exposure(LAST_DAY)
    for row in rows:
        assert row["exp_overlay"] == pytest.approx(expected[row["date"]], abs=1e-12), row["date"]
        if row["qqq_tr"] is not None:
            assert row["ret_overlay"] == pytest.approx(
                row["exp_overlay"] * row["qqq_tr"], abs=1e-12
            )
    by_day = {row["date"]: row for row in rows}
    # 2026-09-30: 21-day positive, 252-day negative -> half; 2026-10-30: both negative -> none,
    # held from 2026-11-03.
    assert by_day["2026-10-12"]["exp_overlay"] == pytest.approx(0.5, abs=1e-12)
    assert by_day["2026-10-13"]["exp_overlay"] != pytest.approx(0.5, abs=1e-6)
    assert by_day["2026-11-03"]["exp_overlay"] == 0.0


def test_overlay_exposure_matches_the_bench_function_directly(daily_root: Path) -> None:
    prices = fl.read_prices(daily_root, LAST_DAY)
    assert prices is not None
    exposure = fl.overlay_exposure(prices, fl.BACKFILL_START, LAST_DAY)
    expected = _bench_exposure(LAST_DAY)
    assert [stamp.date().isoformat() for stamp in exposure.index] == list(expected)
    np.testing.assert_allclose(exposure.to_numpy(), list(expected.values()), atol=1e-12, rtol=0)


def test_overlay_needs_a_year_of_lookback(tmp_path: Path) -> None:
    short = PRICES[PRICES.index >= pd.Timestamp(date(2026, 3, 2))]
    with pytest.raises(fl.ForwardLedgerError):
        fl.overlay_exposure(short, fl.BACKFILL_START, LAST_DAY)


# Append-only chain (requirement test b) ----------------------------------------------------------


def test_appending_twice_gives_the_same_chain_as_appending_once(
    daily_root: Path, tmp_path: Path
) -> None:
    once, twice = tmp_path / "once", tmp_path / "twice"
    fl.append(once, daily_root, LAST_DAY, provenance=PROVENANCE)
    first = fl.append(twice, daily_root, date(2026, 10, 16), provenance=PROVENANCE)
    second = fl.append(twice, daily_root, LAST_DAY, provenance=PROVENANCE)
    assert first[-1]["date"] == "2026-10-16" and second[0]["date"] == "2026-10-19"
    assert (once / fl.LEDGER_FILE).read_bytes() == (twice / fl.LEDGER_FILE).read_bytes()
    assert fl.append(twice, daily_root, LAST_DAY, provenance=PROVENANCE) == []
    assert fl.verify(twice) == []


def test_a_ledger_with_crlf_line_ends_is_whole_and_appends_on_the_same_chain(
    daily_root: Path, tmp_path: Path
) -> None:
    once, crlf = tmp_path / "once", tmp_path / "crlf"
    fl.append(once, daily_root, LAST_DAY, provenance=PROVENANCE)
    fl.append(crlf, daily_root, date(2026, 10, 16), provenance=PROVENANCE)
    path = crlf / fl.LEDGER_FILE
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    assert fl.verify(crlf) == []
    fl.append(crlf, daily_root, LAST_DAY, provenance=PROVENANCE)
    assert fl.verify(crlf) == []
    assert path.read_bytes().replace(b"\r\n", b"\n") == (once / fl.LEDGER_FILE).read_bytes()


def test_check_finds_an_edited_old_row_a_missing_row_and_a_bad_return(
    daily_root: Path, ledger_dir: Path
) -> None:
    fl.append(ledger_dir, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    path = ledger_dir / fl.LEDGER_FILE
    original = path.read_bytes()
    assert fl.verify(ledger_dir) == []

    lines = original.splitlines()
    row = json.loads(lines[2])
    row["exp_fixed"] = 0.8
    row["daily_label"] = "改过"
    lines[2] = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(b"\n".join(lines) + b"\n")
    assert any(
        "line 4" in problem and "prev_sha256" in problem for problem in fl.verify(ledger_dir)
    )

    lines = original.splitlines()
    path.write_bytes(b"\n".join(lines[:3] + lines[4:]) + b"\n")
    assert fl.verify(ledger_dir)

    lines = original.splitlines()
    row = json.loads(lines[-1])
    row["ret_fixed"] = row["ret_fixed"] + 0.001
    lines[-1] = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(b"\n".join(lines) + b"\n")
    assert any("ret_fixed" in problem for problem in fl.verify(ledger_dir))
    with pytest.raises(fl.ForwardLedgerError):
        fl.append(ledger_dir, daily_root, LAST_DAY, provenance=PROVENANCE)

    lines = original.splitlines()
    row = json.loads(lines[-1])
    row["seq"] = 99
    lines[-1] = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(b"\n".join(lines) + b"\n")
    assert any("seq" in problem for problem in fl.verify(ledger_dir))


def test_check_finds_a_skipped_trading_day_and_a_wrong_backfill_flag(
    daily_root: Path, ledger_dir: Path
) -> None:
    rows = fl.build_rows(
        daily_root,
        [date(2026, 10, 12), date(2026, 10, 14)],
        first_seq=1,
        prev_sha256=None,
        provenance=PROVENANCE,
    )
    ledger_dir.mkdir()
    text = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    )
    (ledger_dir / fl.LEDGER_FILE).write_text(text, encoding="utf-8")
    assert any("2026-10-13" in problem for problem in fl.verify(ledger_dir))

    rows = fl.build_rows(
        daily_root, [date(2026, 10, 12)], first_seq=1, prev_sha256=None, provenance=PROVENANCE
    )
    rows[0]["backfill"] = False
    (ledger_dir / fl.LEDGER_FILE).write_text(
        json.dumps(rows[0], ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    assert any("backfill" in problem for problem in fl.verify(ledger_dir))


def test_check_finds_an_exposure_outside_zero_to_one(daily_root: Path, ledger_dir: Path) -> None:
    rows = fl.build_rows(
        daily_root, [date(2026, 10, 12)], first_seq=1, prev_sha256=None, provenance=PROVENANCE
    )
    rows[0]["exp_daily_held"] = 1.5
    rows[0]["ret_daily"] = 1.5 * rows[0]["qqq_tr"]
    ledger_dir.mkdir()
    (ledger_dir / fl.LEDGER_FILE).write_text(
        json.dumps(rows[0], ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    assert any("exp_daily_held" in problem for problem in fl.verify(ledger_dir))
    rows[0]["exp_daily_held"] = 0.5
    rows[0]["ret_daily"] = 0.5 * rows[0]["qqq_tr"]
    rows[0]["exp_overlay"] = 1.2
    rows[0]["ret_overlay"] = 1.2 * rows[0]["qqq_tr"]
    (ledger_dir / fl.LEDGER_FILE).write_text(
        json.dumps(rows[0], ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    assert [problem for problem in fl.verify(ledger_dir) if "exp_overlay" in problem]


def test_check_finds_a_price_file_older_than_its_row_or_not_a_capture(
    daily_root: Path, ledger_dir: Path
) -> None:
    rows = fl.build_rows(
        daily_root, [date(2026, 10, 12)], first_seq=1, prev_sha256=None, provenance=PROVENANCE
    )
    ledger_dir.mkdir()
    for price_file in (fl.PRICES_REL.format(day="2026-10-09"), "prices_daily.csv"):
        rows[0]["price_file"] = price_file
        (ledger_dir / fl.LEDGER_FILE).write_text(
            json.dumps(rows[0], ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        assert [problem for problem in fl.verify(ledger_dir) if "price_file" in problem]
    assert fl.capture_day(fl.PRICES_REL.format(day="2026-10-09")) == date(2026, 10, 9)
    assert fl.capture_day("data/raw/daily_input_capture/2026-10-09/x/prices_daily.csv") is None


# Refusals and read-only -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "provenance",
    [
        {"commit": COMMIT, "branch": "main", "code_modified": True},
        {"commit": COMMIT, "branch": "main", "code_modified": "unavailable"},
        {"commit": "unavailable", "branch": "main", "code_modified": False},
        {"commit": COMMIT[:12], "branch": "main", "code_modified": False},
    ],
)
def test_append_refuses_modified_or_unknown_code(
    daily_root: Path, ledger_dir: Path, provenance: dict[str, object]
) -> None:
    with pytest.raises(fl.ForwardLedgerError):
        fl.append(ledger_dir, daily_root, date(2026, 10, 23), provenance=provenance)
    assert not (ledger_dir / fl.LEDGER_FILE).exists()


def test_append_refuses_when_the_last_day_has_no_price_capture(
    daily_root: Path, ledger_dir: Path
) -> None:
    with pytest.raises(fl.ForwardLedgerError):
        fl.append(ledger_dir, daily_root, NO_CAPTURE, provenance=PROVENANCE)
    assert not (ledger_dir / fl.LEDGER_FILE).exists()


def test_append_and_check_never_write_under_the_daily_root(
    daily_root: Path, ledger_dir: Path
) -> None:
    def tree() -> dict[str, bytes]:
        return {
            path.relative_to(daily_root).as_posix(): path.read_bytes()
            for path in sorted(daily_root.rglob("*"))
            if path.is_file()
        }

    before = tree()
    fl.append(ledger_dir, daily_root, LAST_DAY, provenance=PROVENANCE)
    fl.write_weekly(ledger_dir)
    assert fl.check_day(ledger_dir, daily_root, date(2026, 10, 13)) == []
    assert tree() == before
    assert sorted(path.name for path in ledger_dir.iterdir()) == [fl.LEDGER_FILE, fl.WEEKLY_FILE]


def test_check_day_compares_a_row_with_its_snapshot_and_price_capture(
    daily_root: Path, ledger_dir: Path
) -> None:
    fl.append(ledger_dir, daily_root, date(2026, 10, 23), provenance=PROVENANCE)
    assert fl.check_day(ledger_dir, daily_root, NO_SNAPSHOT) == []
    assert fl.check_day(ledger_dir, daily_root, NO_CAPTURE) == []
    low, high, _, model_low, model_high = _band(date(2026, 10, 13))
    _write_snapshot(daily_root, date(2026, 10, 13), (low, high, "改名", model_low, model_high))
    problems = fl.check_day(ledger_dir, daily_root, date(2026, 10, 13))
    assert problems and all("daily_label" in problem for problem in problems)
    _write_snapshot(daily_root, date(2026, 10, 13), (0.0, 0.2, "空仓", 0.0, 0.2))
    problems = fl.check_day(ledger_dir, daily_root, date(2026, 10, 13))
    assert any("exp_daily" in problem for problem in problems)
    path = daily_root / fl.PRICES_REL.format(day="2026-10-14")
    path.write_text(path.read_text(encoding="utf-8").replace(",QQQ,", ",QQQX,"), encoding="utf-8")
    problems = fl.check_day(ledger_dir, daily_root, date(2026, 10, 14))
    assert len(problems) == 2
    assert "qqq_tr" in problems[0] and "price_last_date" in problems[1]
    assert fl.check_day(ledger_dir, daily_root, date(2026, 10, 26))


def test_the_module_imports_no_production_daily_or_broker_code() -> None:
    allowed = (
        "ai_trading_system.config",
        "ai_trading_system.core.provenance",
        "ai_trading_system.paths",
        "ai_trading_system.research_bench",
        "ai_trading_system.research_probes",
        "ai_trading_system.trading_calendar",
    )
    tree = ast.parse(Path(fl.__file__).read_text(encoding="utf-8"))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    ours = [name for name in names if name.split(".")[0] == "ai_trading_system"]
    assert ours
    for name in ours:
        assert name.startswith(allowed), name


# Summary -----------------------------------------------------------------------------------------


def _row(day: str, held: float | None, qqq: float | None, overlay: float | None) -> dict:
    return {
        "date": day,
        "qqq_tr": qqq,
        "exp_daily_held": held,
        "exp_fixed": 0.8,
        "exp_overlay": overlay,
        "ret_daily": None if held is None or qqq is None else held * qqq,
        "ret_fixed": None if qqq is None else 0.8 * qqq,
        "ret_overlay": None if overlay is None or qqq is None else overlay * qqq,
    }


def test_summarize_compounds_present_returns_and_measures_drawdown() -> None:
    rows = [
        _row("2026-10-20", 0.5, 0.10, 1.0),
        _row("2026-10-21", 0.5, None, 1.0),
        _row("2026-10-22", None, -0.20, 0.0),
        _row("2026-10-23", 1.0, 0.05, 0.5),
    ]
    summary = fl.summarize(rows, date(2026, 10, 12))
    daily = summary["daily"]
    assert (daily["days"], daily["missing"]) == (4, 2)
    assert daily["cumulative"] == pytest.approx(1.05 * 1.05 - 1, abs=1e-15)
    assert daily["max_drawdown"] == 0.0
    assert daily["mean_exposure"] == pytest.approx(2.0 / 3.0, abs=1e-15)
    fixed = summary["fixed"]
    assert fixed["missing"] == 1
    assert fixed["cumulative"] == pytest.approx(1.08 * 0.84 * 1.04 - 1, abs=1e-15)
    assert fixed["max_drawdown"] == pytest.approx(-0.16, abs=1e-15)
    assert summary["overlay"]["cumulative"] == pytest.approx(1.10 * 1.0 * 1.025 - 1, abs=1e-15)
    same = summary["fixed_at_daily_mean"]
    assert same["mean_exposure"] == pytest.approx(0.75, abs=1e-15)
    assert same["days"] == 2
    assert same["cumulative"] == pytest.approx(1.075 * 1.0375 - 1, abs=1e-15)
    later = fl.summarize(rows, date(2026, 10, 22))
    assert later["fixed"]["days"] == 2
    assert later["fixed"]["cumulative"] == pytest.approx(0.84 * 1.04 - 1, abs=1e-15)
    # The first day's loss counts from the starting value.
    assert later["fixed"]["max_drawdown"] == pytest.approx(-0.16, abs=1e-15)


def test_weekly_file_shows_both_starts_and_each_week(daily_root: Path, ledger_dir: Path) -> None:
    fl.append(ledger_dir, daily_root, LAST_DAY, provenance=PROVENANCE)
    path = fl.write_weekly(ledger_dir)
    assert path == ledger_dir / fl.WEEKLY_FILE
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# 前瞻记录台周汇总\n")
    assert "## 自 2026-10-12（含回填）" in text
    assert "## 自 2026-10-21（前瞻）" in text
    assert "价格捕获文件的文件夹日期和最后的价格日：相差 0 个交易日 20 行（恒定）" in text
    assert "## 按周" in text
    rows = fl.read_ledger(ledger_dir)
    cumulative = fl.summarize(rows, date(2026, 10, 12))["fixed"]["cumulative"]
    assert f"{cumulative * 100:+.2f}%" in text
    for friday in ("2026-10-16", "2026-10-23", "2026-10-30", "2026-11-06"):
        assert f"| {friday} |" in text
    assert text.rstrip().endswith("production_effect=none；broker_action=none。")
    assert b"\r" not in path.read_bytes()
    empty = fl.render_weekly([])
    assert "账本还没有行。" in empty
    assert empty.endswith("production_effect=none；broker_action=none。\n")


# Command line ------------------------------------------------------------------------------------


def test_command_line_appends_writes_the_summary_and_checks(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fl, "git_provenance", lambda anchor: dict(PROVENANCE))
    runner = CliRunner()
    common = ["--ledger-dir", str(ledger_dir)]
    result = runner.invoke(
        fl.app,
        ["append", "--through", "2026-10-23", "--daily-root", str(daily_root), *common],
    )
    assert result.exit_code == 0, result.output
    assert "forward_ledger: 追加 10 行（2026-10-12..2026-10-23），共 10 行" in result.output
    assert "forward_ledger: 2026-10-12（回填）" in result.output
    result = runner.invoke(
        fl.app,
        ["append", "--through", "2026-10-23", "--daily-root", str(daily_root), *common],
    )
    assert result.exit_code == 0, result.output
    assert "forward_ledger: 没有新的交易日，共 10 行" in result.output
    result = runner.invoke(fl.app, ["weekly", *common])
    assert result.exit_code == 0, result.output
    assert (ledger_dir / fl.WEEKLY_FILE).exists()
    result = runner.invoke(
        fl.app, ["check", *common, "--day", "2026-10-13", "--daily-root", str(daily_root)]
    )
    assert result.exit_code == 0, result.output
    assert "forward_ledger: 账本完好，10 行（2026-10-12..2026-10-23）" in result.output


def test_command_line_refuses_modified_code(
    daily_root: Path, ledger_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fl, "git_provenance", lambda anchor: {**PROVENANCE, "code_modified": True})
    result = CliRunner().invoke(
        fl.app,
        [
            "append",
            "--through",
            "2026-10-23",
            "--daily-root",
            str(daily_root),
            "--ledger-dir",
            str(ledger_dir),
        ],
    )
    assert result.exit_code == 1
    assert "forward_ledger: 拒绝追加：" in result.output
    assert not (ledger_dir / fl.LEDGER_FILE).exists()
