"""Append a forward record of the daily band, fixed 0.8 and F0001 fast_slow.

Reads only the named decision snapshots and QQQ adjusted-close capture files under
an explicit daily root, plus F0001's preregistration. Reuses the research bench for
signals, execution timing and drift; cash earns zero and costs are zero. Records
price dates, missing inputs, provenance and an append-only hash chain; writes only
the ledger and its descriptive weekly summary. Does not run research trials, judge
eligibility, alter daily outputs or authorize production or broker actions.
weekly 给了日报状态根目录时只读账本最后一行的价格捕获文件，印 QQQ 离运行最高
（只印现在的状态，记录起点以前的价格只用来定运行最高和划段）和现金按 SGOV 的一组，
都只印，不改账本的行和判的口径。
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any, TypeGuard

import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import ASSETS
from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.simulate import simulate
from ai_trading_system.research_probes import backfill_check
from ai_trading_system.trading_calendar import is_us_equity_trading_day

RUN_ID = "TRADING-2565-45"
SCHEMA = "forward_ledger.v1"
DAILY_POLICY = "owner_decision:TRADING-2565:2026-10-22:daily_policy"
READ_DAILY_OUTPUTS = "owner_decision:TRADING-2565:2026-10-22:read_daily_outputs"
# Owner-scoped record boundaries and fixed comparator, not investment acceptance rules.
BACKFILL_START = date(2026, 10, 12)
FORWARD_START = date(2026, 10, 21)
HELD_FROM = date(2026, 10, 8)
FIXED_EXPOSURE = 0.8
OVERLAY_ID = "F0001"
OVERLAY_CONFIG = backfill_check.CONFIGS[0]
TICKER = "QQQ"
# 只印的「现金按 SGOV」一组，依据 owner_decision:TRADING-2565:2026-10-23:research_loop_p6。
CASH_TICKER = "SGOV"
# 周汇总的显示规则：一年以内不印年化，不是门槛。
ANNUAL_DAYS = 252
LABELS = {
    "daily": "日报模型（区间中点，t+2 持有）",
    "fixed": "固定敞口 0.8",
    "overlay": "F0001 fast_slow 覆盖",
    "fixed_at_daily_mean": "同平均敞口的固定（日报模型的对照）",
    "fixed_at_overlay_mean": "同平均敞口的固定（F0001 的对照）",
}
NO_CAPTURE = "没有读价格文件（`weekly` 没给 `--daily-root`）。"
REAL_RETURN = "研究的实际收益（F0008 第 2 行的读数，描述，不判）："
SEGMENT_RULE = (
    "划段规则同前瞻记录（`research_bench.segments`：月末水平跌破运行最高的 "
    f"{1 - segments.DROP:.0%} 开段，每个谷底的下个月重新起算运行最高）。"
    "记录起点以前的价格只在这一节里用来定运行最高和划段，不印，也不算别的指标；"
    "以前的段（含 2022）不印；不是一致性窗口或验证窗口的读取，不计数。"
)
LEDGER_FILE = "ledger.jsonl"
WEEKLY_FILE = "weekly.md"
DEFAULT_LEDGER_DIR = PROJECT_ROOT / "research" / "_forward"
RESEARCH_ROOT = PROJECT_ROOT / "research"
SNAPSHOT_REL = "data/processed/decision_snapshots/decision_snapshot_{day}.json"
PRICES_REL = "data/raw/daily_input_capture/{day}/market_macro/prices_daily.csv"
BAND_KEY = "final_risk_asset_ai_band"
MODEL_BAND_KEY = "model_risk_asset_ai_band"
# Requirement's floating-point identity tolerance; it never judges a strategy's performance.
TOLERANCE = 1e-12
LINES = ("daily", "fixed", "overlay")
ROW_KEYS = frozenset(
    {
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
)

app = typer.Typer(help="前瞻记录台：只读日报输出，只追加记录，描述不判。")


class ForwardLedgerError(ValueError):
    """The ledger or a named input cannot be used; do not write anything."""


@dataclass(frozen=True)
class Band:
    min_position: float
    max_position: float
    label: str
    model_mid: float
    file: str

    @property
    def mid(self) -> float:
        return (self.min_position + self.max_position) / 2.0


def trading_days(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _next_day(day: date) -> date:
    day += timedelta(days=1)
    while not is_us_equity_trading_day(day):
        day += timedelta(days=1)
    return day


def _folder_day(day: date) -> date:
    while not is_us_equity_trading_day(day):
        day -= timedelta(days=1)
    return day


def _number(value: Any) -> TypeGuard[float | int]:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _position(value: Any) -> TypeGuard[float | int]:
    return _number(value) and 0 <= value <= 1


def _equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return _number(left) and _number(right) and abs(left - right) <= TOLERANCE


def _product(exposure: Any, daily_return: Any) -> float | None:
    if exposure is None or daily_return is None:
        return None
    return float(exposure * daily_return)


def read_band(daily_root: Path, day: date) -> Band | None:
    relative = SNAPSHOT_REL.format(day=day.isoformat())
    path = daily_root / relative
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError) as exc:
        raise ForwardLedgerError(f"{relative}: {exc}") from exc
    try:
        snapshot = json.loads(text)
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        if not isinstance(positions, dict):
            raise ValueError("positions is not a dict")
        bounds = []
        for key in (BAND_KEY, MODEL_BAND_KEY):
            band = positions.get(key)
            if not isinstance(band, dict):
                raise ValueError(f"{key} is not a dict")
            low, high = band.get("min_position"), band.get("max_position")
            if not _position(low) or not _position(high) or low > high:
                raise ValueError(f"{key}: invalid min_position or max_position")
            bounds.append((float(low), float(high)))
        label = positions[BAND_KEY].get("label")
        if not isinstance(label, str):
            raise ValueError(f"{BAND_KEY}: label is not a string")
        low, high = bounds[0]
        model_low, model_high = bounds[1]
        return Band(low, high, label, (model_low + model_high) / 2.0, relative)
    except (ValueError, OverflowError) as exc:
        raise ForwardLedgerError(f"{relative}: {exc}") from exc


def read_capture(daily_root: Path, relative: str, ticker: str = TICKER) -> pd.Series | None:
    path = daily_root / relative
    try:
        frame = pd.read_csv(path, dtype={"ticker": str})
        if not {"date", "ticker", "adj_close"}.issubset(frame.columns):
            raise ValueError("missing date, ticker or adj_close")
        frame = frame.loc[frame["ticker"] == ticker, ["date", "adj_close"]].copy()
        frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
        frame = frame.dropna(subset=["adj_close"])
        index = pd.DatetimeIndex(pd.to_datetime(frame["date"], errors="raise"))
        if any(index.isna()) or not index.equals(index.normalize()):
            raise ValueError("invalid price date")
        values = frame["adj_close"].to_numpy(dtype=float)
        if index.has_duplicates:
            raise ValueError(f"duplicate {ticker} date")
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError(f"{ticker} adj_close must be finite and positive")
        return pd.Series(values, index=index, dtype=float).sort_index()
    except FileNotFoundError:
        return None
    except (OSError, ValueError, UnicodeError) as exc:
        raise ForwardLedgerError(f"{relative}: {exc}") from exc


def read_prices(daily_root: Path, day: date) -> pd.Series | None:
    return read_capture(daily_root, PRICES_REL.format(day=day.isoformat()))


@dataclass(frozen=True)
class Capture:
    file: str
    qqq: pd.Series
    cash: pd.Series


def read_weekly_capture(daily_root: Path, rows: Sequence[Mapping[str, Any]]) -> Capture | None:
    if not rows:
        return None
    relative = rows[-1]["price_file"]
    if capture_day(relative) is None:
        raise ForwardLedgerError(f"{relative}: not a price capture file")
    qqq = read_capture(daily_root, relative)
    if qqq is None:
        raise ForwardLedgerError(f"{relative}: no such price capture file")
    if qqq.empty:
        raise ForwardLedgerError(f"{relative}: no {TICKER} prices")
    cash = read_capture(daily_root, relative, CASH_TICKER)
    return Capture(relative, qqq, pd.Series(dtype=float) if cash is None else cash)


def cash_returns(cash: pd.Series, rows: Sequence[Mapping[str, Any]]) -> dict[str, float | None]:
    return {row["date"]: day_return(cash, date.fromisoformat(row["date"])) for row in rows}


def day_return(prices: pd.Series, day: date) -> float | None:
    stamp = pd.Timestamp(day)
    if stamp not in prices.index:
        return None
    position = int(prices.index.get_indexer(pd.DatetimeIndex([stamp]))[0])
    if position == 0:
        return None
    return float(prices.iloc[position] / prices.iloc[position - 1] - 1.0)


def capture_day(file: Any) -> date | None:
    if not isinstance(file, str):
        return None
    pattern = re.escape(PRICES_REL).replace(re.escape("{day}"), r"(\d{4}-\d{2}-\d{2})")
    match = re.fullmatch(pattern, file)
    if match is None:
        return None
    try:
        return date.fromisoformat(match[1])
    except ValueError:
        return None


def overlay_exposure(
    prices: pd.Series, first_day: date, last_day: date, research_root: Path = RESEARCH_ROOT
) -> pd.Series:
    try:
        entry = backfill_check.source_entry(research_root, OVERLAY_CONFIG)
        returns = prices.pct_change()
        targets = strategy_targets(
            signal_exposure, pd.DataFrame({"ndx_return": returns}), entry.params
        )
        history = prices.index[prices.index < pd.Timestamp(first_day)]
        if len(history) == 0:
            raise ValueError(f"no price before {first_day}")
        assets = pd.DataFrame(
            {asset: returns if asset == "qqq" else 0.0 for asset in ASSETS}, index=prices.index
        )
        return simulate(
            assets,
            targets,
            start=pd.Timestamp(history[-1]).date(),
            end=last_day,
            rebalance=entry.rebalance.to_rebalance(),
            cost_bps=0.0,
        ).exposure
    except (OSError, ValueError) as exc:
        raise ForwardLedgerError(f"overlay F0001: {exc}") from exc


def _row_bytes(row: Mapping[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _last_date(prices: pd.Series | None) -> str | None:
    return (
        None
        if prices is None or prices.empty
        else pd.Timestamp(prices.index[-1]).date().isoformat()
    )


def build_rows(
    daily_root: Path,
    days: Sequence[date],
    *,
    first_seq: int,
    prev_sha256: str | None,
    provenance: Mapping[str, Any],
    research_root: Path = RESEARCH_ROOT,
) -> list[dict[str, Any]]:
    if not days:
        return []
    folder = days[-1]
    cache: dict[date, pd.Series | None] = {}

    def prices_for(day: date) -> pd.Series | None:
        if day not in cache:
            cache[day] = read_prices(daily_root, day)
        return cache[day]

    latest = prices_for(folder)
    relative = PRICES_REL.format(day=folder.isoformat())
    if latest is None or latest.empty:
        raise ForwardLedgerError(f"{relative}: no QQQ prices for {folder}")
    last_price_day = pd.Timestamp(latest.index[-1]).date()
    days = [day for day in days if day <= last_price_day]
    if not days:
        return []
    overlay = overlay_exposure(latest, BACKFILL_START, days[-1], research_root)
    band_days = trading_days(HELD_FROM, days[-1])
    bands = {day: read_band(daily_root, day) for day in band_days}
    rows: list[dict[str, Any]] = []
    for day in days:
        source_day, source_prices = folder, latest
        for candidate in trading_days(day, folder):
            candidate_prices = prices_for(candidate)
            if candidate_prices is not None and day_return(candidate_prices, day) is not None:
                source_day, source_prices = candidate, candidate_prices
                break
        qqq_return = day_return(source_prices, day)
        last_date = _last_date(source_prices)
        band = bands.get(day)
        held = None
        # The bench convention: a t close decision trades at t+1 close, held at t+2.
        position = band_days.index(day)
        for earlier in reversed(band_days[: max(0, position - 1)]):
            earlier_band = bands[earlier]
            if earlier_band is not None:
                held = earlier_band.mid
                break
        stamp = pd.Timestamp(day)
        exposure = float(overlay.loc[stamp]) if stamp in overlay.index else None
        notes = []
        if source_day != day:
            reason = (
                "没有当天的价格捕获文件"
                if prices_for(day) is None
                else "当天的价格捕获文件里没有当天的价格"
            )
            notes.append(f"{reason}，价格取自 {source_day} 的文件")
        if last_date != source_day.isoformat():
            notes.append(f"价格捕获文件 {source_day} 里最后的价格日是 {last_date}")
        if qqq_return is None:
            notes.append(f"价格捕获文件里没有 QQQ {day} 的收益")
        if band is None:
            notes.append("没有当天的决策快照")
        if held is None:
            notes.append("没有可持有的日报区间")
        if exposure is None:
            notes.append("覆盖 F0001 当天没有敞口")
        row = {
            "schema_version": SCHEMA,
            "seq": first_seq + len(rows),
            "date": day.isoformat(),
            "backfill": day < FORWARD_START,
            "qqq_tr": qqq_return,
            "price_file": PRICES_REL.format(day=source_day.isoformat()),
            "price_last_date": last_date,
            "exp_daily": None if band is None else band.mid,
            "daily_min": None if band is None else band.min_position,
            "daily_max": None if band is None else band.max_position,
            "daily_label": None if band is None else band.label,
            "daily_model_mid": None if band is None else band.model_mid,
            "snapshot_file": None if band is None else band.file,
            "exp_daily_held": held,
            "exp_fixed": FIXED_EXPOSURE,
            "overlay": OVERLAY_ID,
            "exp_overlay": exposure,
            "ret_daily": _product(held, qqq_return),
            "ret_fixed": _product(FIXED_EXPOSURE, qqq_return),
            "ret_overlay": _product(exposure, qqq_return),
            "notes": notes,
            "code_commit": provenance.get("commit"),
            "code_modified": provenance.get("code_modified"),
            "prev_sha256": prev_sha256,
        }
        rows.append(row)
        prev_sha256 = chain_digest(_row_bytes(row))
    return rows


def _ledger_lines(ledger_dir: Path) -> list[bytes]:
    path = ledger_dir / LEDGER_FILE
    try:
        return [line for line in path.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ForwardLedgerError(f"{path}: {exc}") from exc


def _inline_problems(row: Mapping[str, Any], day: date) -> list[str]:
    problems = []
    for field in (
        "exp_daily",
        "daily_min",
        "daily_max",
        "daily_model_mid",
        "exp_daily_held",
        "exp_overlay",
    ):
        if row[field] is not None and not _position(row[field]):
            problems.append(f"{field} must be in [0, 1] or None")
    if not _number(row["exp_fixed"]) or row["exp_fixed"] != FIXED_EXPOSURE:
        problems.append("exp_fixed is not 0.8")
    if row["overlay"] != OVERLAY_ID:
        problems.append("overlay is not F0001")
    low, high = row["daily_min"], row["daily_max"]
    if low is None and high is None:
        if row["exp_daily"] is not None:
            problems.append("exp_daily must be None without a band")
    elif not _position(low) or not _position(high) or low > high:
        problems.append("daily_min/daily_max is not a valid band")
    elif not _equal(row["exp_daily"], (low + high) / 2.0):
        problems.append("exp_daily is not the band midpoint")
    source_day = capture_day(row["price_file"])
    if source_day is None or source_day < day:
        problems.append("price_file is not a capture on or after the row date")
    qqq = row["qqq_tr"]
    if qqq is not None and not _number(qqq):
        problems.append("qqq_tr is not a finite number or None")
    for line in LINES:
        exposure = row["exp_daily_held" if line == "daily" else f"exp_{line}"]
        if (qqq is not None and not _number(qqq)) or (
            exposure is not None and not _number(exposure)
        ):
            problems.append(f"ret_{line} cannot equal exposure * qqq_tr")
        elif not _equal(row[f"ret_{line}"], _product(exposure, qqq)):
            problems.append(f"ret_{line} does not equal exposure * qqq_tr")
    return problems


def verify(ledger_dir: Path) -> list[str]:
    problems = []
    previous = None
    expected = BACKFILL_START
    for position, raw in enumerate(_ledger_lines(ledger_dir), 1):
        prefix = f"{LEDGER_FILE} line {position}: "
        try:
            row = json.loads(raw)
        except (ValueError, UnicodeError):
            problems.append(prefix + "not JSON")
        else:
            if not isinstance(row, dict) or row.keys() != ROW_KEYS:
                problems.append(prefix + "keys do not match ROW_KEYS")
            elif row["schema_version"] != SCHEMA:
                problems.append(prefix + "schema_version does not match")
            elif type(row["seq"]) is not int or row["seq"] != position:
                problems.append(prefix + f"seq is not {position}")
            elif row["prev_sha256"] != previous:
                problems.append(prefix + "prev_sha256 does not match")
            elif row["date"] != expected.isoformat():
                problems.append(prefix + f"date is not expected trading day {expected}")
            elif row["backfill"] is not (expected < FORWARD_START):
                problems.append(prefix + "backfill does not match date")
            else:
                problems.extend(prefix + problem for problem in _inline_problems(row, expected))
        previous = chain_digest(raw)
        expected = _next_day(expected)
    return problems


def read_ledger(ledger_dir: Path) -> list[dict[str, Any]]:
    problems = verify(ledger_dir)
    if problems:
        raise ForwardLedgerError(f"the ledger is broken: {problems[0]}")
    return [json.loads(raw) for raw in _ledger_lines(ledger_dir)]


def append(
    ledger_dir: Path,
    daily_root: Path,
    through: date,
    *,
    provenance: Mapping[str, Any],
    research_root: Path = RESEARCH_ROOT,
) -> list[dict[str, Any]]:
    if provenance.get("code_modified") is not False:
        raise ForwardLedgerError(f"{LEDGER_FILE}: code_modified is not False")
    commit = provenance.get("commit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise ForwardLedgerError(f"{LEDGER_FILE}: commit is not a 40-character string")
    existing = read_ledger(ledger_dir)
    start = BACKFILL_START if not existing else _next_day(date.fromisoformat(existing[-1]["date"]))
    raw = _ledger_lines(ledger_dir)
    rows = build_rows(
        daily_root,
        trading_days(start, through),
        first_seq=len(existing) + 1,
        prev_sha256=chain_digest(raw[-1]) if raw else None,
        provenance=provenance,
        research_root=research_root,
    )
    if rows:
        path = ledger_dir / LEDGER_FILE
        try:
            # Preserve existing bytes, including CRLF. A final unterminated row needs a separator.
            needs_separator = path.exists() and path.stat().st_size > 0
            if needs_separator:
                with path.open("rb") as source:
                    source.seek(-1, 2)
                    needs_separator = source.read(1) != b"\n"
            ledger_dir.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as sink:
                sink.write(
                    (b"\n" if needs_separator else b"")
                    + b"".join(_row_bytes(row) + b"\n" for row in rows)
                )
        except OSError as exc:
            raise ForwardLedgerError(f"{path}: {exc}") from exc
    return rows


def check_day(ledger_dir: Path, daily_root: Path, day: date) -> list[str]:
    row = next((row for row in read_ledger(ledger_dir) if row["date"] == day.isoformat()), None)
    if row is None:
        return [f"the ledger has no row for {day}"]
    band = read_band(daily_root, day)
    source_day = capture_day(row["price_file"])
    if source_day is None:
        raise ForwardLedgerError(f"{LEDGER_FILE} {day}: invalid price_file")
    prices = read_prices(daily_root, source_day)
    values = {
        "exp_daily": None if band is None else band.mid,
        "daily_min": None if band is None else band.min_position,
        "daily_max": None if band is None else band.max_position,
        "daily_model_mid": None if band is None else band.model_mid,
        "daily_label": None if band is None else band.label,
        "qqq_tr": None if prices is None else day_return(prices, day),
        "price_last_date": _last_date(prices),
    }
    problems = []
    for key, value in values.items():
        equal = (
            row[key] == value
            if key in ("daily_label", "price_last_date")
            else _equal(row[key], value)
        )
        if not equal:
            source = (
                row["price_file"]
                if key in ("qqq_tr", "price_last_date")
                else SNAPSHOT_REL.format(day=day.isoformat())
            )
            problems.append(f"{LEDGER_FILE} {day}: {key} does not match {source}")
    return problems


def price_offsets(rows: Sequence[Mapping[str, Any]]) -> dict[int, int]:
    offsets: Counter[int] = Counter()
    for row in rows:
        folder = capture_day(row.get("price_file"))
        try:
            last = date.fromisoformat(row["price_last_date"])
        except (ValueError, TypeError, KeyError):
            continue
        if folder is None:
            continue
        offset = len(trading_days(min(last, folder), max(last, folder))) - 1
        offsets[offset if last <= folder else -offset] += 1
    return dict(sorted(offsets.items()))


def offsets_line(rows: Sequence[Mapping[str, Any]]) -> str:
    prefix = "价格捕获文件的文件夹日期和最后的价格日："
    offsets = price_offsets(rows)
    if not offsets:
        return prefix + "没有可比的行"
    values = "，".join(f"相差 {offset} 个交易日 {count} 行" for offset, count in offsets.items())
    return prefix + values + ("（恒定）" if len(offsets) == 1 else "（不恒定）")


def _mean(values: Sequence[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) / len(present) if present else None


def _summary(returns: Sequence[float | None], exposures: Sequence[float | None]) -> dict[str, Any]:
    wealth, peak, drawdown = 1.0, 1.0, 0.0
    for value in returns:
        if value is not None:
            wealth *= 1.0 + value
            peak = max(peak, wealth)
            drawdown = min(drawdown, wealth / peak - 1.0)
    return {
        "days": len(returns),
        "missing": sum(value is None for value in returns),
        "cumulative": wealth - 1.0,
        "max_drawdown": drawdown,
        "mean_exposure": _mean(exposures),
    }


def _exposure(row: Mapping[str, Any], line: str) -> float | None:
    value = row["exp_daily_held" if line == "daily" else f"exp_{line}"]
    return None if value is None else float(value)


def _cash_return(
    row: Mapping[str, Any], exposure: float | None, cash: Mapping[str, float | None] | None
) -> float | None:
    qqq = row["qqq_tr"]
    if cash is None:
        return _product(exposure, qqq)
    value = cash.get(row["date"])
    if exposure is None or qqq is None or value is None:
        return None
    return float(exposure * qqq + (1 - exposure) * value)


def _line_return(
    row: Mapping[str, Any], line: str, cash: Mapping[str, float | None] | None
) -> float | None:
    if cash is not None:
        return _cash_return(row, _exposure(row, line), cash)
    value = row[f"ret_{line}"]
    return None if value is None else float(value)


def _matched(
    rows: Sequence[Mapping[str, Any]], line: str
) -> tuple[list[Mapping[str, Any]], float | None]:
    matched = [row for row in rows if row[f"ret_{line}"] is not None]
    return matched, _mean([_exposure(row, line) for row in matched])


def summarize(
    rows: Sequence[Mapping[str, Any]],
    since: date,
    cash: Mapping[str, float | None] | None = None,
) -> dict[str, dict[str, Any]]:
    selected = [row for row in rows if row["date"] >= since.isoformat()]
    result = {
        line: _summary(
            [_line_return(row, line, cash) for row in selected],
            [_exposure(row, line) for row in selected],
        )
        for line in LINES
    }
    for line in ("daily", "overlay"):
        matched, mean = _matched(selected, line)
        result[f"fixed_at_{line}_mean"] = _summary(
            [_cash_return(row, mean, cash) for row in matched], [mean] * len(matched)
        )
    return result


def overlay_difference(
    rows: Sequence[Mapping[str, Any]],
    since: date,
    cash: Mapping[str, float | None] | None = None,
) -> dict[str, Any]:
    selected = [row for row in rows if row["date"] >= since.isoformat()]
    matched, mean = _matched(selected, "overlay")

    def logs(candidates: Sequence[Mapping[str, Any]], *, fixed: bool) -> tuple[int, float, float]:
        pairs = [
            (
                _line_return(row, "overlay", cash),
                _line_return(row, "fixed", cash) if fixed else _cash_return(row, mean, cash),
            )
            for row in candidates
        ]
        present = [(own, other) for own, other in pairs if own is not None and other is not None]
        return (
            len(present),
            sum(math.log1p(own) for own, _ in present),
            sum(math.log1p(other) for _, other in present),
        )

    days, own, other = logs(matched, fixed=False)
    fixed_days, fixed_own, fixed_other = logs(selected, fixed=True)
    annual = None
    if days >= ANNUAL_DAYS:
        first = date.fromisoformat(selected[0]["date"])
        prior = _folder_day(first - timedelta(days=1))
        years = (date.fromisoformat(selected[-1]["date"]) - prior).days / DAYS_PER_YEAR
        annual = math.exp(own / years) - math.exp(other / years)
    return {
        "days": days,
        "log": own - other if days else None,
        "fixed_days": fixed_days,
        "fixed_log": fixed_own - fixed_other if fixed_days else None,
        "annual": annual,
    }


def difference_line(difference: Mapping[str, Any], *, annual: bool = True) -> str:
    prefix = "F0001 对同平均敞口固定的累计对数差"
    if not difference["days"]:
        return prefix + "：还没有行。"
    text = f"{prefix} {difference['log'] * 100:+.2f} 点（{difference['days']} 个交易日）"
    if not annual:
        return text + "。"
    fixed = difference["fixed_log"]
    fixed_text = "还没有行" if fixed is None else f"{fixed * 100:+.2f} 点"
    text += f"；对固定 0.8 {fixed_text}。"
    value = difference["annual"]
    return text + (
        "不满一年，不印年化。" if value is None else f"几何年化差 {value * 100:+.2f} 点/年。"
    )


def drawdown_state(prices: pd.Series) -> dict[str, Any]:
    first = pd.Timestamp(prices.index[0]).date()
    last = pd.Timestamp(prices.index[-1]).date()
    state: dict[str, Any] = {"first": first.isoformat(), "last": last.isoformat()}
    level = segments.monthly_level(prices.pct_change().iloc[1:])
    level = level.loc[[_folder_day(month.end_time.date()) <= last for month in level.index]]
    if level.empty:
        return {**state, "state": "short", "month_end": None}
    state["month_end"] = str(level.index[-1])
    drops = segments.drop_segments(level)
    drop = drops[-1] if drops else None
    if drop is not None:
        # drop_segments returns only declines; unlike bull segments, both endpoints exist.
        assert drop.peak is not None and drop.trough is not None
        peak_level = float(level.loc[drop.peak])
        after_peak = level.loc[level.index > pd.Period(drop.peak, freq="M")]
        if not (after_peak >= peak_level).any():
            cross = after_peak.loc[after_peak <= peak_level * (1 - segments.DROP)].index[0]
            return {
                **state,
                "state": "open",
                "kind": drop.kind,
                "peak": drop.peak,
                "trough": drop.trough,
                "months": drop.months,
                "change": drop.change,
                "cross": str(cross),
                "latest": float(level.iloc[-1]) / peak_level - 1,
                "last_day": float(prices.iloc[-1] / prices.iloc[0]) / peak_level - 1,
            }
    running = level if drop is None else level.loc[level.index > pd.Period(drop.trough, freq="M")]
    peak = running.idxmax()
    peak_level = float(running.loc[peak])
    latest = float(level.iloc[-1]) / peak_level - 1
    return {
        **state,
        "state": "none",
        "peak": str(peak),
        "latest": latest,
        "to_line": latest + segments.DROP,
        "last_day": float(prices.iloc[-1] / prices.iloc[0]) / peak_level - 1,
    }


def _signed_pct(value: float) -> str:
    return f"{value:+.1%}"


def render_drawdown(capture: Capture | None) -> list[str]:
    lines = ["", "## QQQ 离运行最高（月末总收益，划段规则）", ""]
    if capture is None:
        return [*lines, NO_CAPTURE]
    state = drawdown_state(capture.qqq)
    month_end = state["month_end"] or "没有"
    lines += [
        f"价格文件 `{capture.file}`（账本最后一行用的那份），"
        f"QQQ adj_close {state['first']}..{state['last']}，最后一个完整月末 {month_end}。",
        SEGMENT_RULE,
        "",
    ]
    if state["state"] == "short":
        return [*lines, "现在：价格文件里还没有一个完整的月，不印。"]
    if state["state"] == "open":
        kind = (
            "已定为慢熊（谷底只会更晚）。"
            if state["kind"] == "slow_bear"
            else "到目前按快跌划；谷底还会变，慢熊还是快跌未定"
            f"（谷底离高点满 {segments.SLOW_MONTHS} 个月为慢熊）。"
        )
        lines.append(
            f"现在：开段。高点 {state['peak']}，{state['cross']} 月末跌破 "
            f"{1 - segments.DROP:.0%}；到目前最低 {state['trough']}"
            f"（{_signed_pct(state['change'])}，离高点 {state['months']} 个月）；"
            f"最近一个完整月末离高点 {_signed_pct(state['latest'])}。{kind}"
            "这是第 6 期章程第 9 节第 3 条和 F0008 第 1 行的触发。"
        )
        reference = "高点"
    else:
        lines.append(
            f"现在：未开段。运行最高在 {state['peak']}（月末），"
            f"最近一个完整月末 {state['month_end']} 离它 {_signed_pct(state['latest'])}，"
            f"离开段线还差 {state['to_line'] * 100:.1f} 点。"
        )
        reference = "运行最高"
    lines.append(
        f"最后价格日 {state['last']} 离{reference} {_signed_pct(state['last_day'])}"
        "（不是月末，不划段，只印）。"
    )
    return lines


def render_cash(rows: Sequence[Mapping[str, Any]], capture: Capture | None) -> list[str]:
    lines = ["", "## 现金按 SGOV（只印，不是判的口径）", ""]
    if capture is None:
        return [*lines, NO_CAPTURE]
    if capture.cash.empty:
        return [*lines, "价格文件里没有 SGOV，这一节不印。"]
    lines.append(
        "现金按同一个价格文件里 SGOV 的 adj_close：每条线当天收益 = 敞口 × QQQ 收益 +"
        "（1 − 敞口）× SGOV 收益，没有那天的 SGOV 就记空；"
        "对照的敞口用上面现金 0 那套的平均。账本的行不改，现金 0 仍是判的口径。"
    )
    cash = cash_returns(capture.cash, rows)
    for since, title in ((BACKFILL_START, "含回填"), (FORWARD_START, "前瞻")):
        lines += [
            "",
            f"### 自 {since}（{title}）",
            "",
            "| 线 | 交易日 | 缺收益 | 累计 |",
            "|---|---|---|---|",
        ]
        for key, summary in summarize(rows, since, cash).items():
            cumulative = _pct(summary["cumulative"]) if summary["days"] else "—"
            lines.append(
                f"| {LABELS[key]} | {summary['days']} | {summary['missing']} | {cumulative} |"
            )
        lines += ["", difference_line(overlay_difference(rows, since, cash), annual=False)]
    return lines


def _pct(value: float | None, missing: str = "—") -> str:
    return missing if value is None else f"{value * 100:+.2f}%"


def _exp(value: float | None, missing: str = "—") -> str:
    return missing if value is None else f"{value:.2f}"


def render_weekly(rows: Sequence[Mapping[str, Any]], capture: Capture | None = None) -> str:
    backfill_end = _folder_day(FORWARD_START - timedelta(days=1))
    lines = [
        "# 前瞻记录台周汇总",
        "",
        f"- 运行：{RUN_ID}；依据 `{DAILY_POLICY}`、`{READ_DAILY_OUTPUTS}`。"
        "三条线：日报模型、固定敞口 0.8、F0001（QQQ-Q3 fast_slow）。",
        f"- {BACKFILL_START} 到 {backfill_end} 是回填（快照在 Baker 点卡以前生成），"
        f"{FORWARD_START} 起是前瞻。现金收益记 0；收益不扣成本。描述，不判。",
        "",
    ]
    if not rows:
        lines.append("账本还没有行。")
    else:
        lines.append(
            f"账本 {len(rows)} 行，{rows[0]['date']}..{rows[-1]['date']}。{offsets_line(rows)}。"
        )
        for since, title in ((BACKFILL_START, "含回填"), (FORWARD_START, "前瞻")):
            lines += [
                "",
                f"## 自 {since}（{title}）",
                "",
                "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |",
                "|---|---|---|---|---|---|",
            ]
            for key, summary in summarize(rows, since).items():
                cumulative = _pct(summary["cumulative"]) if summary["days"] else "—"
                drawdown = _pct(summary["max_drawdown"]) if summary["days"] else "—"
                lines.append(
                    f"| {LABELS[key]} | {summary['days']} | {summary['missing']} | "
                    f"{cumulative} | {drawdown} | {_exp(summary['mean_exposure'])} |"
                )
            prefix = REAL_RETURN if since == FORWARD_START else ""
            lines += ["", prefix + difference_line(overlay_difference(rows, since))]
        lines += render_drawdown(capture)
        lines += render_cash(rows, capture)
        lines += [
            "",
            "## 按周",
            "",
            "| 周（最后一个交易日） | 交易日 | 日报模型 | 固定 | 覆盖 | "
            "日报平均敞口 | 覆盖平均敞口 |",
            "|---|---|---|---|---|---|---|",
        ]
        weeks: dict[tuple[int, int], list[Mapping[str, Any]]] = {}
        for row in rows:
            iso = date.fromisoformat(row["date"]).isocalendar()
            weeks.setdefault((iso.year, iso.week), []).append(row)
        for week in weeks.values():
            summary_by_line = summarize(week, BACKFILL_START)
            returns = " | ".join(_pct(summary_by_line[line]["cumulative"]) for line in LINES)
            lines.append(
                f"| {week[-1]['date']} | {len(week)} | {returns} | "
                f"{_exp(summary_by_line['daily']['mean_exposure'])} | "
                f"{_exp(summary_by_line['overlay']['mean_exposure'])} |"
            )
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_weekly(ledger_dir: Path, daily_root: Path | None = None) -> Path:
    rows = read_ledger(ledger_dir)
    capture = None if daily_root is None else read_weekly_capture(daily_root, rows)
    text = render_weekly(rows, capture)
    path = ledger_dir / WEEKLY_FILE
    try:
        ledger_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise ForwardLedgerError(f"{path}: {exc}") from exc
    return path


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError("not ISO date")
        return parsed
    except ValueError:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from None


@app.command("append")
def append_command(
    through: Annotated[str, typer.Option(help="追加截至日期 YYYY-MM-DD。")],
    daily_root: Annotated[Path, typer.Option(help="日报状态根目录（只读）。")],
    ledger_dir: Annotated[
        Path, typer.Option(help="前瞻账本目录。日数据目录不会写入。")
    ] = DEFAULT_LEDGER_DIR,
) -> None:
    folder = _folder_day(_date(through))
    try:
        rows = append(ledger_dir, daily_root, folder, provenance=git_provenance(PROJECT_ROOT))
        total = len(read_ledger(ledger_dir))
    except ForwardLedgerError as exc:
        typer.echo(f"forward_ledger: 拒绝追加：{exc}")
        raise typer.Exit(code=1) from None
    if rows:
        typer.echo(
            f"forward_ledger: F0001 的敞口用 {PRICES_REL.format(day=folder.isoformat())} "
            "里的 QQQ 历史算"
        )
        for row in rows:
            backfill = "（回填）" if row["backfill"] else ""
            notes = "（" + "；".join(row["notes"]) + "）" if row["notes"] else ""
            typer.echo(
                f"forward_ledger: {row['date']}{backfill} QQQ {_pct(row['qqq_tr'], '空')} "
                f"日报 {_exp(row['exp_daily_held'], '空')} 固定 {_exp(row['exp_fixed'], '空')} "
                f"覆盖 {_exp(row['exp_overlay'], '空')}{notes}"
            )
        if rows[-1]["date"] < folder.isoformat():
            typer.echo(
                f"forward_ledger: {folder} 的价格捕获文件里最后的价格日是 {rows[-1]['date']}，"
                "只追加到这一天"
            )
        typer.echo(
            f"forward_ledger: 追加 {len(rows)} 行（{rows[0]['date']}..{rows[-1]['date']}），"
            f"共 {total} 行"
        )
    else:
        typer.echo(f"forward_ledger: 没有新的交易日，共 {total} 行")


@app.command("weekly")
def weekly_command(
    ledger_dir: Annotated[Path, typer.Option(help="前瞻账本目录。")] = DEFAULT_LEDGER_DIR,
    daily_root: Annotated[
        Path | None,
        typer.Option(help="日报状态根目录（只读）；给了才印 QQQ 离运行最高和现金按 SGOV"),
    ] = None,
) -> None:
    try:
        path = write_weekly(ledger_dir, daily_root)
    except ForwardLedgerError as exc:
        typer.echo(f"forward_ledger: {exc}")
        raise typer.Exit(code=1) from None
    typer.echo(f"forward_ledger: 周汇总已写 {path}")


@app.command("check")
def check_command(
    ledger_dir: Annotated[Path, typer.Option(help="前瞻账本目录。")] = DEFAULT_LEDGER_DIR,
    day: Annotated[str | None, typer.Option(help="核对这一天的源文件 YYYY-MM-DD。")] = None,
    daily_root: Annotated[Path | None, typer.Option(help="日报状态根目录（只读）。")] = None,
) -> None:
    selected_day = None if day is None else _date(day)
    if selected_day is not None and daily_root is None:
        raise typer.BadParameter("--day 需要 --daily-root")
    try:
        problems = verify(ledger_dir)
        if not problems and selected_day is not None and daily_root is not None:
            problems = check_day(ledger_dir, daily_root, selected_day)
        rows = [] if problems else read_ledger(ledger_dir)
    except ForwardLedgerError as exc:
        problems, rows = [str(exc)], []
    for problem in problems:
        typer.echo(f"forward_ledger: {problem}")
    if problems:
        raise typer.Exit(code=1)
    span = f"{rows[0]['date']}..{rows[-1]['date']}" if rows else "空"
    typer.echo(f"forward_ledger: 账本完好，{len(rows)} 行（{span}）")
    if rows:
        typer.echo(f"forward_ledger: {offsets_line(rows)}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
