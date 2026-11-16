"""The frozen line of the forward ledger: the picked scaled buy-back line (TRADING-2565-137).

Owner decision owner_decision:TRADING-2565:2026-11-10:same_drawdown_table and
Baker's 2026-11-16 freeze choice. Reuse TRADING-2565-126's x2_s5_under_ladder
unchanged, calling the original modules only. Walk each row's own price file from
the anchor. A close-t decision trades at t+1 close: sales share the table's close,
purchases are one close later. Idle cash earns zero; costs are zero. Continue from
written rows. Reproduce the printed trip and every written decision or refuse to
write. Neither the main ledger nor its added lines is ever modified or blocked.
Accounting only: no judgment, daily-report integration, broker or production action.
"""

from __future__ import annotations

import functools
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_probes import entry_structure_shift as es
from ai_trading_system.research_probes import entry_timing_counts as ec
from ai_trading_system.research_probes import exit_rules_counts as xc
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.research_probes import structure_shift_ladder as sl
from ai_trading_system.research_probes import structure_shift_periods as sp

RUN_ID = "TRADING-2565-137"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-10:same_drawdown_table"
SCHEMA = "forward_frozen.v1"
FROZEN_FILE = "frozen_lines.jsonl"
LINE = "x2_s5_under_ladder"
# Owner-frozen accounting boundaries and table reprint checks, not promotion gates.
FROZEN_START = date(2026, 11, 23)
WALK_FROM = date(2024, 1, 2)
ANCHOR = date(2025, 5, 12)
SNAPSHOT_END = date(2026, 10, 12)
PRINTED = (
    {
        "decision": "2026-03-20",
        "reason": "X2",
        "sell": "2026-03-23",
        "buy": "2026-04-13",
        "how": "shift",
        "fills": (),
        "floor_date": "2026-03-30",
    },
)
PRINTED_FLOOR = date(2026, 3, 30)
ANCHOR_SALE = date(2025, 3, 7)
ANCHOR_FILL = date(2025, 4, 4)
ANCHOR_AT = 0.8607226346224827
ANCHOR_AT_TOLERANCE = 0.001
# The table's frozen tranche levels and descriptive cost, never acceptance thresholds.
LEVELS = (0.9, 0.8, 0.7, 0.6)
HOWS = frozenset({"recovery", "ladder", "shift", "entry"})
PHASES = frozenset({"in", "fired", "out"})
TABLE_COST = 0.0005
SECTION = "## 冻结的线"
STATE_KEYS = frozenset(
    {
        "rule_frac",
        "phase",
        "sale_date",
        "filled_levels",
        "close_vs_x",
        "high_date",
        "close_vs_high",
        "floor_date",
        "close_vs_floor",
        "close_vs_line",
    }
)
ROW_KEYS = frozenset(
    {
        "schema_version",
        "seq",
        "date",
        "ledger_seq",
        "ledger_sha256",
        "qqq_tr",
        "price_file",
        "rule_price_file",
        "sgov_tr",
        "line",
        "rule_actions",
        "rule_frac",
        "trade_frozen",
        "exp_frozen",
        "ret_frozen",
        "phase",
        "sale_date",
        "filled_levels",
        "close_vs_x",
        "high_date",
        "close_vs_high",
        "floor_date",
        "close_vs_floor",
        "close_vs_line",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }
)
app = typer.Typer(help="冻结的线：只读核对，只记账，不判。")


class FrozenLinesError(ValueError):
    """The frozen line or its checks cannot be used; do not append anything."""


class WalkGap(FrozenLinesError):
    """No named capture can support the rule through the requested close."""


@dataclass(frozen=True)
class Walk:
    """Cached rule output; consumers must copy mutable payloads before storing them."""

    dates: tuple[date, ...]
    close: tuple[float, ...]
    actions: dict[str, list[dict[str, Any]]]
    state: dict[str, Any]
    trips: tuple[dict[str, Any], ...]


@functools.cache
def _days(start: date, end: date) -> tuple[date, ...]:
    return tuple(fl.trading_days(start, end))


def walk(dates: Sequence[date], close: Sequence[float]) -> Walk:
    return _walk(ANCHOR, tuple(dates), tuple(close))


@functools.lru_cache(maxsize=2048)
def _walk(anchor: date, dates: tuple[date, ...], close: tuple[float, ...]) -> Walk:
    if len(dates) != len(close) or anchor not in dates:
        raise FrozenLinesError("价格不能从锚点起走")
    prepared = xc.prepare(dates, close)
    line = ec.moving_average(close)
    entries = sp.v0_entries(close, 0)
    moves = es.shifts(close, line, 5, ec.SMA_DAYS)
    b = dates.index(anchor)
    path = sl.walk_ladder(
        prepared,
        entries,
        moves,
        b,
        lambda rule, data, bought: sp.exit_under(rule, data, line, bought),
    )
    actions: dict[str, list[dict[str, Any]]] = {}
    trips: list[dict[str, Any]] = []
    floor: str | None = None
    for original in path.route.trips:
        trip = dict(original)
        trip["floor_date"] = None
        actions.setdefault(trip["decision"], []).append({"kind": "fire", "reason": trip["reason"]})
        for fill in trip["fills"]:
            actions.setdefault(fill["date"], []).append(
                {"kind": "fill", "level": fill["level"], "share": fill["share"]}
            )
        floor = None
        if trip["how"] != "none":
            b = dates.index(date.fromisoformat(trip["buy"]))
            if trip["how"] == "shift":
                sold = dates.index(date.fromisoformat(trip["sell"]))
                move = next(m for m in moves if m.entry > sold)
                floor = dates[move.low].isoformat()
            trip["floor_date"] = floor
            actions.setdefault(trip["buy"], []).append(
                {"kind": "full", "how": trip["how"], "floor_date": floor}
            )
        trips.append(trip)
    last = dates[-1].isoformat()
    out = bool(trips and trips[-1]["how"] == "none")
    phase = "out" if out else "in"
    if not out:
        fired = sp.exit_under("x2", prepared, line, b)
        if floor is not None:
            low = close[dates.index(date.fromisoformat(floor))]
            under = next((k for k in range(b + 1, len(close)) if close[k] < low), None)
            if under is not None and (fired is None or under < fired[0]):
                fired = (under, "L")
        if fired is not None:
            if fired[0] != len(dates) - 1:
                raise FrozenLinesError("规则和分批买回路径不一致：更早的出场未记")
            actions.setdefault(last, []).append({"kind": "fire", "reason": fired[1]})
            phase = "fired"
    state: dict[str, Any] = dict.fromkeys(STATE_KEYS)
    state["phase"] = phase
    at_line = line[-1]
    if at_line is None:
        raise FrozenLinesError("最后收盘没有 200 日线")
    state["close_vs_line"] = close[-1] / at_line - 1.0
    if out:
        trip = trips[-1]
        sale = dates.index(date.fromisoformat(trip["sell"]))
        state.update(
            rule_frac=_fraction(dates, close, trip, len(dates) - 1),
            sale_date=trip["sell"],
            filled_levels=[f["level"] for f in trip["fills"]],
            close_vs_x=close[-1] / close[sale] - 1.0,
        )
    else:
        high = max(range(b, len(close)), key=lambda k: close[k])
        state.update(
            rule_frac=1.0,
            high_date=dates[high].isoformat(),
            close_vs_high=close[-1] / close[high] - 1.0,
            floor_date=floor,
        )
        if floor is not None:
            state["close_vs_floor"] = (
                close[-1] / close[dates.index(date.fromisoformat(floor))] - 1.0
            )
    return Walk(dates, close, actions, state, tuple(trips))


def _fraction(
    dates: Sequence[date], close: Sequence[float], trip: Mapping[str, Any], k: int
) -> float:
    fills = [f for f in trip["fills"] if f["date"] <= dates[k].isoformat()]
    held = sum(
        f["share"] * close[k] / close[dates.index(date.fromisoformat(f["date"]))] for f in fills
    )
    idle = 1.0 - sum(f["share"] for f in fills)
    return float(held / (held + idle))


def _read_walk(daily_root: Path, relative: str, through: date) -> Walk:
    prices = fl.read_capture(daily_root, relative)
    if prices is None:
        raise WalkGap(f"{relative}: 文件不在")
    pairs = []
    for stamp, value in prices.items():
        if not isinstance(stamp, datetime):
            raise WalkGap(f"{relative}: 价格索引不是日期")
        day = stamp.date()
        if WALK_FROM <= day <= through:
            pairs.append((day, float(value)))
    dates = tuple(day for day, _ in pairs)
    expected = _days(WALK_FROM, through)
    if dates != expected:
        missing = next((day for day in expected if day not in dates), None)
        raise WalkGap(f"{relative}: 交易日不齐或多出，缺 {missing}，截至 {through}")
    return walk(dates, [value for _, value in pairs])


def rule_file(
    main_rows: Sequence[Mapping[str, Any]], main: Mapping[str, Any], daily_root: Path
) -> tuple[str, Walk, str | None]:
    day = date.fromisoformat(main["date"])
    first_reason = None
    candidates = [main, *(r for r in main_rows if r["seq"] > main["seq"])]
    for candidate in candidates:
        relative = candidate["price_file"]
        try:
            result = _read_walk(daily_root, relative, day)
            return relative, result, first_reason
        except (WalkGap, fl.ForwardLedgerError) as exc:
            if first_reason is None:
                first_reason = str(exc)
    raise WalkGap(f"{day}: {first_reason}")


def _guard_walk(result: Walk) -> None:
    try:
        cut = result.dates.index(SNAPSHOT_END) + 1
        snapshot = walk(result.dates[:cut], result.close[:cut])
        expected = [{**trip, "fills": list(trip["fills"])} for trip in PRINTED]
        actual = [{key: trip[key] for key in expected_keys()} for trip in snapshot.trips]
        if actual != expected:
            raise FrozenLinesError("买卖不等于表印的那一笔")
        if (
            snapshot.state["phase"] != "in"
            or snapshot.state["floor_date"] != PRINTED_FLOOR.isoformat()
        ):
            raise FrozenLinesError("快照末日状态不符")
        dates, close = snapshot.dates, snapshot.close
        sale, fill = dates.index(ANCHOR_SALE), dates.index(ANCHOR_FILL)
        if abs(close[fill] / close[sale] - ANCHOR_AT) > ANCHOR_AT_TOLERANCE:
            raise FrozenLinesError("锚点的分档相对卖价不符")
        recovered = next((k for k in range(fill + 1, len(close)) if close[k] >= close[sale]), None)
        if recovered is None or dates[recovered] != ANCHOR:
            raise FrozenLinesError("锚点不是首次回到卖价")
        entries = sp.v0_entries(close, 0)
        moves = es.shifts(close, ec.moving_average(close), 5, ec.SMA_DAYS)
        signal = sl.signal_after(close, entries, moves, sale)
        if signal is not None and dates[signal[0]] < ANCHOR:
            raise FrozenLinesError("锚点以前已有信号")
    except (ValueError, LookupError, ArithmeticError) as exc:
        raise FrozenLinesError(f"核对 A 不过：{exc}") from exc


def expected_keys() -> tuple[str, ...]:
    return ("decision", "reason", "sell", "buy", "how", "fills", "floor_date")


def guard(daily_root: Path, price_file: str) -> None:
    try:
        result = _read_walk(daily_root, price_file, SNAPSHOT_END)
        _guard_walk(result)
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        if str(exc).startswith("核对 A 不过"):
            raise
        raise FrozenLinesError(f"核对 A 不过：{exc}") from exc


def target(actions: Sequence[Mapping[str, Any]], fraction: float) -> float | None:
    kinds = [a["kind"] for a in actions]
    if "fire" in kinds:
        return 0.0
    if "full" in kinds:
        return 1.0
    if "fill" in kinds:
        return fraction
    return None


def drift(w: float | None, q: float | None) -> float | None:
    if w is None or w in (0.0, 1.0):
        return w
    if q is None:
        return None
    return w * (1.0 + q) / (1.0 + w * q)


def _simulate(result: Walk, day: date) -> tuple[float | None, float | None]:
    """Return the first row's trade and held weight, without transaction costs."""
    dates, close = result.dates, result.close
    start, end = dates.index(ANCHOR), dates.index(day)
    if end - start < 2:
        raise FrozenLinesError("起点离锚点不到两个收盘")

    def decided(k: int) -> float | None:
        if k == start:
            return 1.0
        key = dates[k].isoformat()
        frac = 1.0
        for trip in result.trips:
            if trip["sell"] <= key and (trip["buy"] is None or key < trip["buy"]):
                frac = _fraction(dates, close, trip, k)
                break
        return target(result.actions.get(key, []), frac)

    w: float | None = None
    for k in range(start + 2, end + 1):
        traded = decided(k - 2)
        w = traded if traded is not None else drift(w, close[k - 1] / close[k - 2] - 1.0)
    return decided(end - 1), w


def _category(value: Any) -> str:
    return "none" if value is None else "zero" if value == 0 else "one" if value == 1 else "partial"


def _check_b(rows: Sequence[Mapping[str, Any]], result: Walk) -> None:
    for row in rows:
        if row["date"] <= result.dates[-1].isoformat():
            if row["rule_actions"] != result.actions.get(row["date"], []):
                raise FrozenLinesError(f"核对 B 不过：{row['date']} 已写的决定变了")
    if rows:
        trade, _ = _simulate(result, date.fromisoformat(rows[0]["date"]))
        if _category(trade) != _category(rows[0]["trade_frozen"]):
            raise FrozenLinesError("核对 B 不过：第一行调仓类别变了")


def _row_bytes(row: Mapping[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _frozen_lines(ledger_dir: Path) -> list[bytes]:
    try:
        return [
            line
            for line in (ledger_dir / FROZEN_FILE).read_bytes().replace(b"\r\n", b"\n").split(b"\n")
            if line
        ]
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise FrozenLinesError(f"{FROZEN_FILE}: {exc}") from exc


def _date(value: Any) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise FrozenLinesError("不是 YYYY-MM-DD 日期")
    return parsed


def _actions_problem(actions: Any) -> str | None:
    if not isinstance(actions, list):
        return "rule_actions is not a list"
    full = False
    seen: list[float] = []
    for action in actions:
        if not isinstance(action, dict):
            return "rule_actions has a non-object"
        kind = action.get("kind")
        if kind == "fire":
            if (
                action.keys() != {"kind", "reason"}
                or action["reason"] not in {"X2", "L"}
                or len(actions) != 1
            ):
                return "fire must be X2 or L and stand alone"
        elif kind == "fill":
            if action.keys() != {"kind", "level", "share"} or full:
                return "fill keys or order do not match"
            if not fl._number(action["level"]) or not fl._number(action["share"]):
                return "fill level/share is not numeric"
            if (action["level"], action["share"]) not in sl.LEVELS or action["level"] in seen:
                return "fill level/share is not a frozen tranche"
            seen.append(action["level"])
            if seen != sorted(seen, reverse=True):
                return "fills are out of order"
        elif kind == "full":
            if action.keys() != {"kind", "how", "floor_date"} or full or action["how"] not in HOWS:
                return "full keys/how or order do not match"
            full = True
            if action["how"] == "shift":
                _date(action["floor_date"])
            elif action["floor_date"] is not None:
                return "only shift has a floor_date"
        else:
            return "unknown rule action"
    return None


def _inline_problem(row: Mapping[str, Any], previous: Mapping[str, Any] | None) -> str | None:
    if row["line"] != LINE:
        return "line does not match"
    problem = _actions_problem(row["rule_actions"])
    if problem:
        return problem
    for key in (
        "qqq_tr",
        "sgov_tr",
        "rule_frac",
        "exp_frozen",
        "trade_frozen",
        "ret_frozen",
        "close_vs_x",
        "close_vs_high",
        "close_vs_floor",
        "close_vs_line",
    ):
        if row[key] is not None and not fl._number(row[key]):
            return f"{key} is not a finite number or None"
    for key in ("rule_frac", "exp_frozen", "trade_frozen"):
        if row[key] is not None and not 0 <= row[key] <= 1:
            return f"{key} is not in [0, 1]"
    if row["rule_frac"] is None or row["close_vs_line"] is None:
        return "rule_frac/close_vs_line is missing"
    if not fl._equal(row["ret_frozen"], fl._product(row["exp_frozen"], row["qqq_tr"])):
        return "ret_frozen does not equal exp_frozen * qqq_tr"
    phase = row["phase"]
    if phase not in PHASES:
        return "phase is not in PHASES"
    kinds = [a["kind"] for a in row["rule_actions"]]
    if ("fire" in kinds) != (phase == "fired"):
        return "fire and fired must occur together"
    if "full" in kinds and phase != "in":
        return "full must end in phase in"
    if phase in {"in", "fired"}:
        if row["rule_frac"] != 1.0 or any(
            row[k] is not None for k in ("sale_date", "filled_levels", "close_vs_x")
        ):
            return "invested phase has sale state or fraction is not one"
        if row["high_date"] is None or row["close_vs_high"] is None:
            return "invested phase needs high state"
        if _date(row["high_date"]) > _date(row["date"]):
            return "high_date is after the row"
        if (row["floor_date"] is None) != (row["close_vs_floor"] is None):
            return "floor state must be present together"
        if row["floor_date"] is not None and _date(row["floor_date"]) > _date(row["date"]):
            return "floor_date is after the row"
        if "fill" in kinds and "full" not in kinds:
            return "fill without full must be out"
    else:
        if _date(row["sale_date"]) > _date(row["date"]):
            return "sale_date is after the row"
        levels = row["filled_levels"]
        if not isinstance(levels, list) or len(levels) > 3 or levels != list(LEVELS[: len(levels)]):
            return "filled_levels is not a prefix of 0.9, 0.8, 0.7"
        if any(not fl._number(level) for level in levels):
            return "filled_levels is not numeric"
        if row["close_vs_x"] is None or any(
            row[k] is not None
            for k in ("high_date", "close_vs_high", "floor_date", "close_vs_floor")
        ):
            return "out phase has wrong sale/high/floor state"
        if row["rule_frac"] >= 1 or (row["rule_frac"] == 0) != (not levels):
            return "out fraction does not match tranches"
        fills = [a["level"] for a in row["rule_actions"] if a["kind"] == "fill"]
        if fills and (not levels or levels[-len(fills) :] != fills):
            return "today's fills do not match filled_levels"
    if previous is not None:
        if _days(_date(previous["date"]), _date(row["date"])) != (
            _date(previous["date"]),
            _date(row["date"]),
        ):
            return "rows are not adjacent trading days"
        if not fl._equal(
            row["trade_frozen"], target(previous["rule_actions"], previous["rule_frac"])
        ):
            return "trade_frozen does not follow the previous decision"
        expected = previous["trade_frozen"]
        if expected is None:
            expected = drift(previous["exp_frozen"], previous["qqq_tr"])
        if not fl._equal(row["exp_frozen"], expected):
            return "exp_frozen does not follow trade and drift"
        if previous["phase"] == "fired":
            if (
                phase != "out"
                or row["sale_date"] != row["date"]
                or row["rule_frac"] != 0
                or row["filled_levels"] != []
            ):
                return "the close after fired must sell into an empty out phase"
        elif previous["phase"] == "out":
            if phase == "out":
                before, after = previous["filled_levels"], row["filled_levels"]
                if row["sale_date"] != previous["sale_date"] or after[: len(before)] != before:
                    return "out sale/tranches changed backwards"
            elif "full" not in kinds:
                return "out can only end with a full purchase"
        elif phase == "out":
            return "in cannot turn out without a fire"
    if not isinstance(row["notes"], list) or any(not isinstance(n, str) for n in row["notes"]):
        return "notes is not a list of strings"
    if not isinstance(row["code_commit"], str) or len(row["code_commit"]) != 40:
        return "code_commit is not a 40-character string"
    if row["code_modified"] is not False:
        return "code_modified is not False"
    return None


def _rule_source(row: Mapping[str, Any], main_rows: Sequence[Mapping[str, Any]]) -> bool:
    relative = row["rule_price_file"]
    return isinstance(relative, str) and (
        relative == row["price_file"]
        or (
            any(r["seq"] > row["ledger_seq"] and r["price_file"] == relative for r in main_rows)
            and isinstance(row["notes"], list)
            and any(isinstance(n, str) and relative in n for n in row["notes"])
        )
    )


def verify(ledger_dir: Path) -> list[str]:
    prefix = f"{FROZEN_FILE} line 1: "
    try:
        lines = _frozen_lines(ledger_dir)
        if not lines:
            return []
        main_rows = fl.read_ledger(ledger_dir)
        selected = [
            (r, raw)
            for r, raw in zip(main_rows, fl._ledger_lines(ledger_dir), strict=True)
            if r["date"] >= FROZEN_START.isoformat()
        ]
        previous = None
        digest = None
        for position, raw in enumerate(lines, 1):
            prefix = f"{FROZEN_FILE} line {position}: "
            row = json.loads(raw)
            if not isinstance(row, dict) or row.keys() != ROW_KEYS:
                return [prefix + "keys do not match ROW_KEYS"]
            if row["schema_version"] != SCHEMA:
                return [prefix + "schema_version does not match"]
            if type(row["seq"]) is not int or row["seq"] != position:
                return [prefix + f"seq is not {position}"]
            if row["prev_sha256"] != digest:
                return [prefix + "prev_sha256 does not match"]
            if position > len(selected):
                return [prefix + "main ledger has no matching row"]
            main, main_raw = selected[position - 1]
            if type(row["ledger_seq"]) is not int or row["ledger_seq"] != main["seq"]:
                return [prefix + "ledger_seq does not continue from FROZEN_START"]
            if row["date"] != main["date"]:
                return [prefix + "date does not match the main ledger"]
            if row["ledger_sha256"] != chain_digest(main_raw):
                return [prefix + "ledger_sha256 does not match the main ledger"]
            if (
                not fl._equal(row["qqq_tr"], main["qqq_tr"])
                or row["price_file"] != main["price_file"]
            ):
                return [prefix + "qqq_tr or price_file differs from the main ledger"]
            if not _rule_source(row, main_rows):
                return [prefix + "rule_price_file is not own or a noted later capture"]
            problem = _inline_problem(row, previous)
            if problem:
                return [prefix + problem]
            previous, digest = row, chain_digest(raw)
        return []
    except (ValueError, TypeError, LookupError, ArithmeticError, AttributeError) as exc:
        return [prefix + str(exc)]


def read_frozen(ledger_dir: Path) -> list[dict[str, Any]]:
    problems = verify(ledger_dir)
    if problems:
        raise FrozenLinesError(problems[0])
    return [json.loads(raw) for raw in _frozen_lines(ledger_dir)]


def _sgov(daily_root: Path, relative: str, day: date) -> float | None:
    try:
        prices = fl.read_capture(daily_root, relative, fl.CASH_TICKER)
        return None if prices is None else fl.day_return(prices, day)
    except fl.ForwardLedgerError:
        return None


def _write_rows(ledger_dir: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    path = ledger_dir / FROZEN_FILE
    try:
        separator = path.exists() and path.stat().st_size > 0
        if separator:
            with path.open("rb") as source:
                source.seek(-1, 2)
                separator = source.read(1) != b"\n"
        ledger_dir.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as sink:
            sink.write(
                (b"\n" if separator else b"") + b"".join(_row_bytes(r) + b"\n" for r in rows)
            )
    except OSError as exc:
        raise FrozenLinesError(f"{FROZEN_FILE}: {exc}") from exc


def append(
    ledger_dir: Path, daily_root: Path, *, provenance: Mapping[str, Any]
) -> list[dict[str, Any]]:
    commit = provenance.get("commit")
    if provenance.get("code_modified") is not False:
        raise FrozenLinesError(f"{FROZEN_FILE}: code_modified is not False")
    if not isinstance(commit, str) or len(commit) != 40:
        raise FrozenLinesError(f"{FROZEN_FILE}: commit is not a 40-character string")
    existing = read_frozen(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    last_seq = existing[-1]["ledger_seq"] if existing else 0
    previous = existing[-1] if existing else None
    raw_existing = _frozen_lines(ledger_dir)
    digest = chain_digest(raw_existing[-1]) if raw_existing else None
    rows: list[dict[str, Any]] = []
    for main, raw in zip(main_rows, fl._ledger_lines(ledger_dir), strict=True):
        if main["date"] < FROZEN_START.isoformat() or main["seq"] <= last_seq:
            continue
        day = date.fromisoformat(main["date"])
        try:
            relative, result, reason = rule_file(main_rows, main, daily_root)
        except WalkGap as exc:
            _write_rows(ledger_dir, rows)
            raise WalkGap(f"已写 {len(rows)} 行，停在 {day}：{exc}") from exc
        _guard_walk(result)
        _check_b([*existing, *rows], result)
        if previous is None:
            trade, w = _simulate(result, day)
        else:
            trade = target(previous["rule_actions"], previous["rule_frac"])
            w = previous["trade_frozen"]
            if w is None:
                w = drift(previous["exp_frozen"], previous["qqq_tr"])
        notes = []
        if relative != main["price_file"]:
            notes.append(f"规则用 {relative}；自己的文件撑不起：{reason}")
        if main["qqq_tr"] is None:
            notes.append("没有 QQQ 当天的收益")
        # Copy nested action/state payloads; never mutate the cached walk.
        state = {
            key: list(value) if isinstance(value, list) else value
            for key, value in result.state.items()
        }
        row = {
            "schema_version": SCHEMA,
            "seq": len(existing) + len(rows) + 1,
            "date": main["date"],
            "ledger_seq": main["seq"],
            "ledger_sha256": chain_digest(raw),
            "qqq_tr": main["qqq_tr"],
            "price_file": main["price_file"],
            "rule_price_file": relative,
            "sgov_tr": _sgov(daily_root, main["price_file"], day),
            "line": LINE,
            "rule_actions": [dict(action) for action in result.actions.get(main["date"], [])],
            **state,
            "trade_frozen": trade,
            "exp_frozen": w,
            "ret_frozen": fl._product(w, main["qqq_tr"]),
            "notes": notes,
            "code_commit": commit,
            "code_modified": False,
            "prev_sha256": digest,
        }
        problem = _inline_problem(row, previous)
        if problem:
            raise FrozenLinesError(f"{FROZEN_FILE} {day}: {problem}")
        rows.append(row)
        previous, digest = row, chain_digest(_row_bytes(row))
    _write_rows(ledger_dir, rows)
    return rows


def check_walk(ledger_dir: Path, daily_root: Path) -> list[str]:
    try:
        rows = read_frozen(ledger_dir)
        if not rows:
            return []
        last = rows[-1]
        guard(daily_root, last["rule_price_file"])
        result = _read_walk(daily_root, last["rule_price_file"], _date(last["date"]))
        _check_b(rows, result)
        _, w = _simulate(result, _date(rows[0]["date"]))
        if not fl._equal(w, rows[0]["exp_frozen"]):
            return ["核对 B 不过：第一行敞口和锚点模拟不符"]
        return []
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        return [str(exc)]


def check_day(
    ledger_dir: Path, daily_root: Path, day: date, echo: Callable[[str], Any] | None = None
) -> list[str]:
    try:
        rows = read_frozen(ledger_dir)
        row = next((r for r in rows if r["date"] == day.isoformat()), None)
        if row is None:
            return []
        if not _rule_source(row, fl.read_ledger(ledger_dir)):
            return [f"{FROZEN_FILE} {day}: rule_price_file 不符"]
        result = _read_walk(daily_root, row["rule_price_file"], day)
        problems = []
        values = {**result.state, "rule_actions": result.actions.get(day.isoformat(), [])}
        for key, value in values.items():
            equal = (
                fl._equal(row[key], value)
                if fl._number(value)
                else type(row[key]) is type(value) and row[key] == value
            )
            if not equal:
                problems.append(f"{FROZEN_FILE} {day}: {key} 和规则价格文件不符")
        sgov = _sgov(daily_root, row["price_file"], day)
        if row["sgov_tr"] is not None and not fl._equal(row["sgov_tr"], sgov):
            problems.append(f"{FROZEN_FILE} {day}: sgov_tr 和自己的价格文件不符")
        elif row["sgov_tr"] is None and sgov is not None and echo is not None:
            echo(f"forward_frozen: 提示：{day} 自己的文件现在读得到 SGOV 收益；已写的空值不改")
        if row["rule_price_file"] != row["price_file"]:
            try:
                _read_walk(daily_root, row["price_file"], day)
            except (FrozenLinesError, fl.ForwardLedgerError):
                pass
            else:
                if echo is not None:
                    echo(f"forward_frozen: 提示：{day} 自己的文件现在撑得起；保留已写的规则文件")
        if row["seq"] == 1:
            trade, w = _simulate(result, day)
            if not fl._equal(trade, row["trade_frozen"]) or not fl._equal(w, row["exp_frozen"]):
                problems.append(f"{FROZEN_FILE} {day}: 第一次敞口/调仓和锚点模拟不符")
        return problems
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        return [str(exc)]


def _action_text(actions: Sequence[Mapping[str, Any]]) -> str:
    parts = []
    for action in actions:
        if action["kind"] == "fire":
            parts.append(f"{action['reason']} 出场响")
        elif action["kind"] == "fill":
            parts.append(f"分档 {action['level']:.1f}X，份额 {action['share']:.1f}")
        else:
            parts.append(
                f"买满 {action['how']}"
                + (f"，L 止损 {action['floor_date']}" if action["floor_date"] else "")
            )
    return "；".join(parts) or "无决定"


def _state_text(state: Mapping[str, Any]) -> str:
    if state["phase"] == "out":
        return (
            f"out，卖出 {state['sale_date']}，已成交 {state['filled_levels']}，"
            f"规则份额 {fl._exp(state['rule_frac'])}"
        )
    return f"{state['phase']}，X2 最高 {state['high_date']}" + (
        f"，L 止损 {state['floor_date']}" if state["floor_date"] else ""
    )


def render_section(
    main_rows: Sequence[Mapping[str, Any]], frozen: Sequence[Mapping[str, Any]]
) -> list[str]:
    lines = [
        "",
        f"{SECTION}（自 {FROZEN_START}，只记账，不判）",
        "",
        f"依据 `{OWNER_DECISION}`，Baker 2026-11-16 选择冻，只记账。"
        "原样调用 TRADING-2565-126 的 x2_s5_under_ladder：X2 出场、分四档买回、"
        "结构转变带 L 止损；每行从锚点重走自己的价格文件，接着已写的行记账。"
        "收盘 t 的决定在 t+1 收盘调，卖出和表里同一个收盘，买进比表里晚一个收盘。"
        "空仓 0、不扣成本；不进日报、不动仓位、不判。",
        "",
    ]
    lag = sum(r["date"] >= FROZEN_START.isoformat() for r in main_rows) - len(frozen)
    if not frozen:
        lines.append(f"冻结的线自 {FROZEN_START} 起记，还没有行。")
    else:
        main = {r["seq"]: r for r in main_rows}
        matched = [main[r["ledger_seq"]] for r in frozen]
        lines += [
            "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |",
            "|---|---|---|---|---|---|",
        ]
        for label, rows, ret, exp in (
            ("冻结的线（账本口径）", frozen, "ret_frozen", "exp_frozen"),
            ("买入持有", matched, "qqq_tr", None),
            ("F0001（主账本）", matched, "ret_overlay", "exp_overlay"),
            ("固定 0.8（主账本）", matched, "ret_fixed", "exp_fixed"),
        ):
            summary = fl._summary(
                [r[ret] for r in rows], [1.0 if exp is None else r[exp] for r in rows]
            )
            lines.append(
                f"| {label} | {summary['days']} | {summary['missing']} | "
                f"{fl._pct(summary['cumulative'])} | {fl._pct(summary['max_drawdown'])} | "
                f"{fl._exp(summary['mean_exposure'])} |"
            )
        lines += ["", "最后状态：" + _state_text(frozen[-1]) + "。", "", "规则的决定和账本的调仓："]
        for row in frozen:
            if row["rule_actions"] or row["trade_frozen"] is not None:
                trade_text = (
                    "不调仓"
                    if row["trade_frozen"] is None
                    else f"账本调到 {fl._exp(row['trade_frozen'])}"
                )
                lines.append(
                    f"- {row['date']}：{_action_text(row['rule_actions'])}；{trade_text}。"
                )
        idle = [
            None
            if r["ret_frozen"] is None or r["exp_frozen"] is None or r["sgov_tr"] is None
            else r["ret_frozen"] + (1 - r["exp_frozen"]) * r["sgov_tr"]
            for r in frozen
        ]
        summary = fl._summary(idle, [r["exp_frozen"] for r in frozen])
        weights = [r["exp_frozen"] for r in frozen]
        turnover = sum(
            abs(b - a)
            for a, b in zip(weights, weights[1:], strict=False)
            if a is not None and b is not None
        )
        trades = sum(r["trade_frozen"] is not None for r in frozen)
        lines += [
            "",
            f"空仓那部分拿 SGOV 时（只印）：冻结的线累计 {fl._pct(summary['cumulative'])}"
            f"（缺收益 {summary['missing']} 天）。",
            f"账本调仓 {trades} 次，换手 {turnover:.2f}，按表的 0.05% 算成本 "
            f"{turnover * TABLE_COST * 100:.3f} 点（只印）。",
        ]
    lines += [
        "",
        "表里印过的最深回撤：−43.84%（2000-03-24 → 2001-04-04，空仓拿 DTB3），"
        "同段空仓记 0 时 −46.08%；QQQ 2011 年以后冻结线 −37.21%，买入持有 −35.12%。"
        "这是已看过的历史表，不是前瞻成绩，不判。",
        "",
        f"冻结线落后主账本 {lag} 行。",
    ]
    return lines


def extend_weekly(ledger_dir: Path) -> None:
    frozen = read_frozen(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    path = ledger_dir / fl.WEEKLY_FILE
    footer = "\nproduction_effect=none；broker_action=none。\n"
    try:
        text = path.read_text(encoding="utf-8")
        if not text.endswith(footer):
            raise FrozenLinesError(f"{fl.WEEKLY_FILE}: 页脚不符")
        if SECTION in text:
            raise FrozenLinesError(f"{fl.WEEKLY_FILE}: 已有「冻结的线」一节")
        section = "\n".join(render_section(main_rows, frozen)) + "\n"
        path.write_text(text[: -len(footer)] + section + footer, encoding="utf-8", newline="\n")
    except (OSError, UnicodeError, ArithmeticError) as exc:
        raise FrozenLinesError(f"{path}: {exc}") from exc


def after_append(
    ledger_dir: Path, daily_root: Path, provenance: Mapping[str, Any], echo: Callable[[str], Any]
) -> bool:
    try:
        rows = append(ledger_dir, daily_root, provenance=provenance)
        total = len(read_frozen(ledger_dir))
        if not total:
            echo(f"forward_frozen: 冻结的线自 {FROZEN_START} 起记，还没有行")
        else:
            for row in rows:
                echo(
                    f"forward_frozen: {row['date']} 敞口 {fl._exp(row['exp_frozen'], '空')} "
                    f"收益 {fl._pct(row['ret_frozen'], '空')} {row['phase']} "
                    f"{_action_text(row['rule_actions'])}"
                )
            echo(f"forward_frozen: 追加 {len(rows)} 行，共 {total} 行")
        return True
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_frozen: 冻结的线没有写完（主账本照写）：{exc}")
        return False


def after_check(
    ledger_dir: Path, day: date | None, daily_root: Path | None, echo: Callable[[str], Any]
) -> bool:
    try:
        problems = verify(ledger_dir)
        if not problems and daily_root is not None:
            problems = check_walk(ledger_dir, daily_root)
        if not problems and day is not None and daily_root is not None:
            problems = check_day(ledger_dir, daily_root, day, echo)
        if problems:
            for problem in problems:
                echo(f"forward_frozen: {problem}")
            return False
        rows = read_frozen(ledger_dir)
        span = f"{rows[0]['date']}..{rows[-1]['date']}" if rows else "空"
        echo(f"forward_frozen: 冻结的线完好，{len(rows)} 行（{span}）")
        return True
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_frozen: {exc}")
        return False


def after_weekly(ledger_dir: Path, echo: Callable[[str], Any]) -> bool:
    try:
        extend_weekly(ledger_dir)
        echo("forward_frozen: 周汇总加了「冻结的线」一节")
        return True
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_frozen: 周汇总没有加「冻结的线」一节：{exc}")
        return False


@app.callback()
def main() -> None:
    """Expose read-only controls for the frozen line."""


@app.command("guard")
def guard_command(
    daily_root: Annotated[Path, typer.Option(help="日报状态根目录（只读）。")],
    price_file: Annotated[str | None, typer.Option(help="价格捕获相对路径（只读）。")] = None,
    ledger_dir: Annotated[
        Path, typer.Option(help="前瞻账本目录（只读）。")
    ] = fl.DEFAULT_LEDGER_DIR,
) -> None:
    try:
        if price_file is None:
            rows = fl.read_ledger(ledger_dir)
            if not rows:
                raise FrozenLinesError("核对 A 不过：主账本没有行")
            price_file = rows[-1]["price_file"]
        guard(daily_root, price_file)
        typer.echo(f"forward_frozen: 核对 A 过，{price_file}")
        prices = fl.read_capture(daily_root, price_file)
        if prices is None or prices.empty:
            raise FrozenLinesError("核对 A 不过：价格文件为空")
        through = prices.index[-1].date()
        result = _read_walk(daily_root, price_file, through)
        for day, actions in sorted(result.actions.items()):
            if day > ANCHOR.isoformat():
                typer.echo(f"forward_frozen: {day} {_action_text(actions)}")
        typer.echo(f"forward_frozen: {through} {_state_text(result.state)}")
    except (FrozenLinesError, fl.ForwardLedgerError) as exc:
        typer.echo(f"forward_frozen: {exc}")
        raise typer.Exit(code=1) from None


if __name__ == "__main__":
    app()
