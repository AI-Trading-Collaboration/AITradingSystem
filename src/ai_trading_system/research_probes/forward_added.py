"""Added lines of the forward ledger: F0001 x 2 and the total-asset column (TRADING-2565-135).

Owner decision owner_decision:TRADING-2565:2026-11-01:research_loop_p7,
charter sections 3.3, 3.5 and 5.4. A second append-only file hangs on each same-day
main-ledger row. F0001 x 2 doubles the target, resets on the row where F0001 holds
its new target, and drifts between resets. Borrowing pays SGOV plus 1% per year;
idle cash earns zero and costs are zero. Continue from written rows, never recompute.
The total-asset midpoint is held at t+2 and printed only. The main ledger is never
modified or blocked: added-line failures are reported after its work is complete.
Accounting only: no judgment, daily-report integration, broker or production action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_probes import forward_ledger as fl
from ai_trading_system.trading_calendar import is_us_equity_trading_day

RUN_ID = "TRADING-2565-135"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-01:research_loop_p7"
SCHEMA = "forward_added.v1"
ADDED_FILE = "added_lines.jsonl"
# Owner-scoped accounting rules from charter sections 3.3, 3.5 and 5.4, not gates.
ADDED_START = date(2026, 11, 19)
FACTOR = 2.0
TARGETS = frozenset({0.0, 1.0, 2.0})
LEDGER_TARGETS = frozenset({0.0, 0.5, 1.0})
SPREAD = 0.01
TOTAL_KEY = "total_asset_ai_band"
WIPED = "净值归零"
SECTION = "## 加的线"
ROW_KEYS = frozenset(
    {
        "schema_version",
        "seq",
        "date",
        "ledger_seq",
        "ledger_sha256",
        "qqq_tr",
        "price_file",
        "target_overlay2",
        "exp_overlay2",
        "borrow_overlay2",
        "sgov_tr",
        "ret_overlay2",
        "total_min",
        "total_max",
        "total_snapshot_file",
        "exp_total_held",
        "ret_total",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }
)


class AddedLinesError(ValueError):
    """The added file or its inputs cannot be used; do not append anything."""


@dataclass(frozen=True)
class Total:
    min_position: float
    max_position: float
    file: str

    @property
    def mid(self) -> float:
        return (self.min_position + self.max_position) / 2.0


def previous_day(day: date) -> date:
    day -= timedelta(days=1)
    while not is_us_equity_trading_day(day):
        day -= timedelta(days=1)
    return day


def is_reset(day: date) -> bool:
    previous = previous_day(day)
    return previous.month != previous_day(previous).month


def ledger_target(row: Mapping[str, Any]) -> float:
    value = row.get("exp_overlay")
    if not fl._number(value) or value not in LEDGER_TARGETS:
        raise AddedLinesError(f"{row.get('date')}: F0001 调回行的 exp_overlay 不是 0、0.5 或 1")
    return FACTOR * value


def start_target(main_rows: Sequence[Mapping[str, Any]]) -> float:
    resets = [
        row
        for row in main_rows
        if date.fromisoformat(row["date"]) <= ADDED_START
        and is_reset(date.fromisoformat(row["date"]))
    ]
    if not resets:
        raise AddedLinesError(f"{ADDED_START} 当天或以前没有 F0001 调回行")
    return ledger_target(resets[-1])


def drift(previous: Mapping[str, Any]) -> float | None:
    w = previous["exp_overlay2"]
    if w is None:
        return None
    if w in (0.0, 1.0):
        return float(w)
    q, r = previous["qqq_tr"], previous["ret_overlay2"]
    if q is None or r is None:
        return None
    return float(w * (1.0 + q) / (1.0 + r))


def overlay2(
    w: float | None, q: float | None, s: float | None
) -> tuple[float | None, float | None, list[str]]:
    if w is None:
        return None, None, ["F0001 × 2 没有敞口"]
    borrow = max(w - 1.0, 0.0)
    if q is None:
        return borrow, None, ["没有 QQQ 当天的收益"]
    if borrow > 0 and s is None:
        return borrow, None, ["借钱时没有 SGOV 当天的收益"]
    interest = borrow * (s + SPREAD / fl.ANNUAL_DAYS) if borrow > 0 and s is not None else 0.0
    value = w * q - interest
    if 1.0 + value <= 0:
        return borrow, -1.0, [WIPED]
    return borrow, value, []


def held_snapshot(daily_root: Path, day: date) -> str | None:
    for earlier in reversed(fl.trading_days(fl.HELD_FROM, day)[:-2]):
        relative = fl.SNAPSHOT_REL.format(day=earlier.isoformat())
        if (daily_root / relative).is_file():
            return relative
    return None


def read_total(daily_root: Path, relative: str) -> tuple[Total | None, str | None]:
    try:
        snapshot = json.loads((daily_root / relative).read_text(encoding="utf-8"))
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        band = positions.get(TOTAL_KEY) if isinstance(positions, dict) else None
        if not isinstance(band, dict):
            raise ValueError(f"没有 {TOTAL_KEY} 区间")
        low, high = band.get("min_position"), band.get("max_position")
        if not fl._position(low) or not fl._position(high) or low > high:
            raise ValueError(f"{TOTAL_KEY} 不是合法的 0 到 1 区间")
        return Total(float(low), float(high), relative), None
    except (OSError, UnicodeError, ValueError, TypeError, OverflowError) as exc:
        return None, f"{relative}: 总资产区间读不到：{exc}"


def read_sgov(daily_root: Path, price_file: str, day: date) -> float | None:
    try:
        prices = fl.read_capture(daily_root, price_file, fl.CASH_TICKER)
        return None if prices is None else fl.day_return(prices, day)
    except fl.ForwardLedgerError:
        return None


def _row_bytes(row: Mapping[str, Any]) -> bytes:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def build_rows(
    main_rows: Sequence[Mapping[str, Any]],
    main_raw: Sequence[bytes],
    existing: Sequence[Mapping[str, Any]],
    daily_root: Path,
    *,
    provenance: Mapping[str, Any],
) -> list[dict[str, Any]]:
    last_seq = existing[-1]["ledger_seq"] if existing else 0
    pending = [
        (row, raw)
        for row, raw in zip(main_rows, main_raw, strict=True)
        if row["date"] >= ADDED_START.isoformat() and row["seq"] > last_seq
    ]
    if not pending:
        return []
    previous = existing[-1] if existing else None
    target = float(previous["target_overlay2"]) if previous is not None else start_target(main_rows)
    wiped = any(row["ret_overlay2"] == -1.0 for row in existing)
    rows: list[dict[str, Any]] = []
    for main, raw in pending:
        day = date.fromisoformat(main["date"])
        reset = is_reset(day)
        if reset:
            target = ledger_target(main)
        w = target if previous is None or reset else drift(previous) if not wiped else None
        s = read_sgov(daily_root, main["price_file"], day)
        if wiped:
            w, borrow, value, notes = None, None, None, [f"之前{WIPED}"]
        else:
            borrow, value, notes = overlay2(w, main["qqq_tr"], s)
        relative = held_snapshot(daily_root, day)
        total, reason = (
            (None, "没有可持有的总资产快照")
            if relative is None
            else read_total(daily_root, relative)
        )
        if reason is not None:
            notes.append(reason)
        midpoint = None if total is None else total.mid
        row = {
            "schema_version": SCHEMA,
            "seq": len(existing) + len(rows) + 1,
            "date": main["date"],
            "ledger_seq": main["seq"],
            "ledger_sha256": chain_digest(raw),
            "qqq_tr": main["qqq_tr"],
            "price_file": main["price_file"],
            "target_overlay2": target,
            "exp_overlay2": w,
            "borrow_overlay2": borrow,
            "sgov_tr": s,
            "ret_overlay2": value,
            "total_min": None if total is None else total.min_position,
            "total_max": None if total is None else total.max_position,
            "total_snapshot_file": relative,
            "exp_total_held": midpoint,
            "ret_total": fl._product(midpoint, main["qqq_tr"]),
            "notes": notes,
            "code_commit": provenance.get("commit"),
            "code_modified": provenance.get("code_modified"),
            "prev_sha256": None if previous is None else chain_digest(_row_bytes(previous)),
        }
        problem = _inline_problem(row, main, previous, target, wiped)
        if problem is not None:
            raise AddedLinesError(f"{ADDED_FILE} {day}: {problem}")
        rows.append(row)
        previous = row
        wiped = wiped or value == -1.0
    return rows


def _added_lines(ledger_dir: Path) -> list[bytes]:
    path = ledger_dir / ADDED_FILE
    try:
        return [line for line in path.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise AddedLinesError(f"{path}: {exc}") from exc


def _inline_problem(
    row: Mapping[str, Any],
    main: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
    first_target: float,
    wiped: bool,
) -> str | None:
    target = row["target_overlay2"]
    if not fl._number(target) or target not in TARGETS:
        return "target_overlay2 is not 0, 1 or 2"
    day = date.fromisoformat(row["date"])
    reset = is_reset(day)
    if previous is None:
        if target != first_target:
            return "target_overlay2 is not twice the target before ADDED_START"
    elif reset:
        if target != ledger_target(main):
            return "target_overlay2 is not twice the ledger's exp_overlay on a reset row"
    elif target != previous["target_overlay2"]:
        return "target_overlay2 changed off a reset row"
    for key in ("exp_overlay2", "borrow_overlay2", "sgov_tr", "ret_overlay2", "ret_total"):
        if row[key] is not None and not fl._number(row[key]):
            return f"{key} is not a finite number or None"
    w = row["exp_overlay2"]
    if w is not None and w < 0:
        return "exp_overlay2 is negative"
    if wiped:
        if any(row[key] is not None for key in ("exp_overlay2", "borrow_overlay2", "ret_overlay2")):
            return "rows after a wipe-out must be empty"
    else:
        expected = target if previous is None or reset else drift(previous)
        if not fl._equal(w, expected):
            return "exp_overlay2 does not follow the reset and drift rule"
        borrow, value, _ = overlay2(w, row["qqq_tr"], row["sgov_tr"])
        if not fl._equal(row["borrow_overlay2"], borrow):
            return "borrow_overlay2 does not equal max(exp_overlay2 - 1, 0)"
        if not fl._equal(row["ret_overlay2"], value):
            return "ret_overlay2 does not equal the borrowing-adjusted return"
    low, high = row["total_min"], row["total_max"]
    if low is None and high is None:
        if row["exp_total_held"] is not None or row["ret_total"] is not None:
            return "exp_total_held and ret_total must be None without a band"
    elif not fl._position(low) or not fl._position(high) or low > high:
        return "total_min/total_max is not a valid band"
    else:
        midpoint = (low + high) / 2.0
        if not fl._equal(row["exp_total_held"], midpoint):
            return "exp_total_held is not the band midpoint"
        if not fl._equal(row["ret_total"], fl._product(midpoint, row["qqq_tr"])):
            return "ret_total does not equal exp_total_held * qqq_tr"
    if not isinstance(row["notes"], list) or any(not isinstance(n, str) for n in row["notes"]):
        return "notes is not a list of strings"
    if not isinstance(row["code_commit"], str) or len(row["code_commit"]) != 40:
        return "code_commit is not a 40-character string"
    if row["code_modified"] is not False:
        return "code_modified is not False"
    relative = row["total_snapshot_file"]
    if relative is not None and not isinstance(relative, str):
        return "total_snapshot_file is not a string or None"
    return None


def verify(ledger_dir: Path) -> list[str]:
    prefix = f"{ADDED_FILE} line 1: "
    try:
        lines = _added_lines(ledger_dir)
        if not lines:
            return []
        main_rows = fl.read_ledger(ledger_dir)
        main_raw = fl._ledger_lines(ledger_dir)
        selected = [
            (row, raw)
            for row, raw in zip(main_rows, main_raw, strict=True)
            if row["date"] >= ADDED_START.isoformat()
        ]
        first_target = start_target(main_rows)
        previous = None
        digest = None
        wiped = False
        for position, raw in enumerate(lines, 1):
            prefix = f"{ADDED_FILE} line {position}: "
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeError):
                return [prefix + "not JSON"]
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
            main, main_line = selected[position - 1]
            if type(row["ledger_seq"]) is not int or row["ledger_seq"] != main["seq"]:
                return [prefix + "ledger_seq does not continue from ADDED_START"]
            if row["date"] != main["date"]:
                return [prefix + "date does not match the main ledger"]
            if row["ledger_sha256"] != chain_digest(main_line):
                return [prefix + "ledger_sha256 does not match the main ledger"]
            if (
                not fl._equal(row["qqq_tr"], main["qqq_tr"])
                or row["price_file"] != main["price_file"]
            ):
                return [prefix + "qqq_tr or price_file differs from the main ledger"]
            problem = _inline_problem(row, main, previous, first_target, wiped)
            if problem is not None:
                return [prefix + problem]
            wiped = wiped or row["ret_overlay2"] == -1.0
            previous, digest = row, chain_digest(raw)
        return []
    except (ValueError, TypeError, KeyError, ArithmeticError) as exc:
        return [prefix + str(exc)]


def read_added(ledger_dir: Path) -> list[dict[str, Any]]:
    problems = verify(ledger_dir)
    if problems:
        raise AddedLinesError(problems[0])
    return [json.loads(raw) for raw in _added_lines(ledger_dir)]


def append(
    ledger_dir: Path, daily_root: Path, *, provenance: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if provenance.get("code_modified") is not False:
        raise AddedLinesError(f"{ADDED_FILE}: code_modified is not False")
    commit = provenance.get("commit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise AddedLinesError(f"{ADDED_FILE}: commit is not a 40-character string")
    existing = read_added(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    try:
        rows = build_rows(
            main_rows, fl._ledger_lines(ledger_dir), existing, daily_root, provenance=provenance
        )
        if rows:
            path = ledger_dir / ADDED_FILE
            needs_separator = path.exists() and path.stat().st_size > 0
            if needs_separator:
                with path.open("rb") as source:
                    source.seek(-1, 2)
                    needs_separator = source.read(1) != b"\n"
            payload = (b"\n" if needs_separator else b"") + b"".join(
                _row_bytes(row) + b"\n" for row in rows
            )
            ledger_dir.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as sink:
                sink.write(payload)
    except (OSError, ArithmeticError) as exc:
        raise AddedLinesError(f"{ADDED_FILE}: {exc}") from exc
    return rows


def check_day(ledger_dir: Path, daily_root: Path, day: date) -> list[str]:
    row = next((r for r in read_added(ledger_dir) if r["date"] == day.isoformat()), None)
    if row is None:
        return []
    prefix = f"{ADDED_FILE} {day}: "
    problems = []
    if not fl._equal(row["sgov_tr"], read_sgov(daily_root, row["price_file"], day)):
        problems.append(prefix + "sgov_tr does not match the price file")
    relative = held_snapshot(daily_root, day)
    if relative != row["total_snapshot_file"]:
        problems.append(prefix + "total_snapshot_file is not the held snapshot")
    total = None if relative is None else read_total(daily_root, relative)[0]
    low, high = (None, None) if total is None else (total.min_position, total.max_position)
    if not fl._equal(row["total_min"], low) or not fl._equal(row["total_max"], high):
        problems.append(prefix + "total band does not match the held snapshot")
    main = next(r for r in fl.read_ledger(ledger_dir) if r["seq"] == row["ledger_seq"])
    band = None
    if relative is not None:
        for earlier in reversed(fl.trading_days(fl.HELD_FROM, day)[:-2]):
            if relative == fl.SNAPSHOT_REL.format(day=earlier.isoformat()):
                band = fl.read_band(daily_root, earlier)
                break
    if not fl._equal(None if band is None else band.mid, main["exp_daily_held"]):
        problems.append(prefix + "final band midpoint is not exp_daily_held")
    return problems


def _log_difference(
    added: Sequence[Mapping[str, Any]], main: Mapping[int, Mapping[str, Any]]
) -> str:
    wiped = next((row["date"] for row in added if row["ret_overlay2"] == -1.0), None)
    if wiped is not None:
        return f"{wiped} {WIPED}，对买入持有和对 2 × 同期 F0001 的累计对数差不印。"
    pairs = [
        (row["ret_overlay2"], row["qqq_tr"], FACTOR * main[row["ledger_seq"]]["ret_overlay"])
        for row in added
        if row["ret_overlay2"] is not None
        and row["qqq_tr"] is not None
        and main[row["ledger_seq"]]["ret_overlay"] is not None
    ]
    if not pairs or any(value <= -1 for pair in pairs for value in pair):
        return "没有可取对数的同期收益，对买入持有和对 2 × 同期 F0001 的累计对数差不印。"
    own, hold, double = (sum(math.log1p(pair[k]) for pair in pairs) for k in range(3))
    return (
        f"对买入持有的累计对数差 {(own - hold) * 100:+.2f} 点；"
        f"对 2 × 同期 F0001 {(own - double) * 100:+.2f} 点（{len(pairs)} 个交易日）。"
    )


def render_section(
    main_rows: Sequence[Mapping[str, Any]], added: Sequence[Mapping[str, Any]]
) -> list[str]:
    lines = [
        "",
        f"{SECTION}（自 {ADDED_START}，只记账，不判）",
        "",
        f"依据 `{OWNER_DECISION}` 第 3.3、3.5、5.4 节。F0001 × 2 目标为 F0001 的 2 倍，"
        "在主账本回到目标的那一行调回，中间随价格漂；借的部分付同一价格文件里的 SGOV"
        "当天收益加 1%/年（252 天），空仓记 0，不扣成本，接着已写的行算。"
        "日报总资产区间 t+2 持有，只印；主账本、判的口径、日报和券商都不改。",
        "",
    ]
    main = {row["seq"]: row for row in main_rows}
    lag = sum(row["date"] >= ADDED_START.isoformat() for row in main_rows) - len(added)
    if not added:
        return [*lines, "还没有行。", "", f"加的线落后主账本 {lag} 行。"]
    matched = [main[row["ledger_seq"]] for row in added]
    lines += [
        "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |",
        "|---|---|---|---|---|---|",
    ]
    series = (
        ("F0001 × 2", added, "ret_overlay2", "exp_overlay2"),
        ("买入持有", matched, "qqq_tr", None),
        ("F0001（主账本）", matched, "ret_overlay", "exp_overlay"),
        ("日报总资产口径", added, "ret_total", "exp_total_held"),
        ("日报模型（主账本）", matched, "ret_daily", "exp_daily_held"),
    )
    for label, rows, return_key, exposure_key in series:
        summary = fl._summary(
            [row[return_key] for row in rows],
            [1.0 if exposure_key is None else row[exposure_key] for row in rows],
        )
        lines.append(
            f"| {label} | {summary['days']} | {summary['missing']} | "
            f"{fl._pct(summary['cumulative'])} | {fl._pct(summary['max_drawdown'])} | "
            f"{fl._exp(summary['mean_exposure'])} |"
        )
    borrows = [row["borrow_overlay2"] for row in added]
    weights = [row["exp_overlay2"] for row in added if row["exp_overlay2"] is not None]
    interest_rows = [
        row for row in added if row["borrow_overlay2"] is not None and row["borrow_overlay2"] > 0
    ]
    interest = sum(
        row["borrow_overlay2"] * (row["sgov_tr"] + SPREAD / fl.ANNUAL_DAYS)
        for row in interest_rows
        if row["sgov_tr"] is not None
    )
    interest_missing = sum(row["sgov_tr"] is None for row in interest_rows)
    lines += [
        "",
        f"平均借的比例 {fl._exp(fl._mean(borrows))}；"
        f"期间最大的 w {fl._exp(max(weights) if weights else None)}；"
        f"累计付的利息 {interest * 100:.2f} 点（缺 SGOV {interest_missing} 天）。",
        _log_difference(added, main),
    ]
    cash = [row["sgov_tr"] for row in added if row["sgov_tr"] is not None]
    annual = (
        math.expm1(sum(math.log1p(s) for s in cash) * fl.ANNUAL_DAYS / len(cash))
        if cash and all(s > -1 for s in cash)
        else None
    )
    idle: list[float | None] = []
    for row in added:
        w, r, s = row["exp_overlay2"], row["ret_overlay2"], row["sgov_tr"]
        if w is None or r is None or (w < 1 and s is None):
            idle.append(None)
        else:
            idle.append(r + max(1 - w, 0) * s if s is not None else r)
    idle_summary = fl._summary(idle, [row["exp_overlay2"] for row in added])
    # Charter 5.4's descriptive SGOV flag, never an acceptance or readiness threshold.
    odd = [
        row["date"]
        for row in interest_rows
        if row["sgov_tr"] is not None and (row["sgov_tr"] < 0 or row["sgov_tr"] > 0.001)
    ]
    lines += [
        f"期间 SGOV 年化 {fl._pct(annual)}"
        f"（有收益 {len(cash)} 天，缺 {len(added) - len(cash)} 天）。",
        f"空仓那部分拿 SGOV 时（只印）：F0001 × 2 累计 {fl._pct(idle_summary['cumulative'])}"
        f"（缺收益 {idle_summary['missing']} 天）。",
        "借钱的日子里 SGOV 收益小于 0 或大于 0.1% 的："
        + ("、".join(odd) if odd else "没有")
        + "。",
        "",
        f"加的线落后主账本 {lag} 行。",
    ]
    return lines


def extend_weekly(ledger_dir: Path) -> None:
    added = read_added(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    path = ledger_dir / fl.WEEKLY_FILE
    footer = "\nproduction_effect=none；broker_action=none。\n"
    try:
        text = path.read_text(encoding="utf-8")
        if not text.endswith(footer):
            raise AddedLinesError(f"{fl.WEEKLY_FILE}: 页脚不符")
        if SECTION in text:
            raise AddedLinesError(f"{fl.WEEKLY_FILE}: 已有「加的线」一节")
        section = "\n".join(render_section(main_rows, added)) + "\n"
        path.write_text(text[: -len(footer)] + section + footer, encoding="utf-8", newline="\n")
    except (OSError, UnicodeError, ArithmeticError) as exc:
        raise AddedLinesError(f"{path}: {exc}") from exc


def after_append(
    ledger_dir: Path,
    daily_root: Path,
    provenance: Mapping[str, Any],
    echo: Callable[[str], Any],
) -> bool:
    try:
        rows = append(ledger_dir, daily_root, provenance=provenance)
        total = len(read_added(ledger_dir))
        for row in rows:
            notes = "（" + "；".join(row["notes"]) + "）" if row["notes"] else ""
            echo(
                f"forward_added: {row['date']} F0001×2 {fl._exp(row['exp_overlay2'], '空')} "
                f"收益 {fl._pct(row['ret_overlay2'], '空')} "
                f"总资产 {fl._exp(row['exp_total_held'], '空')}{notes}"
            )
        echo(f"forward_added: 追加 {len(rows)} 行，共 {total} 行")
        return True
    except (AddedLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_added: 加的线没有写（主账本照写）：{exc}")
        return False


def after_check(
    ledger_dir: Path, day: date | None, daily_root: Path | None, echo: Callable[[str], Any]
) -> bool:
    try:
        problems = verify(ledger_dir)
        if not problems and day is not None and daily_root is not None:
            problems = check_day(ledger_dir, daily_root, day)
        if problems:
            for problem in problems:
                echo(f"forward_added: {problem}")
            return False
        rows = read_added(ledger_dir)
        span = f"{rows[0]['date']}..{rows[-1]['date']}" if rows else "空"
        echo(f"forward_added: 加的线完好，{len(rows)} 行（{span}）")
        return True
    except (AddedLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_added: {exc}")
        return False


def after_weekly(ledger_dir: Path, echo: Callable[[str], Any]) -> bool:
    try:
        extend_weekly(ledger_dir)
        echo("forward_added: 周汇总加了「加的线」一节")
        return True
    except (AddedLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_added: 周汇总没有加「加的线」一节：{exc}")
        return False
