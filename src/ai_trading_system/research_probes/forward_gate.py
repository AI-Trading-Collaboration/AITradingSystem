"""Gate-hint line of the forward ledger (TRADING-2565-142).

Owner decision owner_decision:TRADING-2565:2026-11-27:research_loop_p8,
charter sections 3.4 and 5.1. Daily snapshots use full exposure by default from
as_of 2026-11-20; the old-rule band is stored in gated_risk_asset_ai_band.
This line has its own append-only file, linked to each same-day main-ledger row.
From 2026-11-24 it holds the snapshot's gated midpoint at t+2. Cash earns zero;
costs are zero. The main ledger is never modified or blocked. Accounting only:
no judgment, daily-report integration, broker or production action.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.research_bench.ledger import chain_digest
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_probes import forward_ledger as fl

RUN_ID = "TRADING-2565-142"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-27:research_loop_p8"
SCHEMA = "forward_gate.v1"
GATE_FILE = "gate_lines.jsonl"
# First main-ledger row holding the as_of 2026-11-20 full-exposure snapshot at t+2.
GATE_START = date(2026, 11, 24)
# First snapshot after the daily policy switch, as scoped by charter sections 3.4/5.1.
GATE_HELD_FROM = date(2026, 11, 20)
GATE_KEY = "gated_risk_asset_ai_band"
GATE_TOTAL_KEY = "gated_total_asset_ai_band"
SECTION = "## 闸门线（提示）"
# Formatting only: cap the number of printed band changes, not the recorded history.
MAX_PRINTED_CHANGES = 10
ROW_KEYS = frozenset(
    {
        "schema_version",
        "seq",
        "date",
        "ledger_seq",
        "ledger_sha256",
        "qqq_tr",
        "price_file",
        "gate_snapshot_file",
        "policy_mode",
        "gate_min",
        "gate_max",
        "gate_label",
        "exp_gate_held",
        "ret_gate",
        "gate_total_min",
        "gate_total_max",
        "exp_gate_total_held",
        "ret_gate_total",
        "sgov_tr",
        "notes",
        "code_commit",
        "code_modified",
        "prev_sha256",
    }
)
BAND_GROUPS = (
    ("gate_min", "gate_max", "exp_gate_held", "ret_gate"),
    ("gate_total_min", "gate_total_max", "exp_gate_total_held", "ret_gate_total"),
)


class GateLinesError(ValueError):
    """The gate file or its inputs cannot be used; do not append anything."""


def held_snapshot(daily_root: Path, day: date) -> str | None:
    for earlier in reversed(fl.trading_days(GATE_HELD_FROM, day)[:-2]):
        relative = fl.SNAPSHOT_REL.format(day=earlier.isoformat())
        if (daily_root / relative).is_file():
            return relative
    return None


def _snapshot_day(relative: str) -> date | None:
    prefix, suffix = fl.SNAPSHOT_REL.split("{day}")
    if not relative.startswith(prefix) or not relative.endswith(suffix):
        return None
    text = relative[len(prefix) : -len(suffix)] if suffix else relative[len(prefix) :]
    try:
        day = date.fromisoformat(text)
    except ValueError:
        return None
    return day if relative == fl.SNAPSHOT_REL.format(day=day.isoformat()) else None


def read_snapshot(daily_root: Path, relative: str | None) -> dict[str, Any]:
    values: dict[str, Any] = {
        "policy_mode": None,
        "gate_min": None,
        "gate_max": None,
        "gate_label": None,
        "gate_total_min": None,
        "gate_total_max": None,
        "notes": [],
    }
    if relative is None:
        values["notes"].append("没有可持有的闸门快照")
        return values
    try:
        snapshot = json.loads((daily_root / relative).read_text(encoding="utf-8"))
        positions = snapshot.get("positions") if isinstance(snapshot, dict) else None
        if not isinstance(positions, dict):
            raise ValueError("positions 不是对象")
    except (OSError, UnicodeError, ValueError, TypeError, OverflowError) as exc:
        values["notes"].append(f"{relative}: 快照读不到：{exc}")
        return values
    policy = positions.get("position_policy")
    mode = policy.get("mode") if isinstance(policy, dict) else None
    values["policy_mode"] = mode if isinstance(mode, str) else None
    for key, low_key, high_key in (
        (GATE_KEY, "gate_min", "gate_max"),
        (GATE_TOTAL_KEY, "gate_total_min", "gate_total_max"),
    ):
        band = positions.get(key)
        low = band.get("min_position") if isinstance(band, dict) else None
        high = band.get("max_position") if isinstance(band, dict) else None
        if not fl._position(low) or not fl._position(high) or low > high:
            values["notes"].append(f"{relative}: {key} 缺失或不是合法的 0 到 1 区间")
            continue
        values[low_key], values[high_key] = float(low), float(high)
        if key == GATE_KEY and isinstance(band, dict):
            label = band.get("label")
            values["gate_label"] = label if isinstance(label, str) else None
    return values


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


def _gate_lines(ledger_dir: Path) -> list[bytes]:
    path = ledger_dir / GATE_FILE
    try:
        return [line for line in path.read_bytes().replace(b"\r\n", b"\n").split(b"\n") if line]
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise GateLinesError(f"{path}: {exc}") from exc


def _inline_problem(row: Mapping[str, Any]) -> str | None:
    relative = row["gate_snapshot_file"]
    day = date.fromisoformat(row["date"])
    if relative is not None:
        if not isinstance(relative, str):
            return "gate_snapshot_file 不是字符串或空"
        held = _snapshot_day(relative)
        if held is None or held not in fl.trading_days(GATE_HELD_FROM, day)[:-2]:
            return "gate_snapshot_file 不在可持有的 t+2 快照窗口里"
    elif any(
        row[key] is not None
        for key in ("policy_mode", "gate_label", *(key for group in BAND_GROUPS for key in group))
    ):
        return "没有快照时仓位方式和区间各列必须为空"
    for low_key, high_key, mid_key, return_key in BAND_GROUPS:
        low, high = row[low_key], row[high_key]
        if low is None and high is None:
            if row[mid_key] is not None or row[return_key] is not None:
                return f"没有区间时 {mid_key} 和 {return_key} 必须为空"
            if low_key == "gate_min" and row["gate_label"] is not None:
                return "没有闸门区间时 gate_label 必须为空"
        elif not fl._position(low) or not fl._position(high) or low > high:
            return f"{low_key}/{high_key} 不是合法区间"
        else:
            midpoint = (low + high) / 2.0
            if not fl._equal(row[mid_key], midpoint):
                return f"{mid_key} 不等于区间中点"
            if not fl._equal(row[return_key], fl._product(midpoint, row["qqq_tr"])):
                return f"{return_key} 不等于区间中点乘 QQQ 收益"
    for key in ("gate_label", "policy_mode"):
        if row[key] is not None and not isinstance(row[key], str):
            return f"{key} 不是字符串或空"
    if row["sgov_tr"] is not None and not fl._number(row["sgov_tr"]):
        return "sgov_tr 不是有限数或空"
    notes = row["notes"]
    if not isinstance(notes, list) or any(not isinstance(note, str) for note in notes):
        return "notes 不是字符串列表"
    if row["gate_min"] is None and not notes:
        return "闸门区间空时 notes 不能为空"
    if not isinstance(row["code_commit"], str) or len(row["code_commit"]) != 40:
        return "code_commit 不是 40 个字符的字符串"
    if row["code_modified"] is not False:
        return "code_modified 不是 False"
    return None


def verify(ledger_dir: Path) -> list[str]:
    prefix = f"{GATE_FILE} line 1: "
    try:
        lines = _gate_lines(ledger_dir)
        if not lines:
            return []
        main_rows = fl.read_ledger(ledger_dir)
        selected = [
            (row, raw)
            for row, raw in zip(main_rows, fl._ledger_lines(ledger_dir), strict=True)
            if row["date"] >= GATE_START.isoformat()
        ]
        digest = None
        for position, raw in enumerate(lines, 1):
            prefix = f"{GATE_FILE} line {position}: "
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeError):
                return [prefix + "不是 JSON"]
            if not isinstance(row, dict) or row.keys() != ROW_KEYS:
                return [prefix + "键不等于 ROW_KEYS"]
            if row["schema_version"] != SCHEMA:
                return [prefix + "schema_version 不符"]
            if type(row["seq"]) is not int or row["seq"] != position:
                return [prefix + f"seq 不等于 {position}"]
            if row["prev_sha256"] != digest:
                return [prefix + "prev_sha256 不符"]
            if position > len(selected):
                return [prefix + "主账本没有对应行"]
            main, main_raw = selected[position - 1]
            if type(row["ledger_seq"]) is not int or row["ledger_seq"] != main["seq"]:
                return [prefix + "ledger_seq 没有从 GATE_START 起连续挂主账本"]
            if row["date"] != main["date"]:
                return [prefix + "date 与主账本不符"]
            if row["ledger_sha256"] != chain_digest(main_raw):
                return [prefix + "ledger_sha256 与主账本不符"]
            if not fl._equal(row["qqq_tr"], main["qqq_tr"]):
                return [prefix + "qqq_tr 与主账本不符"]
            if row["price_file"] != main["price_file"]:
                return [prefix + "price_file 与主账本不符"]
            problem = _inline_problem(row)
            if problem is not None:
                return [prefix + problem]
            digest = chain_digest(raw)
        return []
    except (OSError, ValueError, TypeError, KeyError, ArithmeticError) as exc:
        return [prefix + str(exc)]


def read_gate(ledger_dir: Path) -> list[dict[str, Any]]:
    problems = verify(ledger_dir)
    if problems:
        raise GateLinesError(problems[0])
    return [json.loads(raw) for raw in _gate_lines(ledger_dir)]


def build_rows(
    main_rows: Sequence[Mapping[str, Any]],
    main_raw: Sequence[bytes],
    existing: Sequence[Mapping[str, Any]],
    daily_root: Path,
    *,
    provenance: Mapping[str, Any],
) -> list[dict[str, Any]]:
    last_seq = existing[-1]["ledger_seq"] if existing else 0
    previous = existing[-1] if existing else None
    rows: list[dict[str, Any]] = []
    for main, raw in zip(main_rows, main_raw, strict=True):
        if main["date"] < GATE_START.isoformat() or main["seq"] <= last_seq:
            continue
        day = date.fromisoformat(main["date"])
        relative = held_snapshot(daily_root, day)
        values = read_snapshot(daily_root, relative)
        row = {
            "schema_version": SCHEMA,
            "seq": len(existing) + len(rows) + 1,
            "date": main["date"],
            "ledger_seq": main["seq"],
            "ledger_sha256": chain_digest(raw),
            "qqq_tr": main["qqq_tr"],
            "price_file": main["price_file"],
            "gate_snapshot_file": relative,
            **values,
            "sgov_tr": read_sgov(daily_root, main["price_file"], day),
            "code_commit": provenance.get("commit"),
            "code_modified": provenance.get("code_modified"),
            "prev_sha256": None if previous is None else chain_digest(_row_bytes(previous)),
        }
        for low_key, high_key, mid_key, return_key in BAND_GROUPS:
            low, high = row[low_key], row[high_key]
            midpoint = None if low is None else (low + high) / 2.0
            row[mid_key] = midpoint
            row[return_key] = fl._product(midpoint, main["qqq_tr"])
        problem = _inline_problem(row)
        if problem is not None:
            raise GateLinesError(f"{GATE_FILE} {day}: {problem}")
        rows.append(row)
        previous = row
    return rows


def append(
    ledger_dir: Path, daily_root: Path, *, provenance: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if provenance.get("code_modified") is not False:
        raise GateLinesError(f"{GATE_FILE}: code_modified 不是 False")
    commit = provenance.get("commit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise GateLinesError(f"{GATE_FILE}: commit 不是 40 个字符的字符串")
    existing = read_gate(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    try:
        rows = build_rows(
            main_rows, fl._ledger_lines(ledger_dir), existing, daily_root, provenance=provenance
        )
        if rows:
            path = ledger_dir / GATE_FILE
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
        raise GateLinesError(f"{GATE_FILE}: {exc}") from exc
    return rows


def check_day(ledger_dir: Path, daily_root: Path, day: date) -> list[str]:
    row = next((r for r in read_gate(ledger_dir) if r["date"] == day.isoformat()), None)
    if row is None:
        return []
    prefix = f"{GATE_FILE} {day}: "
    problems = []
    relative = held_snapshot(daily_root, day)
    if relative != row["gate_snapshot_file"]:
        problems.append(prefix + "gate_snapshot_file 不是持有的快照")
    values = read_snapshot(daily_root, relative)
    for key, value in values.items():
        if key == "notes":
            continue
        equal = (
            row[key] == value
            if key in ("gate_label", "policy_mode")
            else fl._equal(row[key], value)
        )
        if not equal:
            problems.append(prefix + f"{key} 与持有快照不符")
    if not fl._equal(row["sgov_tr"], read_sgov(daily_root, row["price_file"], day)):
        problems.append(prefix + "sgov_tr 与价格文件不符")
    if row["gate_snapshot_file"] is not None:
        held = _snapshot_day(row["gate_snapshot_file"])
        try:
            band = None if held is None else fl.read_band(daily_root, held)
        except fl.ForwardLedgerError as exc:
            problems.append(prefix + f"最终区间读不到：{exc}")
        else:
            main = next(r for r in fl.read_ledger(ledger_dir) if r["seq"] == row["ledger_seq"])
            if not fl._equal(None if band is None else band.mid, main["exp_daily_held"]):
                problems.append(prefix + "最终区间中点不等于主账本 exp_daily_held")
    return problems


def _log_difference(gate: Sequence[Mapping[str, Any]], mean: float | None) -> str:
    pairs: list[tuple[float, float, float]] = []
    if mean is not None:
        pairs = [
            (float(row["ret_gate"]), mean * float(row["qqq_tr"]), float(row["qqq_tr"]))
            for row in gate
            if row["ret_gate"] is not None and row["qqq_tr"] is not None
        ]
    if not pairs or any(value <= -1 for pair in pairs for value in pair):
        return "没有可取对数的同期收益，闸门线的累计对数差不印。"
    own, fixed, hold = (sum(math.log1p(pair[k]) for pair in pairs) for k in range(3))
    text = (
        f"闸门线对同平均敞口固定的累计对数差 {(own - fixed) * 100:+.2f} 点，"
        f"对买入持有 {(own - hold) * 100:+.2f} 点（{len(pairs)} 个交易日）。"
    )
    if len(pairs) < fl.ANNUAL_DAYS:
        return text + "不满一年，不印年化。"
    first = date.fromisoformat(gate[0]["date"])
    prior = fl._folder_day(first - timedelta(days=1))
    years = (date.fromisoformat(gate[-1]["date"]) - prior).days / DAYS_PER_YEAR
    fixed_diff = (math.exp(own / years) - math.exp(fixed / years)) * 100
    hold_diff = (math.exp(own / years) - math.exp(hold / years)) * 100
    return text + (
        f"几何年化差：对同平均敞口固定 {fixed_diff:+.2f} 点/年，对买入持有 {hold_diff:+.2f} 点/年。"
    )


def _cash_return(row: Mapping[str, Any], exposure: float | None) -> float | None:
    q, s = row["qqq_tr"], row["sgov_tr"]
    if exposure is None or q is None or s is None:
        return None
    return float(exposure * q + (1 - exposure) * s)


def _mode_line(gate: Sequence[Mapping[str, Any]]) -> str:
    modes: dict[str, int] = {}
    no_snapshot, no_mode = 0, 0
    for row in gate:
        mode = row["policy_mode"]
        if row["gate_snapshot_file"] is None:
            no_snapshot += 1
        elif mode:
            modes[mode] = modes.get(mode, 0) + 1
        else:
            no_mode += 1
    labels = [f"{mode} {count} 行" for mode, count in modes.items()]
    if no_snapshot:
        labels.append(f"没有快照 {no_snapshot} 行")
    if no_mode:
        labels.append(f"快照里没有 {no_mode} 行")
    return "持有快照的仓位方式：" + "；".join(labels) + "。"


def _changes_line(gate: Sequence[Mapping[str, Any]]) -> str:
    changes: list[str] = []
    previous = None
    for row in gate:
        if row["gate_min"] is None:
            continue
        band = (row["gate_min"], row["gate_max"])
        if previous is not None and band != previous:
            label = row["gate_label"] if row["gate_label"] is not None else "空"
            changes.append(
                f"{row['date']} {fl._exp(previous[0])}–{fl._exp(previous[1])} → "
                f"{fl._exp(band[0])}–{fl._exp(band[1])}（{label}）"
            )
        previous = band
    if not changes:
        return "闸门区间变了的日子：没有。"
    tail = f"，印最近 {MAX_PRINTED_CHANGES} 次" if len(changes) > MAX_PRINTED_CHANGES else ""
    return (
        f"闸门区间变了的日子（共 {len(changes)} 次{tail}）："
        + "；".join(changes[-MAX_PRINTED_CHANGES:])
        + "。"
    )


def render_section(
    main_rows: Sequence[Mapping[str, Any]], gate: Sequence[Mapping[str, Any]]
) -> list[str]:
    lines = [
        "",
        f"{SECTION}（自 {GATE_START}，只记账，不判）",
        "",
        f"依据 `{OWNER_DECISION}` 第 3.4、5.1 节；日报自 as_of {GATE_HELD_FROM} 起按 "
        f"`{fl.POLICY_DECISION}` 满仓为底。闸门线 t+2 持有快照里的 `{GATE_KEY}` 中点，"
        "现金 0，不扣成本；挂主账本同一天的行，自己只追加。总资产口径只印；"
        "主账本、判的口径、日报、券商和生产都不改。",
        "",
    ]
    lag = sum(row["date"] >= GATE_START.isoformat() for row in main_rows) - len(gate)
    if not gate:
        return [
            *lines,
            f"闸门线自 {GATE_START} 起记，还没有行。",
            "",
            f"闸门线落后主账本 {lag} 行。",
        ]
    main = {row["seq"]: row for row in main_rows}
    matched_main = [main[row["ledger_seq"]] for row in gate]
    present = [row for row in gate if row["ret_gate"] is not None]
    mean = fl._mean([row["exp_gate_held"] for row in present])
    fixed = [{"ret": fl._product(mean, row["qqq_tr"]), "exp": mean} for row in present]
    lines += [
        "| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |",
        "|---|---|---|---|---|---|",
    ]
    series = (
        ("闸门线（提示）", gate, "ret_gate", "exp_gate_held"),
        ("同平均敞口的固定（闸门线的对照）", fixed, "ret", "exp"),
        ("买入持有", matched_main, "qqq_tr", None),
        ("日报线（主账本，满仓为底）", matched_main, "ret_daily", "exp_daily_held"),
        ("固定 0.8（主账本）", matched_main, "ret_fixed", "exp_fixed"),
        ("F0001（主账本）", matched_main, "ret_overlay", "exp_overlay"),
        ("闸门总资产口径（只印）", gate, "ret_gate_total", "exp_gate_total_held"),
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
    own_cash = fl._summary(
        [_cash_return(row, row["exp_gate_held"]) for row in gate],
        [row["exp_gate_held"] for row in gate],
    )
    fixed_cash = fl._summary([_cash_return(row, mean) for row in present], [mean] * len(present))
    missing = sum(row["sgov_tr"] is None for row in gate)
    lines += [
        "",
        _log_difference(gate, mean),
        f"现金按同一价格文件里的 SGOV（只印）：闸门线累计 {fl._pct(own_cash['cumulative'])}，"
        f"同平均敞口的固定 {fl._pct(fixed_cash['cumulative'])}（缺 SGOV {missing} 天）。",
        _mode_line(gate),
        _changes_line(gate),
        "",
        f"闸门线落后主账本 {lag} 行。",
    ]
    return lines


def extend_weekly(ledger_dir: Path) -> None:
    gate = read_gate(ledger_dir)
    main_rows = fl.read_ledger(ledger_dir)
    path = ledger_dir / fl.WEEKLY_FILE
    footer = "\nproduction_effect=none；broker_action=none。\n"
    try:
        text = path.read_text(encoding="utf-8")
        if not text.endswith(footer):
            raise GateLinesError(f"{fl.WEEKLY_FILE}: 页脚不符")
        if SECTION in text:
            raise GateLinesError(f"{fl.WEEKLY_FILE}: 已有「闸门线（提示）」一节")
        section = "\n".join(render_section(main_rows, gate)) + "\n"
        path.write_text(text[: -len(footer)] + section + footer, encoding="utf-8", newline="\n")
    except (OSError, UnicodeError, ArithmeticError) as exc:
        raise GateLinesError(f"{path}: {exc}") from exc


def after_append(
    ledger_dir: Path,
    daily_root: Path,
    provenance: Mapping[str, Any],
    echo: Callable[[str], Any],
) -> bool:
    try:
        rows = append(ledger_dir, daily_root, provenance=provenance)
        if not (ledger_dir / GATE_FILE).exists() and not any(
            row["date"] >= GATE_START.isoformat() for row in fl.read_ledger(ledger_dir)
        ):
            echo(f"forward_gate: 闸门线自 {GATE_START} 起记，还没有行")
            return True
        total = len(read_gate(ledger_dir))
        for row in rows:
            notes = "（" + "；".join(row["notes"]) + "）" if row["notes"] else ""
            relative = row["gate_snapshot_file"]
            held = None if relative is None else _snapshot_day(relative)
            label = row["gate_label"] if row["gate_label"] is not None else "空"
            echo(
                f"forward_gate: {row['date']} 闸门 {fl._exp(row['exp_gate_held'], '空')}"
                f"（{fl._exp(row['gate_min'], '空')}–{fl._exp(row['gate_max'], '空')} {label}） "
                f"收益 {fl._pct(row['ret_gate'], '空')} 快照 as_of "
                f"{held if held is not None else '空'}{notes}"
            )
        echo(f"forward_gate: 追加 {len(rows)} 行，共 {total} 行")
        return True
    except (GateLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_gate: 闸门线没有写（主账本照写）：{exc}")
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
                echo(f"forward_gate: {problem}")
            return False
        rows = read_gate(ledger_dir)
        span = f"{rows[0]['date']}..{rows[-1]['date']}" if rows else "空"
        echo(f"forward_gate: 闸门线完好，{len(rows)} 行（{span}）")
        return True
    except (GateLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_gate: {exc}")
        return False


def after_weekly(ledger_dir: Path, echo: Callable[[str], Any]) -> bool:
    try:
        extend_weekly(ledger_dir)
        echo("forward_gate: 周汇总加了「闸门线（提示）」一节")
        return True
    except (GateLinesError, fl.ForwardLedgerError) as exc:
        echo(f"forward_gate: 周汇总没有加「闸门线（提示）」一节：{exc}")
        return False
