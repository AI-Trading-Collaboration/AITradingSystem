"""大权重股财报后第一天和 QQQ 最差的日子（TRADING-2565-99）。

依据 owner_decision:TRADING-2565:2026-10-30:earnings_shock_table、第 6 期章程第 24 节。
名单按每年九月底持仓定，事件日是 8-K 接收后第一个收盘严格较晚的交易日；三条线是
集中倍数至少 2、季内打乱 p 不超过 0.05、收益差的 Welch 区间下沿不高于 0。
只印，不是探针、不是试验、不进账本，不模拟躲开财报日的仓位线，不是长牛代价的读数。
check 只数次数和分布，不碰收益。
"""

from __future__ import annotations

import html
import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_data import earnings_events as ee
from ai_trading_system.research_data import qqq_long_history as qlh

# 章程第 24 节和需求第 2–4 节：读取前固定的身份、窗口、敏感度及判定线。
RUN_ID = "TRADING-2565-99"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-30:earnings_shock_table"
EARNINGS_SNAPSHOT_ID = "ern-a9917351bced"
QQQ_SNAPSHOT_ID = "qlh-20261013-0b0df53cb1a2"
QQQ_COLUMN = "etf_reference_return"
FIRST_DAY, LAST_DAY = pd.Timestamp("2004-01-01"), pd.Timestamp("2010-12-31")
EASTERN = "America/New_York"
# 本表的交易时钟固定为美东常规交易时段，半天交易日的局限在报告列出。
CLOSE_SECONDS, OPEN_SECONDS = 16 * 3600, 9 * 3600 + 30 * 60
TOP, SENSITIVE_TOPS = 5, (3, 10)
BAD_SHARE, SENSITIVE_BAD = 0.05, (0.025, 0.1)
Z, WELCH_Z = 1.959963984540054, 1.96
DRAWS, SEED = 1000, 20261030
SHIFTS, SHIFT_NOTE = (-5, -1, 1, 5), 0.3
CRASH_STOCK, CRASH_QQQ, HORIZONS, MIN_CRASHES = -0.08, -0.01, (5, 21, 63), 5
LINE_C, P_LINE = 2.0, 0.05
TOO_FEW_EVENTS, TOO_FEW_EXPECTED = 80, 4
ISSUERS: dict[str, tuple[int, str | None]] = {
    "MSFT": (789019, "MSFT"),
    "INTC": (50863, "INTC"),
    "CSCO": (858877, "CSCO"),
    "QCOM": (804328, "QCOM"),
    "AMGN": (318154, "AMGN"),
    "EBAY": (1065088, "EBAY"),
    "AAPL": (320193, "AAPL"),
    "GOOG": (1288776, "GOOG"),
    "ORCL": (1341439, "ORCL"),
    "DELL": (826083, None),
    "CMCSA": (1166691, "CMCSA"),
    "NXTL": (824169, None),
    "IACI": (891103, None),
    "SBUX": (829224, "SBUX"),
    "SYMC": (849399, None),
    "GILD": (882095, "GILD"),
    "AMZN": (1018724, "AMZN"),
}
FOREIGN = ("RIMM", "TEVA")
TOP5_IDS = ("MSFT", "INTC", "CSCO", "QCOM", "AMGN", "EBAY", "AAPL", "GOOG", "ORCL")
# 这八份持仓文件的版面约束，不是研究判定线。
NUMBER = re.compile(r"^\(?\s*\d{1,3}(?:,\d{3})+\)?$|^\d{4,}$")
SKIP = re.compile(
    r"total|cost|net assets|liabilit|cash|receivable|payable|dividend|income|expense|"
    r"distribut|unrealized|realized|value|shares|percent|security|description|redemption|"
    r"subscription|operation|year|units|transaction|investment|depreciation",
    re.IGNORECASE,
)
CLASS = re.compile(r"\b(class|cl|series|ser|special|spl)\b", re.IGNORECASE)
DROP = frozenset(
    "class cl series ser special spl adr ads sponsored spon inc incorporated corp corporation "
    "co company ltd limited plc the com new common ord shs nv sa ag".split()
)
MIN_VALUE, PRICE_RANGE, MAX_SHARE, SUM_RANGE = 1_000_000, (0.5, 5000.0), 0.3, (0.97, 1.03)
ROWS = {2003: 103, 2004: 102, 2005: 102, 2006: 101, 2007: 101, 2008: 102, 2009: 101, 2010: 102}
TOTALS = {
    2003: 20_061_689_164,
    2004: 20_376_214_952,
    2005: 19_590_693_619,
    2006: 17_957_728_469,
    2007: 20_559_646_460,
    2008: 18_091_801_491,
    2009: 17_093_021_127,
    2010: 21_232_784_912,
}
# 2003 年的份额变动行计入解析行数和合计比例，但不当持仓合并。
IGNORE = frozenset({(2003, "net increase decrease", 483_150_000)})
TOP10: dict[int, tuple[tuple[str, str, int], ...]] = {
    2003: (
        ("microsoft", "MSFT", 2022752120),
        ("intel", "INTC", 1226465098),
        ("cisco systems", "CSCO", 949407624),
        ("qualcomm", "QCOM", 779561761),
        ("amgen", "AMGN", 759749733),
        ("dell", "DELL", 617066299),
        ("comcast", "CMCSA", 573589268),
        ("ebay", "EBAY", 501158446),
        ("nextel communications", "NXTL", 495873629),
        ("interactivecorp", "IACI", 487979978),
    ),
    2004: (
        ("microsoft", "MSFT", 1833314973),
        ("qualcomm", "QCOM", 1357945537),
        ("ebay", "EBAY", 803096452),
        ("intel", "INTC", 798099276),
        ("cisco systems", "CSCO", 774575074),
        ("amgen", "AMGN", 592443732),
        ("nextel communications", "NXTL", 586337720),
        ("dell", "DELL", 579072804),
        ("comcast", "CMCSA", 475962100),
        ("starbucks", "SBUX", 445095405),
    ),
    2005: (
        ("microsoft", "MSFT", 1432856408),
        ("qualcomm", "QCOM", 1333188912),
        ("apple computer", "AAPL", 937211521),
        ("intel", "INTC", 794381302),
        ("amgen", "AMGN", 692007007),
        ("ebay", "EBAY", 628360976),
        ("cisco systems", "CSCO", 618327489),
        ("dell", "DELL", 455598036),
        ("symantec", "SYMC", 428688678),
        ("comcast", "CMCSA", 423222044),
    ),
    2006: (
        ("microsoft", "MSFT", 1203775048),
        ("apple computer", "AAPL", 1176412636),
        ("qualcomm", "QCOM", 929623224),
        ("google", "GOOG", 687990506),
        ("cisco systems", "CSCO", 643909909),
        ("intel", "INTC", 533318616),
        ("amgen", "AMGN", 500970870),
        ("oracle", "ORCL", 497914860),
        ("starbucks", "SBUX", 459482413),
        ("comcast", "CMCSA", 452472453),
    ),
    2007: (
        ("apple computer", "AAPL", 2181170201),
        ("microsoft", "MSFT", 1112735899),
        ("qualcomm", "QCOM", 999383666),
        ("google", "GOOG", 941333511),
        ("cisco systems", "CSCO", 841023269),
        ("research in motion", "RIMM", 646397630),
        ("intel", "INTC", 619358637),
        ("oracle", "ORCL", 540463953),
        ("ebay", "EBAY", 462913575),
        ("gilead sciences", "GILD", 436556338),
    ),
    2008: (
        ("apple", "AAPL", 1857626420),
        ("qualcomm", "QCOM", 1124883914),
        ("microsoft", "MSFT", 1109346176),
        ("google", "GOOG", 770634525),
        ("cisco systems", "CSCO", 629765649),
        ("oracle", "ORCL", 578225334),
        ("gilead sciences", "GILD", 546184730),
        ("research in motion", "RIMM", 510237336),
        ("intel", "INTC", 487729050),
        ("amgen", "AMGN", 386615484),
    ),
    2009: (
        ("apple", "AAPL", 2551390786),
        ("qualcomm", "QCOM", 993383660),
        ("microsoft", "MSFT", 874551876),
        ("google", "GOOG", 804070360),
        ("cisco systems", "CSCO", 534296372),
        ("oracle", "ORCL", 480073429),
        ("gilead sciences", "GILD", 456861345),
        ("intel", "INTC", 422565010),
        ("research in motion", "RIMM", 422468508),
        ("teva pharmaceutical industries", "TEVA", 410980196),
    ),
    2010: (
        ("apple", "AAPL", 4202892231),
        ("qualcomm", "QCOM", 1015399656),
        ("google", "GOOG", 916427258),
        ("microsoft", "MSFT", 847840959),
        ("oracle", "ORCL", 654113927),
        ("amazon", "AMZN", 542859200),
        ("cisco systems", "CSCO", 519429544),
        ("teva pharmaceutical industries", "TEVA", 449121149),
        ("intel", "INTC", 435837585),
        ("gilead sciences", "GILD", 341867716),
    ),
}
LISTS = {year: tuple(issuer for _, issuer, _ in rows) for year, rows in TOP10.items()}
VERDICTS = ("too_few", "holds", "not_holds")
LINE_NAMES = {
    "line1": "线 1：C ≥ 2",
    "line2": "线 2：季内打乱 p ≤ 0.05",
    "line3": "线 3：差的 Welch 95% 区间下沿 ≤ 0",
}
HOLDS_SENTENCE = "QQQ 的坏日子扎堆在大权重股财报后第一天，躲开的代价测不出为正"
REASONS = {"spread": "不集中", "premium": "要付财报溢价"}
FAMILY_SENTENCE = (
    "财报冲击：慢规则覆盖不到，按日程躲{reason}"
    "（C {c}，p {p}，差 {diff}，区间 {low} 到 {high}）；"
    "2004–2010 只看得出约 2 倍以上的集中，2 倍时约三成判得出（合成模拟，推断，上限约五成）"
)
NOT_AGAIN = "不再用 2004–2010 问"
TOO_FEW_SENTENCE = "too_few：|E| < 80 或 |B| × f < 4，停，families.md 不记"
SHIFT_SENTENCE = "多半是财报季，不只是那一天"
HOLDS_NULL = (
    "没信息时也判 holds 的比例（合成模拟，推断）：什么效应都没有约 0.2%，"
    "财报季整体更波动、那一天没效应约 4%–12%"
)
LIMITS = (
    "前 5 大占 26%–36%，比今天低",
    "坏日子大半在 2008 年",
    "Wilson 区间不算聚集，只印，不作判定线",
    "8-K 接收时间是发布时间的上界",
    "名单按年定，Google 2005-10..2006-09 不进前 5",
    "半天交易日按 16:00 算",
    "只看得出约 2 倍以上的集中",
    "前 10 敏感度里 IAC 和 Nextel 2004 年各缺一季",
)
CHECK, EVENTS, SUMMARY, REPORT = (
    "earnings_check.json",
    "earnings_events.jsonl",
    "earnings_table.json",
    "earnings_table.md",
)
SETS = ("top3", "top5", "top10", "first_per_quarter", "after_close")
EVENT_COLUMNS = ["id", "rank", "list_year", "accession", "accepted", "event_day"]
Record = dict[str, Any]
app = typer.Typer()


class TableError(ValueError):
    """本表的汇总错误，不包含输入数据的原值。"""


@dataclass(frozen=True)
class Inputs:
    snapshot: Any
    qqq: pd.Series
    sources: Record


@dataclass(frozen=True)
class Parsed:
    total: int
    rows: tuple[tuple[str, int, int], ...]


@dataclass(frozen=True)
class Built:
    lists: dict[int, tuple[str, ...]]
    holdings: list[Record]
    calendar: pd.DatetimeIndex
    window: pd.DatetimeIndex
    returns: pd.Series
    filings: int
    results: pd.DataFrame
    sets: dict[str, pd.DataFrame]
    stock: pd.Series
    sources: Record


def clean(text: str) -> str:
    text = re.sub(r"<[^>]*>", " ", text)
    return " ".join(html.unescape(text).replace("\xa0", " ").split())


def cells(text: str, is_html: bool) -> list[list[str]]:
    if is_html:
        text = re.sub(r"<(script|style)\b[^>]*>.*?</\1\s*>", "", text, flags=re.I | re.S)
        return [
            [value for cell in re.split(r"</t[dh]\s*>", row, flags=re.I) if (value := clean(cell))]
            for row in re.split(r"</tr\s*>", text, flags=re.I)
        ]
    return [
        [cell.strip() for cell in re.split(r"\.{3,}|\s{2,}", row) if cell.strip()]
        for row in text.splitlines()
    ]


def holdings(row: Sequence[str]) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    name: list[str] = []
    numbers: list[int] = []
    for word in " ".join(row).split():
        token = word.replace("$", "")
        if not token:
            continue
        if NUMBER.fullmatch(token):
            numbers.append(int(re.sub(r"\D", "", token)))
        else:
            if numbers:
                if name and len(numbers) >= 2:
                    out.append((" ".join(name), numbers[-2], numbers[-1]))
                name, numbers = [], []
            name.append(word)
    if name and len(numbers) >= 2:
        out.append((" ".join(name), numbers[-2], numbers[-1]))
    return out


def normalised(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def issuer_key(name: str) -> str:
    words = re.sub(r"[^a-z0-9]", " ", name.lower()).split()
    keep = [word for word in words if word not in DROP and len(word) > 1]
    return " ".join(keep) if keep else "".join(words)


def parse_document(document: str, payload: bytes) -> Parsed:
    totals: list[int] = []
    found: dict[tuple[str, int], tuple[str, int, int]] = {}
    for row in cells(
        payload.decode("utf-8", errors="replace"), document.lower().endswith((".htm", ".html"))
    ):
        text = " ".join(row)
        if "total investments" in text.lower():
            numbers = [
                int(re.sub(r"\D", "", token))
                for token in text.replace("$", " ").split()
                if NUMBER.fullmatch(token)
            ]
            if numbers:
                totals.append(numbers[-1])
            continue
        for raw, shares, value in holdings(row):
            name = re.sub(r"[\s*.]+$", "", raw).strip(" |")
            if not re.search(r"[a-z]{3}", name, re.I) or SKIP.search(name) or value < MIN_VALUE:
                continue
            if not shares or not PRICE_RANGE[0] <= value / shares <= PRICE_RANGE[1]:
                continue
            found[(normalised(name), value)] = (name, shares, value)
    if not totals:
        raise TableError("no total investments row")
    total = totals[0]
    rows = tuple(
        sorted(
            (row for row in found.values() if row[2] < MAX_SHARE * total),
            key=lambda row: (-row[2], normalised(row[0])),
        )
    )
    return Parsed(total, rows)


def holdings_lists(
    documents: Mapping[int, tuple[str, bytes]],
    *,
    top10: Mapping[int, tuple[tuple[str, str, int], ...]] = TOP10,
    rows: Mapping[int, int] = ROWS,
    totals: Mapping[int, int] = TOTALS,
    ignore: frozenset[tuple[int, str, int]] = IGNORE,
) -> tuple[dict[int, tuple[str, ...]], list[Record]]:
    lists: dict[int, tuple[str, ...]] = {}
    report: list[Record] = []
    for year in sorted(top10):
        prefix = f"fiscal year {year}: "
        if year not in documents:
            raise TableError(prefix + "no holdings document")
        document, payload = documents[year]
        try:
            parsed = parse_document(document, payload)
        except TableError as exc:
            raise TableError(prefix + str(exc)) from exc
        if len(parsed.rows) != rows[year]:
            raise TableError(prefix + "the parsed row count is not the probe's")
        if parsed.total != totals[year]:
            raise TableError(prefix + "total investments is not the pinned total")
        share = sum(value for _, _, value in parsed.rows) / parsed.total
        if not SUM_RANGE[0] <= share <= SUM_RANGE[1]:
            raise TableError(prefix + "the parsed rows do not add up to the total")
        merged: Counter[str] = Counter()
        counts: Counter[str] = Counter()
        for name, _, value in parsed.rows:
            key = issuer_key(name)
            if (year, key, value) not in ignore:
                merged[key] += value
                counts[key] += 1
        actual = sorted(merged.items(), key=lambda item: (-item[1], item[0]))[: len(top10[year])]
        if actual != [(key, value) for key, _, value in top10[year]]:
            raise TableError(prefix + "the merged top ten is not the pinned list")
        lists[year] = tuple(issuer for _, issuer, _ in top10[year])
        report.append(
            {
                "fiscal_year": year,
                "document": document,
                "rows": len(parsed.rows),
                "total": parsed.total,
                "sum_share": share,
                "same_key": [
                    {"key": key, "rows": count}
                    for key, count in sorted(counts.items())
                    if count > 1
                ],
                "class_rows": sum(bool(CLASS.search(name)) for name, _, _ in parsed.rows),
                "top10": [key for key, _ in actual],
            }
        )
    return lists, report


def trading_closes(calendar: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return (
        (calendar.normalize() + pd.Timedelta(seconds=CLOSE_SECONDS))
        .tz_localize(EASTERN)
        .tz_convert("UTC")
    )


def event_days(accepted: pd.Series, calendar: pd.DatetimeIndex) -> pd.Series:
    positions = trading_closes(calendar).searchsorted(
        pd.to_datetime(accepted, utc=True), side="right"
    )
    return pd.Series(
        [calendar[int(pos)] if pos < len(calendar) else pd.NaT for pos in positions],
        index=accepted.index,
        dtype="datetime64[ns]",
    )


def list_year(day: pd.Timestamp) -> int:
    return day.year if day.month >= 10 else day.year - 1


def results(filings: pd.DataFrame) -> pd.DataFrame:
    by_cik = {cik: issuer for issuer, (cik, _) in ISSUERS.items()}
    out: list[Record] = []
    for row in filings.to_dict("records"):
        if row["cik"] not in by_cik:
            raise TableError("a filing's CIK is not one of the configured issuers")
        if ee.is_results_filing(str(row["form"]), str(row["items"])):
            out.append(
                {
                    "id": by_cik[row["cik"]],
                    "accession": str(row["accessionNumber"]),
                    "accepted": ee.parse_acceptance(str(row["acceptanceDateTime"])),
                }
            )
    found = pd.DataFrame(out, columns=["id", "accession", "accepted"])
    found["accepted"] = pd.to_datetime(found["accepted"], utc=True)
    return found.sort_values(["accepted", "accession"], kind="stable").reset_index(drop=True)


def events(
    found: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    window: pd.DatetimeIndex,
    lists: Mapping[int, tuple[str, ...]],
    top: int,
) -> pd.DataFrame:
    found = found.copy()
    found["event_day"] = event_days(found["accepted"], calendar)
    out: list[Record] = []
    for row in found[found["event_day"].isin(window)].to_dict("records"):
        year = list_year(pd.Timestamp(row["event_day"]))
        names = lists[year][:top]
        if row["id"] in names:
            out.append(
                {
                    "id": row["id"],
                    "accession": row["accession"],
                    "accepted": row["accepted"],
                    "event_day": row["event_day"],
                    "rank": names.index(row["id"]) + 1,
                    "list_year": year,
                }
            )
    frame = pd.DataFrame(out, columns=EVENT_COLUMNS)
    frame["accepted"] = pd.to_datetime(frame["accepted"], utc=True)
    frame["event_day"] = pd.to_datetime(frame["event_day"])
    return frame.sort_values(["event_day", "accepted", "accession"], kind="stable").reset_index(
        drop=True
    )


def first_per_quarter(found: pd.DataFrame) -> pd.DataFrame:
    quarters = pd.DatetimeIndex(found["event_day"]).to_period("Q")
    return found.loc[~pd.MultiIndex.from_arrays([found["id"], quarters]).duplicated()].copy()


def after_close(found: pd.DataFrame) -> pd.DataFrame:
    accepted = pd.DatetimeIndex(found["accepted"]).tz_convert(EASTERN)
    seconds = accepted.hour * 3600 + accepted.minute * 60 + accepted.second
    return found.loc[(seconds >= CLOSE_SECONDS) | (seconds < OPEN_SECONDS)].copy()


def days_of(found: pd.DataFrame) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(found["event_day"]).unique().sort_values()


def bad_count(n: int, share: float) -> int:
    return math.ceil(round(share * n, 9))


def bad_days(returns: pd.Series, share: float = BAD_SHARE) -> pd.DatetimeIndex:
    ordered = returns.sort_index().sort_values(kind="stable")
    return pd.DatetimeIndex(ordered.index[: bad_count(len(returns), share)]).sort_values()


def wilson(hits: int, n: int, z: float = Z) -> tuple[float, float]:
    p = hits / n
    center = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return center - half, center + half


def concentration(
    days: pd.DatetimeIndex, window: pd.DatetimeIndex, bad: pd.DatetimeIndex
) -> Record:
    if len(days.difference(window)):
        raise TableError("event days are outside the window")
    hits = len(days.intersection(bad))
    f = len(days) / len(window) if len(window) else 0.0
    share = hits / len(bad) if len(bad) else 0.0
    low, high = wilson(hits, len(bad)) if len(bad) else (0.0, 0.0)
    return {
        "events": len(days),
        "window": len(window),
        "bad": len(bad),
        "hits": hits,
        "f": f,
        "share": share,
        "c": share / f if f else None,
        "wilson_low": low / f if f else None,
        "wilson_high": high / f if f else None,
    }


def welch(
    returns: pd.Series, days: pd.DatetimeIndex, bad: pd.DatetimeIndex | None = None
) -> Record | None:
    mask = returns.index.isin(days)
    e, o = returns[mask], returns[~mask]
    if min(len(e), len(o)) < 2:
        return None
    diff = float(e.mean() - o.mean())
    se = math.sqrt(float(e.var(ddof=1) / len(e) + o.var(ddof=1) / len(o)))
    out: Record = {
        "n_e": len(e),
        "n_o": len(o),
        "mean_e": float(e.mean()),
        "mean_o": float(o.mean()),
        "median_e": float(e.median()),
        "median_o": float(o.median()),
        "diff": diff,
        "se": se,
        "low": diff - WELCH_Z * se,
        "high": diff + WELCH_Z * se,
        "point_le": bool(e.mean() <= o.mean()),
        "log_sum_e": float(np.log1p(e).sum()),
    }
    if bad is not None:
        eg, og = e[~e.index.isin(bad)], o[~o.index.isin(bad)]
        out.update(
            mean_e_good=float(eg.mean()) if len(eg) else None,
            mean_o_good=float(og.mean()) if len(og) else None,
        )
    return out


def quarters(window: pd.DatetimeIndex) -> dict[tuple[int, int], pd.DatetimeIndex]:
    return {
        (int(year), int(quarter)): window[(window.year == year) & (window.quarter == quarter)]
        for year, quarter in sorted({(day.year, day.quarter) for day in window})
    }


def shuffle_draw(
    days: pd.DatetimeIndex,
    pools: Mapping[tuple[int, int], pd.DatetimeIndex],
    rng: np.random.Generator,
) -> pd.DatetimeIndex:
    picks = []
    for day in days.sort_values():
        pool = pools[(day.year, day.quarter)]
        picks.append(pool[int(rng.integers(0, len(pool)))])
    return pd.DatetimeIndex(picks).unique().sort_values()


def shuffle(
    days: pd.DatetimeIndex,
    window: pd.DatetimeIndex,
    bad: pd.DatetimeIndex,
    draws: int = DRAWS,
    seed: int = SEED,
) -> Record:
    actual = concentration(days, window, bad)["c"]
    if actual is None:
        raise TableError("no event days for the shuffle")
    rng, pools = np.random.default_rng(seed), quarters(window)
    values = np.array(
        [concentration(shuffle_draw(days, pools, rng), window, bad)["c"] for _ in range(draws)],
        dtype=float,
    )
    ge = int((values >= actual).sum())
    p = (1 + ge) / (1 + draws)
    return {
        "draws": draws,
        "seed": seed,
        "actual": actual,
        "ge": ge,
        "p": p,
        "mid_rank": float(((values < actual).sum() + 0.5 * (values == actual).sum()) / draws),
        "q05": float(np.quantile(values, 0.05)),
        "q50": float(np.quantile(values, 0.5)),
        "q95": float(np.quantile(values, 0.95)),
        "passes": p <= P_LINE,
    }


def shifted(days: pd.DatetimeIndex, window: pd.DatetimeIndex, shift: int) -> pd.DatetimeIndex:
    positions = window.get_indexer(days) + shift
    return window[positions[(positions >= 0) & (positions < len(window))]]


def shift_rows(
    days: pd.DatetimeIndex, window: pd.DatetimeIndex, bad: pd.DatetimeIndex
) -> list[Record]:
    return [{"shift": s, **concentration(shifted(days, window, s), window, bad)} for s in SHIFTS]


def shift_note(c: float, shifts: Sequence[Record]) -> bool:
    return any(row["c"] is not None and row["c"] >= c - SHIFT_NOTE for row in shifts)


def year_rows(
    days: pd.DatetimeIndex, window: pd.DatetimeIndex, bad: pd.DatetimeIndex
) -> list[Record]:
    out: list[Record] = []
    for year in range(FIRST_DAY.year, LAST_DAY.year + 1):
        w, e, b = (x[x.year == year] for x in (window, days, bad))
        hits = len(e.intersection(b))
        out.append(
            {
                "year": year,
                "window": len(w),
                "events": len(e),
                "bad": len(b),
                "hits": hits,
                "c": (hits / len(b)) / (len(e) / len(w)) if len(b) and len(e) else None,
            }
        )
    return out


def stock_returns(
    found: pd.DataFrame, prices: pd.DataFrame, calendar: pd.DatetimeIndex
) -> pd.Series:
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"])
    table = prices.set_index(["ticker", "date"])["adj_close"]
    out: list[float] = []
    for row in found.to_dict("records"):
        ticker = ISSUERS[row["id"]][1]
        day = pd.Timestamp(row["event_day"])
        pos = int(calendar.get_indexer(pd.DatetimeIndex([day]))[0])
        if ticker is None or pos <= 0:
            out.append(float("nan"))
            continue
        before = calendar[pos - 1]
        if (ticker, day) not in table.index or (ticker, before) not in table.index:
            out.append(float("nan"))
        else:
            out.append(float(table.loc[(ticker, day)] / table.loc[(ticker, before)] - 1))
    return pd.Series(out, index=found.index, dtype=float)


def months_in_top(issuer: str, lists: Mapping[int, tuple[str, ...]]) -> int:
    return sum(
        issuer in lists[list_year(month.to_timestamp())][:TOP]
        for month in pd.period_range(FIRST_DAY, LAST_DAY, freq="M")
    )


def issuer_rows(
    found: pd.DataFrame,
    stock: pd.Series,
    bad: pd.DatetimeIndex,
    lists: Mapping[int, tuple[str, ...]],
) -> list[Record]:
    out: list[Record] = []
    for issuer in TOP5_IDS:
        mine = found[found["id"] == issuer]
        values = stock.loc[mine.index].dropna()
        out.append(
            {
                "id": issuer,
                "months": months_in_top(issuer, lists),
                "events": len(mine),
                "bad_events": int(mine["event_day"].isin(bad).sum()),
                "returns": len(values),
                "missing": len(mine) - len(values),
                "median": float(values.median()) if len(values) else None,
                "crashes": int((values <= CRASH_STOCK).sum()),
            }
        )
    return out


def crash_days(found: pd.DataFrame, stock: pd.Series, returns: pd.Series) -> pd.DatetimeIndex:
    qqq = returns.reindex(pd.DatetimeIndex(found["event_day"])).to_numpy(dtype=float)
    return days_of(found.loc[(stock <= CRASH_STOCK) & (qqq <= CRASH_QQQ)])


def follow(returns: pd.Series, days: pd.DatetimeIndex, horizon: int) -> list[float]:
    out: list[float] = []
    for pos in returns.index.get_indexer(days):
        if pos >= 0 and pos + horizon < len(returns):
            values = returns.iloc[pos + 1 : pos + horizon + 1].to_numpy(dtype=float)
            out.append(float(np.prod(1 + values) - 1))
    return out


def stats(values: Sequence[float]) -> Record:
    if not len(values):
        return dict.fromkeys(("mean", "median", "min", "q25", "q75", "max")) | {"n": 0}
    return {
        "n": len(values),
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "min": float(np.min(values)),
        "q25": float(np.quantile(values, 0.25)),
        "q75": float(np.quantile(values, 0.75)),
        "max": float(np.max(values)),
    }


def crash_table(
    found: pd.DataFrame, stock: pd.Series, returns: pd.Series, days: pd.DatetimeIndex
) -> Record:
    crashes = crash_days(found, stock, returns)
    control = pd.DatetimeIndex(returns.index[(returns <= CRASH_QQQ) & ~returns.index.isin(days)])
    horizons: list[Record] = []
    for horizon in HORIZONS:
        values = follow(returns, crashes, horizon)
        horizons.append(
            {
                "horizon": horizon,
                "crash": stats(values) if len(crashes) >= MIN_CRASHES else {"n": len(values)},
                "control": stats(follow(returns, control, horizon)),
            }
        )
    return {
        "events": len(crashes),
        "days": [_date(day) for day in crashes],
        "control_days": len(control),
        "horizons": horizons,
    }


def sensitivity(built: Built, bad: pd.DatetimeIndex) -> Record:
    out: Record = {}
    for name in ("top3", "top10", "first_per_quarter", "after_close"):
        days = days_of(built.sets[name])
        found = welch(built.returns, days)
        out[name] = {
            **concentration(days, built.window, bad),
            "welch_low": None if found is None else found["low"],
        }
    for share in SENSITIVE_BAD:
        out[f"bad_{share}"] = concentration(
            days_of(built.sets["top5"]), built.window, bad_days(built.returns, share)
        )
    return out


def judge(main: Record, shuffled: Record | None, found: Record | None) -> Record:
    if main["events"] < TOO_FEW_EVENTS or main["bad"] * main["f"] < TOO_FEW_EXPECTED:
        return {"verdict": "too_few", "lines": None, "failed": [], "sentence": TOO_FEW_SENTENCE}
    if shuffled is None or found is None or main.get("c") is None:
        raise TableError("missing measures for the three lines")
    lines = {
        "line1": bool(main["c"] >= LINE_C),
        "line2": bool(shuffled["p"] <= P_LINE),
        "line3": bool(found["low"] <= 0),
    }
    failed = [name for name, passes in lines.items() if not passes]
    sentence = HOLDS_SENTENCE
    if failed:
        sentence = FAMILY_SENTENCE.format(
            reason=REASONS["premium" if failed == ["line3"] else "spread"],
            c=f"{main['c']:.2f}",
            p=f"{shuffled['p']:.3f}",
            diff=f"{100 * found['diff']:+.3f}%",
            low=f"{100 * found['low']:+.3f}%",
            high=f"{100 * found['high']:+.3f}%",
        )
    return {
        "verdict": "not_holds" if failed else "holds",
        "lines": lines,
        "failed": failed,
        "sentence": sentence,
    }


def qqq_series(daily: pd.DataFrame) -> pd.Series:
    if QQQ_COLUMN not in daily:
        raise TableError(f"QQQ daily has no {QQQ_COLUMN} column")
    series = daily[QQQ_COLUMN].copy()
    series.index = pd.DatetimeIndex(series.index)
    series = series.loc[series.index <= LAST_DAY].astype(float).dropna()
    if not series.index.is_unique or not series.index.is_monotonic_increasing:
        raise TableError("QQQ dates must be unique and increasing")
    if not np.isfinite(series.to_numpy(dtype=float)).all():
        raise TableError("QQQ returns are not finite")
    return series


def build(inputs: Inputs) -> Built:
    lists, holdings_report = holdings_lists(inputs.snapshot.documents)
    calendar = pd.DatetimeIndex(inputs.qqq.index)
    window = calendar[(calendar >= FIRST_DAY) & (calendar <= LAST_DAY)]
    found = results(inputs.snapshot.filings)
    sets = {f"top{top}": events(found, calendar, window, lists, top) for top in (3, TOP, 10)}
    sets["first_per_quarter"] = first_per_quarter(sets["top5"])
    sets["after_close"] = after_close(sets["top5"])
    return Built(
        lists=lists,
        holdings=holdings_report,
        calendar=calendar,
        window=window,
        returns=inputs.qqq.reindex(window),
        filings=len(inputs.snapshot.filings),
        results=found,
        sets=sets,
        stock=stock_returns(sets["top5"], inputs.snapshot.prices, calendar),
        sources=inputs.sources,
    )


def _date(day: Any) -> str:
    return str(pd.Timestamp(day).date().isoformat())


def _identity() -> Record:
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "earnings_snapshot_id": EARNINGS_SNAPSHOT_ID,
        "qqq_snapshot_id": QQQ_SNAPSHOT_ID,
    }


def check_summary(built: Built) -> Record:
    if not len(built.window):
        raise TableError("no QQQ trading days in the requested window")
    counts = {
        name: {
            "rows": len(built.sets[name]),
            "days": len(days_of(built.sets[name])),
            "repeated_days": int((built.sets[name]["event_day"].value_counts() > 1).sum()),
        }
        for name in SETS
    }
    nbad = bad_count(len(built.window), BAD_SHARE)
    expected = nbad * counts["top5"]["days"] / len(built.window)
    top5, top10 = built.sets["top5"], built.sets["top10"]
    hours = pd.DatetimeIndex(top5["accepted"]).tz_convert(EASTERN).hour
    per_list = [
        {
            "list_year": year,
            "rank": rank,
            "id": issuer,
            "events": int(((top10["list_year"] == year) & (top10["id"] == issuer)).sum()),
        }
        for year, issuers in sorted(built.lists.items())
        for rank, issuer in enumerate(issuers, start=1)
        if issuer in ISSUERS
    ]
    return {
        **_identity(),
        "qqq_column": QQQ_COLUMN,
        "sources": built.sources,
        "window": {
            "first": _date(built.window[0]),
            "last": _date(built.window[-1]),
            "days": len(built.window),
        },
        "bad_days": nbad,
        "filings": {"rows": built.filings, "results": len(built.results)},
        "sets": counts,
        "expected_hits": expected,
        "too_few": counts["top5"]["days"] < TOO_FEW_EVENTS or expected < TOO_FEW_EXPECTED,
        "per_list": per_list,
        "hours": {f"{hour:02d}": int((hours == hour).sum()) for hour in range(24)},
        "evening_share": float((hours >= 16).mean()) if len(hours) else None,
        "missing_prices": int(built.stock.isna().sum()),
        "holdings": built.holdings,
    }


def event_rows(built: Built) -> list[Record]:
    return [
        {
            "id": row["id"],
            "rank": int(row["rank"]),
            "list_year": int(row["list_year"]),
            "accession": row["accession"],
            "accepted_utc": row["accepted"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "accepted_eastern": row["accepted"].tz_convert(EASTERN).isoformat(),
            "event_day": _date(row["event_day"]),
        }
        for row in built.sets["top5"].to_dict("records")
    ]


def read(inputs: Inputs) -> tuple[Record, list[Record]]:
    built = build(inputs)
    checked = check_summary(built)
    days = days_of(built.sets["top5"])
    bad = bad_days(built.returns)
    main = concentration(days, built.window, bad)
    result: Record = {
        **_identity(),
        "check": checked,
        "main": main,
        **dict.fromkeys(
            ("shuffle", "welch", "shifts", "years", "issuers", "crashes", "sensitivity")
        ),
    }
    if checked["too_few"]:
        result["decision"] = {**judge(main, None, None), "shift_note": None}
    else:
        result.update(
            shuffle=shuffle(days, built.window, bad),
            welch=welch(built.returns, days, bad),
            shifts=shift_rows(days, built.window, bad),
            years=year_rows(days, built.window, bad),
            issuers=issuer_rows(built.sets["top5"], built.stock, bad, built.lists),
            crashes=crash_table(built.sets["top5"], built.stock, built.returns, days),
            sensitivity=sensitivity(built, bad),
        )
        result["decision"] = {
            **judge(main, result["shuffle"], result["welch"]),
            "shift_note": shift_note(main["c"], result["shifts"]),
        }
    return result, event_rows(built)


def _display(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True).replace("|", "\\|")
    return str(value).replace("|", "\\|")


def _table(rows: Sequence[Record]) -> str:
    if not rows:
        return "无行。"
    keys = list(dict.fromkeys(key for row in rows for key in row))
    return "\n".join(
        ["| " + " | ".join(keys) + " |", "| " + " | ".join("---" for _ in keys) + " |"]
        + ["| " + " | ".join(_display(row.get(key)) for key in keys) + " |" for row in rows]
    )


def render(result: Record) -> str:
    checked, main, decision = result["check"], result["main"], result["decision"]
    window = checked["window"]
    lines = [
        "# 大权重股财报后第一天和 QQQ 最差的日子",
        "",
        f"运行 {RUN_ID}；决定 `{OWNER_DECISION}`；依据第 6 期章程第 24 节。",
        f"财报快照 `{EARNINGS_SNAPSHOT_ID}`；QQQ 快照 `{QQQ_SNAPSHOT_ID}`；列 `{QQQ_COLUMN}`。",
        "只印，不是探针、不是试验、不进账本，不模拟躲开财报日的仓位线，不是长牛代价的读数。",
        f"研究窗口：QQQ 开发窗口中的财报子窗口；请求 {_date(FIRST_DAY)}..{_date(LAST_DAY)}；"
        f"实际 {window['first']}..{window['last']}，{window['days']} 天。",
        "数据质量与来源：" + _display(checked["sources"]),
        "主名单取每年九月底前 5 大，10 月起生效；外国公司占位但不补名。",
        "判定线（第 24 节，读取前固定）：" + "；".join(LINE_NAMES.values()) + "。",
        "",
        f"## 判定：{decision['verdict']}",
        "",
        decision["sentence"] + "。",
    ]
    if decision["verdict"] == "holds":
        lines += [
            "当天告诉 Baker；11-20 续期卡问要不要在前瞻账本加一条「按日程降仓」的只记账线"
            "（只记账，不改日报），连同平移对照一起看。",
            HOLDS_NULL + "。",
        ]
    elif decision["verdict"] == "not_holds":
        lines += [
            "没过的线：" + "；".join(LINE_NAMES[name] for name in decision["failed"]) + "。",
            NOT_AGAIN + "（不是「以后不再问」，前瞻那条路和财报日历的截断不因这张表关掉）。",
            "给 Baker 看的检出率：2 倍集中时约三成判得出（主数），上限约五成。",
        ]
    if decision["shift_note"]:
        lines.append("平移对照提示：" + SHIFT_SENTENCE + "。")
    lines += [
        "",
        "## 集中倍数",
        "",
        _table(
            [
                {
                    "W": main["window"],
                    "B": main["bad"],
                    "|E|": main["events"],
                    "|E ∩ B|": main["hits"],
                    "f": main["f"],
                    "C": main["c"],
                    "Wilson 下沿（只印）": main["wilson_low"],
                    "Wilson 上沿（只印）": main["wilson_high"],
                }
            ]
        ),
    ]
    if decision["verdict"] == "too_few":
        lines.append(f"|B| × f = {checked['expected_hits']:.6g}；停在这里，其余的表不算。")
    else:
        lines += [
            "",
            "## 季内打乱（只印）",
            "",
            "每个事件日按季有放回抽取，重复日合并；p 含一次加一修正。",
            _table([result["shuffle"]]),
            "",
            "## 躲开的代价",
            "",
            "E 减其他日的日收益差；Welch 区间，log_sum_e 是事件日日收益 log1p 之和，"
            "不模拟仓位，也没有交易成本假设。",
            _table([result["welch"]]),
            "",
            "## 每年",
            "",
            _table(result["years"]),
            "",
            "## 每家公司（前 5 的 9 家）",
            "",
            _table(result["issuers"]),
            "",
            "## 平移对照（只印）",
            "",
            _table(result["shifts"]),
            "",
            "## 急跌之后（只印）",
            "",
            f"急跌事件 {result['crashes']['events']} 天；"
            f"对照 {result['crashes']['control_days']} 天。",
            "急跌日期：" + "、".join(result["crashes"]["days"]),
            "从次日起复利，末端不足整段剔除；急跌事件少于 5 天只印次数。",
            _table(result["crashes"]["horizons"]),
            "",
            "## 敏感度（只印，不跑打乱）",
            "",
            _table([{"set": name, **row} for name, row in result["sensitivity"].items()]),
        ]
    lines += ["", "## 局限", "", *("- " + limit for limit in LIMITS)]
    if "provenance" in result:
        lines += [
            "",
            f"提交：{result['provenance'].get('commit')}；分支：{result['provenance'].get('branch')}。",
        ]
    return "\n".join(lines) + "\n"


def write_check(summary: Record, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / CHECK).write_text(
        json.dumps(summary, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_result(result: Record, rows: Sequence[Record], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / EVENTS).write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    (out_dir / SUMMARY).write_text(
        json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


def spec_problems(spec: ee.EarningsSpec) -> list[str]:
    problems = []
    expected = {issuer: ((cik,), ticker) for issuer, (cik, ticker) in ISSUERS.items()}
    if {issuer.id: (issuer.ciks, issuer.fmp_ticker) for issuer in spec.issuers} != expected:
        problems.append("the configured issuers are not the module's")
    if tuple(issuer.id for issuer in spec.foreign) != FOREIGN:
        problems.append("the configured foreign issuers are not the module's")
    if {document.fiscal_year for document in spec.holdings_documents} != set(TOP10):
        problems.append("the configured holdings years are not the module's")
    return problems


def _first_line(exc: Exception) -> str:
    return str(exc).splitlines()[0] if str(exc) else ""


def load_inputs(root: Path = PROJECT_ROOT, qqq_root: Path = qlh.DEFAULT_SNAPSHOT_ROOT) -> Inputs:
    problems = spec_problems(ee.load_spec())
    if problems:
        raise TableError("; ".join(problems))
    try:
        snapshot = ee.load_snapshot(root, EARNINGS_SNAPSHOT_ID)
    except (OSError, ValueError, KeyError) as exc:
        raise TableError(f"earnings snapshot {EARNINGS_SNAPSHOT_ID}: {_first_line(exc)}") from exc
    try:
        daily, report = qlh.load_daily(qlh.resolve_snapshot_dir(qqq_root, QQQ_SNAPSHOT_ID))
    except (OSError, ValueError, KeyError) as exc:
        raise TableError(f"QQQ snapshot {QQQ_SNAPSHOT_ID}: {_first_line(exc)}") from exc
    qqq = qqq_series(daily)
    sources = {
        "earnings": {
            "snapshot_id": EARNINGS_SNAPSHOT_ID,
            "filings": len(snapshot.filings),
            "prices": len(snapshot.prices),
            "documents": len(snapshot.documents),
        },
        "qqq": {
            "snapshot_id": QQQ_SNAPSHOT_ID,
            "check_status": report["status"],
            "series_csv_sha256": report["series_csv_sha256"],
            "first": _date(qqq.index[0]) if len(qqq) else None,
            "last": _date(qqq.index[-1]) if len(qqq) else None,
            "days": len(qqq),
        },
    }
    return Inputs(snapshot, qqq, sources)


def _error_summary(exc: Exception) -> str:
    return _first_line(exc) if isinstance(exc, TableError) else type(exc).__name__


def _load() -> Inputs:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_table: 数据载入失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc


@app.callback()
def cli() -> None:
    """只印财报冲击表，不进账本、不模拟躲开财报日的仓位线。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        summary = check_summary(build(inputs))
    except (ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_table: 建数据失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc
    write_check(summary, out)
    typer.echo(
        f"earnings_shock_table: W {summary['window']['days']} 天；前 5 的事件 "
        f"{summary['sets']['top5']['rows']} 个、{summary['sets']['top5']['days']} 天；"
        "名单 8 年都对上"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        result, rows = read(inputs)
    except (ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_table: 建数据失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, rows, out)
    typer.echo(f"earnings_shock_table: 判定 {result['decision']['verdict']}")


if __name__ == "__main__":
    app()
