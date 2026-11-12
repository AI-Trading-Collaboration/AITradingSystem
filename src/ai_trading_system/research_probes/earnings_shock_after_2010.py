"""大权重股财报后第一天和 QQQ 最差的日子，QQQ 2011 年以后读一次（TRADING-2565-130）。

依据 owner_decision:TRADING-2565:2026-11-10:earnings_shock_after_2010。
TRADING-2565-99 的规则一字不改、导入它的函数，换到登记窗口
qqq_after_2010_owner_print（2011-01-03..2026-10-12）读一次。
名单按 2010–2025 年 16 份 QQQ 信托年报持仓定死，不用任何个股的价格。
只印，不是探针、不是试验、不进账本，不模拟躲开财报日的仓位线。
check 只数次数和分布，不碰收益；run 读一次，已有结果拒绝覆盖。
"""

from __future__ import annotations

import json
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
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_data import earnings_events as ee
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import earnings_shock_table as es
from ai_trading_system.research_probes import structure_shift_periods as ssp

# 决定号下固定的输入身份和读取边界，不另设研究判定线。
RUN_ID = "TRADING-2565-130"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-11-10:earnings_shock_after_2010"
EARNINGS_SNAPSHOT_ID = "ern-97c76c70e3a0"
QQQ_SNAPSHOT_ID = es.QQQ_SNAPSHOT_ID
QQQ_COLUMN = es.QQQ_COLUMN
SPEC_PATH = PROJECT_ROOT / "config" / "research" / "research_earnings_after_2010.yaml"
WINDOW_ID = ssp.QQQ_WINDOW_ID
FILING_START = pd.Timestamp("2010-10-01")
FIRST_DAY, LAST_DAY = pd.Timestamp("2011-01-03"), pd.Timestamp("2026-10-12")
# TRADING-2565-122 check 核过的交易日数，输入核对，不是研究的线。
WINDOW_DAYS = 3967
LAST_LIST = 2025
ISSUERS: dict[str, tuple[int, ...]] = {
    "AAPL": (320193,),
    "MSFT": (789019,),
    "GOOG": (1288776, 1652044),
    "AMZN": (1018724,),
    "QCOM": (804328,),
    "ORCL": (1341439,),
    "INTC": (50863,),
    "CSCO": (858877,),
    "GILD": (882095,),
    "META": (1326801,),
    "TSLA": (1318605,),
    "NVDA": (1045810,),
    "AVGO": (1730168,),
    "AMGN": (318154,),
    "CMCSA": (1166691,),
    "MDLZ": (1103982,),
    "KHC": (1637459,),
    "CELG": (816284,),
    "NFLX": (1065280,),
    "PEP": (77476,),
    "ADBE": (796343,),
    "PYPL": (1633917,),
    "COST": (909832,),
    "PLTR": (1321655,),
}
FOREIGN = ("TEVA",)
TOP5_IDS = (
    "AAPL",
    "MSFT",
    "GOOG",
    "AMZN",
    "QCOM",
    "ORCL",
    "INTC",
    "CSCO",
    "GILD",
    "META",
    "TSLA",
    "NVDA",
    "AVGO",
)
# 16 份年报版面及持仓核对值，来自读取前固定的持仓探查，不是研究的线。
HOLDINGS: dict[int, tuple[str, str]] = {
    2010: ("0001104659-10-064790", "a10-18275_1n30b2.htm"),
    2011: ("0001104659-12-003105", "a11-28102_1n30b2.htm"),
    2012: ("0001104659-13-005075", "a12-24340_1n30b2.htm"),
    2013: ("0001104659-14-003107", "a13-21957_1n30b2.htm"),
    2014: ("0001104659-15-002429", "a14-24967_1n30b2.htm"),
    2015: ("0001193125-16-422287", "d95463dn30b2.htm"),
    2016: ("0001193125-16-806695", "d272217dn30b2.htm"),
    2017: ("0001193125-18-001208", "d432220dn30b2.htm"),
    2018: ("0001193125-18-355790", "d638310dn30b2.htm"),
    2019: ("0001193125-19-320290", "d813945dn30b2.htm"),
    2020: ("0001193125-20-326183", "d75380dn30b2.htm"),
    2021: ("0001193125-21-368691", "d211991dn30b2.htm"),
    2022: ("0001193125-22-313828", "d385116dn30b2.htm"),
    2023: ("0001193125-23-304771", "d538532dn30b2.htm"),
    2024: ("0001193125-24-285734", "d876223dn30b2.htm"),
    2025: ("0001193125-25-249697", "d29525dn30b2.htm"),
}
ROWS = {
    2010: 103,
    2011: 102,
    2012: 102,
    2013: 102,
    2014: 105,
    2015: 111,
    2016: 108,
    2017: 109,
    2018: 105,
    2019: 104,
    2020: 104,
    2021: 103,
    2022: 103,
    2023: 102,
    2024: 102,
    2025: 101,
}
TOTALS = {
    2010: 21_232_784_912,
    2011: 21_485_139_915,
    2012: 34_622_078_740,
    2013: 38_231_088_980,
    2014: 42_033_212_910,
    2015: 36_804_988_670,
    2016: 39_515_775_492,
    2017: 52_162_471_046,
    2018: 73_980_573_140,
    2019: 75_028_000_046,
    2020: 135_683_471_517,
    2021: 182_439_642_506,
    2022: 147_749_119_851,
    2023: 196_527_434_884,
    2024: 294_370_101_471,
    2025: 385_205_839_095,
}
TOP10: dict[int, tuple[tuple[str, str, int], ...]] = {
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
    2011: (
        ("apple", "AAPL", 3224896392),
        ("microsoft", "MSFT", 1903255615),
        ("oracle", "ORCL", 1326819462),
        ("google", "GOOG", 1193294216),
        ("intel", "INTC", 1022280457),
        ("amazon", "AMZN", 895697962),
        ("cisco systems", "CSCO", 777517746),
        ("qualcomm", "QCOM", 745402050),
        ("amgen", "AMGN", 463444673),
        ("comcast", "CMCSA", 398547179),
    ),
    2012: (
        ("apple", "AAPL", 6826243955),
        ("microsoft", "MSFT", 2724618091),
        ("google", "GOOG", 2157132854),
        ("oracle", "ORCL", 1677961403),
        ("amazon", "AMZN", 1254700436),
        ("intel", "INTC", 1238327501),
        ("qualcomm", "QCOM", 1161654481),
        ("cisco systems", "CSCO", 1116037003),
        ("comcast", "CMCSA", 825582689),
        ("kraft foods", "MDLZ", 800845546),
    ),
    2013: (
        ("apple", "AAPL", 4589062963),
        ("microsoft", "MSFT", 2939862888),
        ("google", "GOOG", 2542230673),
        ("amazon", "AMZN", 1513410829),
        ("cisco systems", "CSCO", 1326142745),
        ("qualcomm", "QCOM", 1224289825),
        ("intel", "INTC", 1209839764),
        ("comcast", "CMCSA", 1020659094),
        ("gilead sciences", "GILD", 1019096326),
        ("facebook", "META", 967469670),
    ),
    2014: (
        ("apple", "AAPL", 5627992929),
        ("microsoft", "MSFT", 3563678272),
        ("google", "GOOG", 3377102454),
        ("intel", "INTC", 1608264489),
        ("gilead sciences", "GILD", 1501203035),
        ("facebook", "META", 1485827591),
        ("amazon", "AMZN", 1389813132),
        ("cisco systems", "CSCO", 1202865088),
        ("qualcomm", "QCOM", 1169078373),
        ("comcast", "CMCSA", 1078475455),
    ),
    2015: (
        ("apple", "AAPL", 4849323617),
        ("google", "GOOG", 3039967765),
        ("microsoft", "MSFT", 2729071954),
        ("amazon", "AMZN", 1845780128),
        ("facebook", "META", 1566186647),
        ("gilead sciences", "GILD", 1110972228),
        ("intel", "INTC", 1104654509),
        ("comcast", "CMCSA", 1089855810),
        ("cisco systems", "CSCO", 1029248141),
        ("amgen", "AMGN", 808593131),
    ),
    2016: (
        ("apple", "AAPL", 4269364565),
        ("alphabet", "GOOG", 3533307260),
        ("microsoft", "MSFT", 3145785581),
        ("amazon", "AMZN", 2781986757),
        ("facebook", "META", 2088308842),
        ("intel", "INTC", 1251697248),
        ("cisco systems", "CSCO", 1118161910),
        ("comcast", "CMCSA", 1116979583),
        ("amgen", "AMGN", 874899434),
        ("kraft heinz", "KHC", 763878250),
    ),
    2017: (
        ("apple", "AAPL", 6074408158),
        ("alphabet", "GOOG", 4759489527),
        ("microsoft", "MSFT", 4377954437),
        ("amazon", "AMZN", 3523903410),
        ("facebook", "META", 3090514242),
        ("comcast", "CMCSA", 1381182765),
        ("intel", "INTC", 1365395566),
        ("cisco systems", "CSCO", 1283097497),
        ("amgen", "AMGN", 1038120412),
        ("celgene", "CELG", 870556336),
    ),
    2018: (
        ("apple", "AAPL", 9203919588),
        ("amazon", "AMZN", 8247064068),
        ("microsoft", "MSFT", 7403389233),
        ("alphabet", "GOOG", 6570719091),
        ("facebook", "META", 3348141971),
        ("cisco systems", "CSCO", 1931393129),
        ("intel", "INTC", 1840726459),
        ("nvidia", "NVDA", 1442312387),
        ("netflix", "NFLX", 1375267086),
        ("comcast", "CMCSA", 1366793989),
    ),
    2019: (
        ("microsoft", "MSFT", 8621301880),
        ("apple", "AAPL", 8220421303),
        ("amazon", "AMZN", 6974990535),
        ("alphabet", "GOOG", 6518236289),
        ("facebook", "META", 3512180307),
        ("intel", "INTC", 2057483502),
        ("cisco systems", "CSCO", 1906361819),
        ("comcast", "CMCSA", 1842831992),
        ("pepsico", "PEP", 1727710070),
        ("adobe", "ADBE", 1208675520),
    ),
    2020: (
        ("apple", "AAPL", 18207146858),
        ("microsoft", "MSFT", 14631699205),
        ("amazon", "AMZN", 14498163551),
        ("alphabet", "GOOG", 9151771786),
        ("facebook", "META", 5788387826),
        ("tesla", "TSLA", 4687602648),
        ("nvidia", "NVDA", 3915655800),
        ("adobe", "ADBE", 2758436286),
        ("paypal holdings", "PYPL", 2710810262),
        ("netflix", "NFLX", 2585846141),
    ),
    2021: (
        ("apple", "AAPL", 20096409443),
        ("microsoft", "MSFT", 18202601776),
        ("amazon", "AMZN", 14293984310),
        ("alphabet", "GOOG", 14247756547),
        ("tesla", "TSLA", 8260012812),
        ("facebook", "META", 6951148407),
        ("nvidia", "NVDA", 6771204622),
        ("paypal holdings", "PYPL", 3997544820),
        ("adobe", "ADBE", 3585946864),
        ("netflix", "NFLX", 3531828844),
    ),
    2022: (
        ("apple", "AAPL", 19656918598),
        ("microsoft", "MSFT", 15372955772),
        ("alphabet", "GOOG", 10320546263),
        ("amazon", "AMZN", 10188716755),
        ("tesla", "TSLA", 7296495144),
        ("meta platforms", "META", 4100934513),
        ("nvidia", "NVDA", 3764864600),
        ("pepsico", "PEP", 3308021139),
        ("costco wholesale", "COST", 3071805596),
        ("broadcom", "AVGO", 2640370735),
    ),
    2023: (
        ("apple", "AAPL", 21263319286),
        ("microsoft", "MSFT", 18635603837),
        ("alphabet", "GOOG", 12243336640),
        ("amazon", "AMZN", 10418950838),
        ("nvidia", "NVDA", 8534942705),
        ("meta platforms", "META", 7429628902),
        ("tesla", "TSLA", 6308906206),
        ("broadcom", "AVGO", 5839479070),
        ("costco wholesale", "COST", 4268428729),
        ("pepsico", "PEP", 3973677906),
    ),
    2024: (
        ("apple", "AAPL", 26605881123),
        ("microsoft", "MSFT", 24021355071),
        ("nvidia", "NVDA", 22372742557),
        ("broadcom", "AVGO", 15506954947),
        ("meta platforms", "META", 14942690402),
        ("amazon", "AMZN", 14687525788),
        ("alphabet", "GOOG", 14310749310),
        ("tesla", "TSLA", 9501626129),
        ("costco wholesale", "COST", 7623988667),
        ("netflix", "NFLX", 5904684098),
    ),
    2025: (
        ("nvidia", "NVDA", 38072230383),
        ("microsoft", "MSFT", 32329487008),
        ("apple", "AAPL", 31731602289),
        ("alphabet", "GOOG", 22979831035),
        ("broadcom", "AVGO", 21550731714),
        ("amazon", "AMZN", 19663790280),
        ("tesla", "TSLA", 13611033167),
        ("meta platforms", "META", 13375492801),
        ("netflix", "NFLX", 10524994972),
        ("palantir technologies", "PLTR", 8570984181),
    ),
}
ROW_BY_ROW = frozenset({"0001193125-16-806695", "0001193125-18-001208", "0001193125-18-355790"})
SKIP = re.compile(es.SKIP.pattern.replace("cost", r"\bcost\b"), re.IGNORECASE)
# 需求 2.3 的只印下限，显示被原解析价格规则丢掉的大行，不影响名单和判定。
PRICED_OUT_MIN = 100_000_000
LISTS: dict[int, tuple[str, ...]] = {
    year: tuple(issuer for _, issuer, _ in rows) for year, rows in TOP10.items()
}
VERDICTS = es.VERDICTS
LINE_NAMES = es.LINE_NAMES
TOO_FEW_SENTENCE = es.TOO_FEW_SENTENCE
SHIFT_SENTENCE = es.SHIFT_SENTENCE
LINE_SHORT = {"line1": "线 1", "line2": "线 2", "line3": "线 3"}
HOLDS_SENTENCE = (
    "2004–2010、2011–2026 两段都看到 QQQ 的坏日子扎堆在大权重股财报后第一天，躲开的代价测不出为正"
)
FAMILY_SENTENCE = (
    "财报冲击：2011–2026 没过（{lines}；C {c}，p {p}，差 {diff}，区间 {low} 到 {high}）；"
    "这一族的历史证据收掉"
)
SHIFT_HOLDS = "照平移读是 not_holds"
HOLDS_NEXT = (
    "当天告诉 Baker；11-27 续期卡写这一句，连同平移对照；第 7 期第 3.4 题的两条账本线照常记"
)
NOT_HOLDS_NEXT = "续期卡问第 7 期第 3.4 题的两条按财报日程降仓的账本线还留不留"
NOT_AGAIN = "不再用别的历史段问这件事"
LIMITS = (
    "Wilson 区间不算聚集，只印，不作判定线",
    "8-K 接收时间是发布时间的上界",
    "名单按年定，年内进指数的大公司要等下一份",
    "半天交易日按 16:00 算",
    "只看得出约 2 倍以上的集中",
    "快照止于 2026-10-12，之后的财报不在表里",
    "前 5 大占 QQQ 的 33.8%–47.0%（2013 年最低，2018 年最高），比 2004–2010 的 26%–36% 高，"
    "一家财报对 QQQ 当天的影响也大",
    "2020-03、2022 年的坏日子成堆，是宏观下跌",
    "同一天几家一起发（2020 年以后常见）算一天，分不出是哪家",
    "持仓解析行里混着几行不是持仓的小行，不影响名单",
    "前 10 敏感度里 TEVA 2011-01..09 跳过、不往下补",
    "这一段对循环线程不是没看过的历史（入场、出场族的报告看过 2011 年以后的 QQQ）",
)
CHECK = "earnings_after_2010_check.json"
EVENTS = "earnings_after_2010_events.jsonl"
SUMMARY = "earnings_after_2010_table.json"
REPORT = "earnings_after_2010_table.md"
SETS = es.SETS
EVENT_COLUMNS = es.EVENT_COLUMNS
Record = dict[str, Any]
app = typer.Typer()


class TableError(ValueError):
    """这张表自己的汇总错误，不带数据的值。"""


@dataclass(frozen=True)
class Inputs:
    snapshot: Any
    qqq: pd.Series
    sources: Record
    window_read: Record


@dataclass(frozen=True)
class Parsed:
    total: int
    rows: tuple[tuple[str, int, int], ...]
    priced_out: tuple[tuple[str, int, int], ...]


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
    sources: Record
    window_read: Record


def row_holdings(row: Sequence[str]) -> list[tuple[str, int, int]]:
    name: list[str] = []
    numbers: list[int] = []
    for word in " ".join(row).split():
        token = word.replace("$", "")
        if not token:
            continue
        if es.NUMBER.fullmatch(token):
            numbers.append(int(re.sub(r"\D", "", token)))
        else:
            name.append(word)
    if name and len(numbers) >= 2:
        return [(" ".join(name), numbers[-2], numbers[-1])]
    return []


def parse_document(document: str, payload: bytes, *, row_by_row: bool) -> Parsed:
    totals: list[int] = []
    found: dict[tuple[str, int], tuple[str, int, int]] = {}
    priced_out: dict[tuple[str, int], tuple[str, int, int]] = {}
    for row in es.cells(
        payload.decode("utf-8", errors="replace"), document.lower().endswith((".htm", ".html"))
    ):
        text = " ".join(row)
        if "total investments" in text.lower():
            numbers = [
                int(re.sub(r"\D", "", token))
                for token in text.replace("$", " ").split()
                if es.NUMBER.fullmatch(token)
            ]
            if numbers:
                totals.append(numbers[-1])
            continue
        for raw, shares, value in row_holdings(row) if row_by_row else es.holdings(row):
            name = re.sub(r"[\s*.]+$", "", raw).strip(" |")
            if not re.search(r"[a-z]{3}", name, re.I) or SKIP.search(name) or value < es.MIN_VALUE:
                continue
            key = (es.normalised(name), value)
            if not shares or not es.PRICE_RANGE[0] <= value / shares <= es.PRICE_RANGE[1]:
                if value >= PRICED_OUT_MIN:
                    priced_out[key] = (name, shares, value)
                continue
            found[key] = (name, shares, value)
    if not totals:
        raise TableError("no total investments row")
    total = totals[0]
    rows = tuple(
        sorted(
            (row for row in found.values() if row[2] < es.MAX_SHARE * total),
            key=lambda row: (-row[2], es.normalised(row[0])),
        )
    )
    excluded = tuple(sorted(priced_out.values(), key=lambda row: (-row[2], es.normalised(row[0]))))
    return Parsed(total, rows, excluded)


def holdings_lists(
    documents: Mapping[int, tuple[str, bytes]],
    *,
    accessions: Mapping[int, str] | None = None,
    top10: Mapping[int, tuple[tuple[str, str, int], ...]] = TOP10,
    rows: Mapping[int, int] = ROWS,
    totals: Mapping[int, int] = TOTALS,
) -> tuple[dict[int, tuple[str, ...]], list[Record]]:
    if accessions is None:
        accessions = {year: accession for year, (accession, _) in HOLDINGS.items()}
    lists: dict[int, tuple[str, ...]] = {}
    report: list[Record] = []
    for year in sorted(top10):
        prefix = f"fiscal year {year}: "
        if year not in documents or year not in accessions:
            raise TableError(prefix + "no holdings document")
        accession = accessions[year]
        document, payload = documents[year]
        row_by_row = accession in ROW_BY_ROW
        try:
            parsed = parse_document(document, payload, row_by_row=row_by_row)
        except TableError as exc:
            raise TableError(prefix + str(exc)) from exc
        if len(parsed.rows) != rows[year]:
            raise TableError(prefix + "the parsed row count is not the probe's")
        if parsed.total != totals[year]:
            raise TableError(prefix + "total investments is not the pinned total")
        share = sum(value for _, _, value in parsed.rows) / parsed.total
        if not es.SUM_RANGE[0] <= share <= es.SUM_RANGE[1]:
            raise TableError(prefix + "the parsed rows do not add up to the total")
        merged: Counter[str] = Counter()
        counts: Counter[str] = Counter()
        for name, _, value in parsed.rows:
            key = es.issuer_key(name)
            merged[key] += value
            counts[key] += 1
        actual = sorted(merged.items(), key=lambda item: (-item[1], item[0]))[: len(top10[year])]
        if actual != [(key, value) for key, _, value in top10[year]]:
            raise TableError(prefix + "the merged top ten is not the pinned list")
        lists[year] = tuple(issuer for _, issuer, _ in top10[year])
        report.append(
            {
                "fiscal_year": year,
                "accession": accession,
                "document": document,
                "rule": "row_by_row" if row_by_row else "groups",
                "rows": len(parsed.rows),
                "total": parsed.total,
                "sum_share": share,
                "same_key": [
                    {"key": key, "rows": count}
                    for key, count in sorted(counts.items())
                    if count > 1
                ],
                "class_rows": sum(bool(es.CLASS.search(name)) for name, _, _ in parsed.rows),
                "priced_out": [
                    {"name": name, "shares": shares, "value": value}
                    for name, shares, value in parsed.priced_out
                ],
                "top10": [key for key, _ in actual],
            }
        )
    return lists, report


def list_year(day: pd.Timestamp) -> int:
    return min(es.list_year(day), LAST_LIST)


def results(filings: pd.DataFrame) -> pd.DataFrame:
    by_cik = {cik: issuer for issuer, ciks in ISSUERS.items() for cik in ciks}
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
    found["event_day"] = es.event_days(found["accepted"], calendar)
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


def shift_rows(
    days: pd.DatetimeIndex, window: pd.DatetimeIndex, bad: pd.DatetimeIndex
) -> list[Record]:
    return [
        {**row, "back_in_e": len(es.shifted(days, window, row["shift"]).intersection(days))}
        for row in es.shift_rows(days, window, bad)
    ]


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


def months_in_top(issuer: str, lists: Mapping[int, tuple[str, ...]]) -> int:
    return sum(
        issuer in lists[list_year(month.to_timestamp())][: es.TOP]
        for month in pd.period_range(FIRST_DAY, LAST_DAY, freq="M")
    )


def issuer_rows(
    found: pd.DataFrame, bad: pd.DatetimeIndex, lists: Mapping[int, tuple[str, ...]]
) -> list[Record]:
    out: list[Record] = []
    for issuer in TOP5_IDS:
        mine = found[found["id"] == issuer]
        out.append(
            {
                "id": issuer,
                "months": months_in_top(issuer, lists),
                "events": len(mine),
                "bad_events": int(mine["event_day"].isin(bad).sum()),
            }
        )
    return out


def hit_rows(found: pd.DataFrame, returns: pd.Series, bad: pd.DatetimeIndex) -> list[Record]:
    return [
        {
            "day": _date(day),
            "ids": sorted(set(found.loc[found["event_day"] == day, "id"])),
            "qqq_return": float(returns.loc[day]),
        }
        for day in es.days_of(found).intersection(bad).sort_values()
    ]


def sensitivity(built: Built, bad: pd.DatetimeIndex) -> Record:
    out: Record = {}
    for name in ("top3", "top10", "first_per_quarter", "after_close"):
        days = es.days_of(built.sets[name])
        found = es.welch(built.returns, days)
        out[name] = {
            **es.concentration(days, built.window, bad),
            "welch_low": None if found is None else found["low"],
        }
    for share in es.SENSITIVE_BAD:
        out[f"bad_{share}"] = es.concentration(
            es.days_of(built.sets["top5"]), built.window, es.bad_days(built.returns, share)
        )
    return out


def judge(main: Record, shuffled: Record | None, found: Record | None) -> Record:
    # 三条线及样本不足的判法直接取原模块；这里只换登记好的结论文字。
    decision = es.judge(main, shuffled, found)
    if decision["verdict"] == "too_few":
        return decision
    sentence = HOLDS_SENTENCE
    if decision["failed"]:
        if shuffled is None or found is None:
            raise TableError("missing measures for the three lines")
        sentence = FAMILY_SENTENCE.format(
            lines="、".join(LINE_SHORT[name] for name in decision["failed"]),
            c=f"{main['c']:.2f}",
            p=f"{shuffled['p']:.3f}",
            diff=f"{100 * found['diff']:+.3f}%",
            low=f"{100 * found['low']:+.3f}%",
            high=f"{100 * found['high']:+.3f}%",
        )
    return {**decision, "sentence": sentence}


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


def window_days(calendar: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return calendar[(calendar >= FIRST_DAY) & (calendar <= LAST_DAY)]


def build(inputs: Inputs) -> Built:
    accessions = {
        document.fiscal_year: document.accession
        for document in inputs.snapshot.spec.holdings_documents
    }
    lists, holdings_report = holdings_lists(inputs.snapshot.documents, accessions=accessions)
    calendar = pd.DatetimeIndex(inputs.qqq.index)
    window = window_days(calendar)
    found = results(inputs.snapshot.filings)
    sets = {f"top{top}": events(found, calendar, window, lists, top) for top in (3, es.TOP, 10)}
    sets["first_per_quarter"] = es.first_per_quarter(sets["top5"])
    sets["after_close"] = es.after_close(sets["top5"])
    return Built(
        lists=lists,
        holdings=holdings_report,
        calendar=calendar,
        window=window,
        returns=inputs.qqq.reindex(window),
        filings=len(inputs.snapshot.filings),
        results=found,
        sets=sets,
        sources=inputs.sources,
        window_read=inputs.window_read,
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
            "days": len(es.days_of(built.sets[name])),
            "repeated_days": int((built.sets[name]["event_day"].value_counts() > 1).sum()),
        }
        for name in SETS
    }
    nbad = es.bad_count(len(built.window), es.BAD_SHARE)
    expected = nbad * counts["top5"]["days"] / len(built.window)
    top5, top10 = built.sets["top5"], built.sets["top10"]
    hours = pd.DatetimeIndex(top5["accepted"]).tz_convert(es.EASTERN).hour
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
        "window_read": built.window_read,
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
        "too_few": counts["top5"]["days"] < es.TOO_FEW_EVENTS or expected < es.TOO_FEW_EXPECTED,
        "per_list": per_list,
        "hours": {f"{hour:02d}": int((hours == hour).sum()) for hour in range(24)},
        "evening_share": float((hours >= 16).mean()) if len(hours) else None,
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
            "accepted_eastern": row["accepted"].tz_convert(es.EASTERN).isoformat(),
            "event_day": _date(row["event_day"]),
        }
        for row in built.sets["top5"].to_dict("records")
    ]


def read(inputs: Inputs) -> tuple[Record, list[Record]]:
    built = build(inputs)
    checked = check_summary(built)
    days = es.days_of(built.sets["top5"])
    bad = es.bad_days(built.returns)
    main = es.concentration(days, built.window, bad)
    result: Record = {
        **_identity(),
        "window_read": built.window_read,
        "check": checked,
        "main": main,
        **dict.fromkeys(("shuffle", "welch", "shifts", "years", "issuers", "hits", "sensitivity")),
    }
    if checked["too_few"]:
        result["decision"] = {**judge(main, None, None), "shift_note": None}
    else:
        result.update(
            shuffle=es.shuffle(days, built.window, bad),
            welch=es.welch(built.returns, days, bad),
            shifts=shift_rows(days, built.window, bad),
            years=year_rows(days, built.window, bad),
            issuers=issuer_rows(built.sets["top5"], bad, built.lists),
            hits=hit_rows(built.sets["top5"], built.returns, bad),
            sensitivity=sensitivity(built, bad),
        )
        result["decision"] = {
            **judge(main, result["shuffle"], result["welch"]),
            "shift_note": es.shift_note(main["c"], result["shifts"]),
        }
    return result, event_rows(built)


def render(result: Record) -> str:
    checked, main, decision = result["check"], result["main"], result["decision"]
    window = checked["window"]
    lines = [
        "# 大权重股财报后第一天和 QQQ 最差的日子：QQQ 2011 年以后读一次",
        "",
        f"运行 {RUN_ID}；决定 `{OWNER_DECISION}`；窗口 `{WINDOW_ID}`，读一次。",
        "规则同 TRADING-2565-99，一字不改；不用任何个股的价格。",
        f"财报快照 `{EARNINGS_SNAPSHOT_ID}`；QQQ 快照 `{QQQ_SNAPSHOT_ID}`；列 `{QQQ_COLUMN}`。",
        "只印，不是探针、不是试验、不进账本，不模拟躲开财报日的仓位线。",
        f"研究窗口：`{WINDOW_ID}`；请求 {_date(FIRST_DAY)}..{_date(LAST_DAY)}；"
        f"实际 {window['first']}..{window['last']}，{window['days']} 天。",
        "数据质量与来源：" + es._display(checked["sources"]),
        "主名单取每年九月底前 5 大，10 月起生效；2025 年名单用到窗口终点；外国公司占位但不补名。",
        "判定线（读取前固定）：" + "；".join(LINE_NAMES.values()) + "。",
        "",
        f"## 判定：{decision['verdict']}",
        "",
        decision["sentence"] + "。",
    ]
    if decision["verdict"] == "holds":
        lines.append(HOLDS_NEXT + "。")
        if decision["shift_note"]:
            lines.append(f"平移对照提示：{SHIFT_SENTENCE}；续期卡并排写「{SHIFT_HOLDS}」。")
    elif decision["verdict"] == "not_holds":
        lines += [
            "没过的线：" + "；".join(LINE_NAMES[name] for name in decision["failed"]) + "。",
            NOT_HOLDS_NEXT + "。",
            NOT_AGAIN + "。",
        ]
        if decision["shift_note"]:
            lines.append("平移对照提示：" + SHIFT_SENTENCE + "。")
    lines += [
        "",
        "## 集中倍数",
        "",
        es._table(
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
            "## 季内打乱",
            "",
            "每个事件日按季有放回抽取，重复日合并；p 含一次加一修正。",
            es._table([result["shuffle"]]),
            "",
            "## 躲开的代价",
            "",
            "E 减其他日的日收益差；Welch 区间，log_sum_e 是事件日日收益 log1p 之和，"
            "不模拟仓位，也没有交易成本假设。",
            es._table([result["welch"]]),
            "",
            "## E ∩ B 的每一天（只印）",
            "",
            es._table(result["hits"]),
            "",
            "## 每年",
            "",
            es._table(result["years"]),
            "",
            "## 每家公司（前 5 的 13 家）",
            "",
            es._table(result["issuers"]),
            "",
            "## 平移对照（只印）",
            "",
            "back_in_e 是平移以后落回 E 的天数。",
            es._table(result["shifts"]),
            "",
            "## 敏感度（只印，不跑打乱）",
            "",
            es._table([{"set": name, **row} for name, row in result["sensitivity"].items()]),
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
    expected = {issuer: (ciks, None) for issuer, ciks in ISSUERS.items()}
    if {issuer.id: (issuer.ciks, issuer.fmp_ticker) for issuer in spec.issuers} != expected:
        problems.append("the configured issuers are not the module's")
    if tuple(issuer.id for issuer in spec.foreign) != FOREIGN:
        problems.append("the configured foreign issuers are not the module's")
    if {
        document.fiscal_year: (document.accession, document.document)
        for document in spec.holdings_documents
    } != HOLDINGS:
        problems.append("the configured holdings documents are not the module's")
    if (
        spec.window_id != WINDOW_ID
        or spec.owner_decision != OWNER_DECISION
        or spec.price_start is not None
        or pd.Timestamp(spec.filing_start) != FILING_START
        or pd.Timestamp(spec.data_end) != LAST_DAY
    ):
        problems.append("the configured window is not the module's")
    return problems


def window_read() -> Record:
    try:
        registered = ssp.registered_qqq_window(load_settings())
    except (OSError, ValueError, KeyError) as exc:
        raise TableError(f"window {WINDOW_ID}: {es._first_line(exc)}") from exc
    return {**registered, "owner_decision": OWNER_DECISION, "reads": 1}


def load_inputs(root: Path = PROJECT_ROOT, qqq_root: Path = qlh.DEFAULT_SNAPSHOT_ROOT) -> Inputs:
    registered = window_read()
    problems = spec_problems(ee.load_spec(SPEC_PATH))
    if problems:
        raise TableError("; ".join(problems))
    try:
        snapshot = ee.load_snapshot(root, EARNINGS_SNAPSHOT_ID)
    except (OSError, ValueError, KeyError) as exc:
        raise TableError(
            f"earnings snapshot {EARNINGS_SNAPSHOT_ID}: {es._first_line(exc)}"
        ) from exc
    problems = spec_problems(snapshot.spec)
    if problems:
        raise TableError(f"earnings snapshot {EARNINGS_SNAPSHOT_ID}: " + "; ".join(problems))
    if len(snapshot.prices):
        raise TableError(f"earnings snapshot {EARNINGS_SNAPSHOT_ID}: has stock prices")
    try:
        daily, report = qlh.load_daily(qlh.resolve_snapshot_dir(qqq_root, QQQ_SNAPSHOT_ID))
    except (OSError, ValueError, KeyError) as exc:
        raise TableError(f"QQQ snapshot {QQQ_SNAPSHOT_ID}: {es._first_line(exc)}") from exc
    qqq = qqq_series(daily)
    window = window_days(pd.DatetimeIndex(qqq.index))
    if len(window) != WINDOW_DAYS or window[0] != FIRST_DAY or window[-1] != LAST_DAY:
        raise TableError(f"QQQ snapshot {QQQ_SNAPSHOT_ID}: the window is not the 3,967 pinned days")
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
            "first": _date(qqq.index[0]),
            "last": _date(qqq.index[-1]),
            "days": len(qqq),
        },
    }
    return Inputs(snapshot, qqq, sources, registered)


def _error_summary(exc: Exception) -> str:
    return es._first_line(exc) if isinstance(exc, TableError) else type(exc).__name__


def _load() -> Inputs:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_after_2010: 数据载入失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc


@app.callback()
def cli() -> None:
    """只印财报冲击表，QQQ 2011 年以后读一次，不模拟仓位。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    if (out / CHECK).exists():
        typer.echo("earnings_shock_after_2010: 已有检查结果，拒绝覆盖。")
        raise typer.Exit(1)
    inputs = _load()
    try:
        summary = check_summary(build(inputs))
    except (ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_after_2010: 建数据失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc
    write_check(summary, out)
    typer.echo(
        f"earnings_shock_after_2010: W {summary['window']['days']} 天；前 5 的事件 "
        f"{summary['sets']['top5']['rows']} 个、{summary['sets']['top5']['days']} 天；"
        "名单 16 年都对上"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    if any((out / name).exists() for name in (CHECK, EVENTS, SUMMARY, REPORT)):
        typer.echo("earnings_shock_after_2010: 已有结果，拒绝再读。")
        raise typer.Exit(1)
    inputs = _load()
    try:
        result, rows = read(inputs)
    except (ValueError, KeyError) as exc:
        typer.echo(f"earnings_shock_after_2010: 建数据失败：{_error_summary(exc)}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, rows, out)
    typer.echo(f"earnings_shock_after_2010: 判定 {result['decision']['verdict']}")


if __name__ == "__main__":
    app()
