"""Acceptance tests for the earnings-shock print-only table (TRADING-2565-99).

Written before the module (the loop thread's tests, pi's module), from requirement v1.1 in the
project files (`knowledge/claude/research-review/earnings-shock-table-requirements-2026-10-30.md`,
sections 2.1, 2.3, 3, 4, 6 and 7) and section 24 of the period-6 charter. Every input is
synthetic: no snapshot, no QQQ return and no price is read.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.data import QQQ_SERIES_COLUMNS
from ai_trading_system.research_bench.prereg import load_prereg
from ai_trading_system.research_data import earnings_events as ee
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import earnings_shock_table as es

# E1: the earnings snapshot the research session fetched after TRADING-2565-98
# (filled in after the fetch).
E1 = "ern-a9917351bced"
MODULE = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes" / "earnings_shock_table.py"
# qqq_long_history is only imported; its bytes at TRADING-2565-98 (main 970e496f).
QLH_SHA256 = "fb3e7b6db889b0e22151487e15d37bc64aa26418bcad465dd7ad1a82baf34710"

EASTERN = "America/New_York"
# A synthetic QQQ calendar: weekdays from 2003-09-01, without 2004-01-01.
CAL = pd.bdate_range("2003-09-01", "2010-12-31").difference(pd.DatetimeIndex(["2004-01-01"]))
W = CAL[CAL >= pd.Timestamp("2004-01-01")]
DOCS = {
    2003: "d13831.txt",
    2004: "c36210.txt",
    2005: "a2167148zn-30b_2.txt",
    2006: "a06-24326_3n30b2.htm",
    2007: "a08-3893_3n30b2.htm",
    2008: "a08-28674_2n30b2.htm",
    2009: "a09-31668_2n30b2.htm",
    2010: "a10-18275_1n30b2.htm",
}
# A schedule name that gives each pinned issuer key.
NAMES = {
    "microsoft": "Microsoft Corp.",
    "intel": "Intel Corp.",
    "cisco systems": "Cisco Systems, Inc.*",
    "qualcomm": "QUALCOMM, Inc.",
    "amgen": "Amgen, Inc.*",
    "dell": "Dell, Inc.*",
    "comcast": "Comcast Corp., Class A*",
    "ebay": "eBay, Inc.*",
    "nextel communications": "Nextel Communications, Inc., Class A*",
    "interactivecorp": "InterActiveCorp*",
    "starbucks": "Starbucks Corp.*",
    "apple computer": "Apple Computer, Inc.*",
    "symantec": "Symantec Corp.*",
    "google": "Google, Inc., Class A*",
    "oracle": "Oracle Corp.*",
    "research in motion": "Research In Motion Ltd.*",
    "gilead sciences": "Gilead Sciences, Inc.*",
    "apple": "Apple, Inc.*",
    "teva pharmaceutical industries": "Teva Pharmaceutical Industries Ltd. ADR",
    "amazon": "Amazon.com, Inc.*",
}
TICKERS = sorted({ticker for _, ticker in es.ISSUERS.values() if ticker})
ACCESSIONS = itertools.count(1)


# --- builders ------------------------------------------------------------------------------------


def _letters(k: int) -> str:
    return "".join(chr(ord("a") + (k // 26**i) % 26) for i in (2, 1, 0)).capitalize()


def _txt(rows: list[tuple[str, int, int]], total: int) -> bytes:
    lines = [
        "SCHEDULE OF INVESTMENTS",
        "September 30, 2004",
        "",
        "Common Stocks      Shares      Value",
    ]
    for k in range(0, len(rows) - 1, 2):
        (n1, s1, v1), (n2, s2, v2) = rows[k], rows[k + 1]
        if k % 4 == 0:
            lines.append(f"{n1} {'.' * 9} {s1:,}   $ {v1:,}")
            lines.append(f"{n2} {'.' * 5} {s2:,}   {v2:,}")
        else:
            lines.append(f"{n1}  {s1:,}  {v1:,}      {n2}  {s2:,}  ${v2:,}")
    if len(rows) % 2:
        name, shares, value = rows[-1]
        lines.append(f"{name} ..... {shares:,}   {value:,}")
    lines.append(f"Total Investments (Cost $19,000,000,000) ........ $ {total:,}")
    lines.append(f"Total Investments again ........ $ {total + 7:,}")
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


def _htm(rows: list[tuple[str, int, int]], total: int) -> bytes:
    parts = [
        "<html><head><style>td { color: red; }</style>",
        "<script>var x = '<tr><td>Phantom Holding Inc.</td><td>1,000,000</td>"
        "<td>50,000,000</td></tr>';</script></head><body>",
        "<p>SCHEDULE OF INVESTMENTS</p><p>September&#160;30, 2008</p><table>",
        "<tr><th>Common Stocks</th><th>Shares</th><th>Value</th></tr>",
    ]
    for name, shares, value in rows:
        parts.append(
            f"<tr><td><font>{name.replace('&', '&amp;')}</font></td>"
            f"<td align=right>{shares:,}</td><td>$</td><td>{value:,}&nbsp;</td></TR>"
        )
    parts.append(
        f"<tr><td><b>Total Investments</b> (Cost&#160;$15,000,000,000)</td><td>$</td>"
        f"<td>{total:,}</td></tr></table></body></html>"
    )
    return "\n".join(parts).encode("utf-8")


def _schedule(year: int) -> list[tuple[str, int, int]]:
    pinned = [(NAMES[key], value // 40, value) for key, _, value in es.TOP10[year]]
    extra = [("Net increase (decrease)", 107_800_000, 483_150_000)] if year == 2003 else []
    fillers = es.ROWS[year] - len(pinned) - len(extra)
    rest = es.TOTALS[year] - sum(row[2] for row in pinned + extra)
    base = rest // fillers
    filler = [
        (f"Filler {_letters(k)} Inc.*", (base + 1000 * k) // 50, base + 1000 * k)
        for k in range(fillers)
    ]
    return pinned + extra + filler


def _document(year: int) -> tuple[str, bytes]:
    rows, total = _schedule(year), es.TOTALS[year]
    name = DOCS[year]
    return name, (_txt(rows, total) if name.endswith(".txt") else _htm(rows, total))


DOCUMENTS = {year: _document(year) for year in DOCS}


def _sec_time(eastern: str) -> str:
    stamp = pd.Timestamp(eastern).tz_localize(EASTERN).tz_convert("UTC")
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _filing(
    issuer: str,
    eastern: str,
    *,
    form: str = "8-K",
    items: str = "2.02,9.01",
    accession: str | None = None,
) -> dict[str, Any]:
    return {
        "cik": es.ISSUERS[issuer][0],
        "accessionNumber": accession or f"0000000000-09-{next(ACCESSIONS):06d}",
        "form": form,
        "filingDate": eastern[:10],
        "acceptanceDateTime": _sec_time(eastern),
        "items": items,
    }


def _filings(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(ee.FILING_COLUMNS))


def _season() -> list[dict[str, Any]]:
    """Each of the nine top-5 companies reports once a quarter after the close."""

    rows = []
    for year in range(2004, 2011):
        for month in (1, 4, 7, 10):
            for k, issuer in enumerate(es.TOP5_IDS):
                rows.append((issuer, f"{year}-{month:02d}-{12 + 2 * k} 16:05", "8-K", "2.02,9.01"))
    rows.append(("INTC", "2006-01-17 16:10", "8-K/A", "2.02,9.01"))
    rows.append(("DELL", "2004-02-12 16:20", "8-K", "2.02,9.01"))
    rows.append(("GILD", "2008-04-17 16:01", "8-K", "2.02"))
    rows.append(("MSFT", "2005-07-21 16:30", "8-K", "8.01"))
    return [
        _filing(issuer, eastern, form=form, items=items, accession=f"0000000001-09-{k:06d}")
        for k, (issuer, eastern, form, items) in enumerate(rows, start=1)
    ]


def _returns(seed: int = 7, *, calendar: pd.DatetimeIndex = CAL) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(0.0004, 0.015, len(calendar)), index=calendar, name="r")


def _prices(scale: float = 1.0, *, missing: tuple[str, ...] = ()) -> pd.DataFrame:
    frames = []
    for k, ticker in enumerate(TICKERS):
        steps = 1 + 0.01 * np.sin(np.arange(len(CAL)) * (k + 1) / 7)
        frame = pd.DataFrame(
            {"ticker": ticker, "date": CAL, "adj_close": scale * 50 * np.cumprod(steps)}
        )
        frames.append(frame[~frame["date"].dt.strftime("%Y-%m-%d").isin(missing)])
    return pd.concat(frames, ignore_index=True)


def _snapshot(
    filings: list[dict[str, Any]] | None = None,
    prices: pd.DataFrame | None = None,
    documents: dict[int, tuple[str, bytes]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        snapshot_id=E1,
        spec=None,
        entities=(),
        filings=_filings(_season() if filings is None else filings),
        documents=DOCUMENTS if documents is None else documents,
        prices=_prices() if prices is None else prices,
    )


SOURCES = {"earnings": {"snapshot_id": E1}, "qqq": {"snapshot_id": es.QQQ_SNAPSHOT_ID}}


def _inputs(returns: pd.Series | None = None, snapshot: SimpleNamespace | None = None) -> es.Inputs:
    return es.Inputs(
        snapshot=_snapshot() if snapshot is None else snapshot,
        qqq=_returns() if returns is None else returns,
        sources=SOURCES,
    )


def _crashing(returns: pd.Series, days: pd.DatetimeIndex, value: float = -0.06) -> pd.Series:
    out = returns.copy()
    out[days] = value
    return out


BASE = _inputs()
BUILT = es.build(BASE)
TOP5_DAYS = es.days_of(BUILT.sets["top5"])
# Every event day is one of QQQ's worst days: all three lines pass.
HOLDS = _inputs(returns=_crashing(_returns(), TOP5_DAYS))


@pytest.fixture(scope="module")
def results() -> dict[str, tuple[dict[str, Any], list[dict[str, Any]]]]:
    return {"base": es.read(BASE), "holds": es.read(HOLDS)}


def _strings(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [s for item in value.values() for s in _strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return [value] if isinstance(value, str) else []


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("check must not compute this")


# --- the design ----------------------------------------------------------------------------------


def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert es.RUN_ID == "TRADING-2565-99"
    assert es.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-30:earnings_shock_table"
    assert es.EARNINGS_SNAPSHOT_ID == E1
    prereg = load_prereg(PROJECT_ROOT / "research" / "QQQ-Q3" / "prereg.yaml")
    assert es.QQQ_SNAPSHOT_ID == "qlh-20261013-0b0df53cb1a2" == prereg.data.snapshot_id
    assert es.QQQ_COLUMN == QQQ_SERIES_COLUMNS[prereg.data.qqq_series] == "etf_reference_return"
    assert (es.FIRST_DAY, es.LAST_DAY) == (pd.Timestamp("2004-01-01"), pd.Timestamp("2010-12-31"))
    assert es.EASTERN == EASTERN
    assert (es.TOP, es.SENSITIVE_TOPS) == (5, (3, 10))
    assert (es.BAD_SHARE, es.SENSITIVE_BAD) == (0.05, (0.025, 0.1))
    assert (es.Z, es.WELCH_Z) == (1.959963984540054, 1.96)
    assert (es.DRAWS, es.SEED) == (1000, 20261030)
    assert (es.SHIFTS, es.SHIFT_NOTE) == ((-5, -1, 1, 5), 0.3)
    assert (es.CRASH_STOCK, es.CRASH_QQQ, es.HORIZONS, es.MIN_CRASHES) == (
        -0.08,
        -0.01,
        (5, 21, 63),
        5,
    )
    assert (es.LINE_C, es.P_LINE, es.TOO_FEW_EVENTS, es.TOO_FEW_EXPECTED) == (2.0, 0.05, 80, 4)
    assert es.VERDICTS == ("too_few", "holds", "not_holds")
    assert (es.CHECK, es.EVENTS, es.SUMMARY, es.REPORT) == (
        "earnings_check.json",
        "earnings_events.jsonl",
        "earnings_table.json",
        "earnings_table.md",
    )


def test_the_pinned_list_is_the_probe_table() -> None:
    assert {
        year: [(key, issuer) for key, issuer, _ in rows] for year, rows in es.TOP10.items()
    } == {
        2003: [
            ("microsoft", "MSFT"),
            ("intel", "INTC"),
            ("cisco systems", "CSCO"),
            ("qualcomm", "QCOM"),
            ("amgen", "AMGN"),
            ("dell", "DELL"),
            ("comcast", "CMCSA"),
            ("ebay", "EBAY"),
            ("nextel communications", "NXTL"),
            ("interactivecorp", "IACI"),
        ],
        2004: [
            ("microsoft", "MSFT"),
            ("qualcomm", "QCOM"),
            ("ebay", "EBAY"),
            ("intel", "INTC"),
            ("cisco systems", "CSCO"),
            ("amgen", "AMGN"),
            ("nextel communications", "NXTL"),
            ("dell", "DELL"),
            ("comcast", "CMCSA"),
            ("starbucks", "SBUX"),
        ],
        2005: [
            ("microsoft", "MSFT"),
            ("qualcomm", "QCOM"),
            ("apple computer", "AAPL"),
            ("intel", "INTC"),
            ("amgen", "AMGN"),
            ("ebay", "EBAY"),
            ("cisco systems", "CSCO"),
            ("dell", "DELL"),
            ("symantec", "SYMC"),
            ("comcast", "CMCSA"),
        ],
        2006: [
            ("microsoft", "MSFT"),
            ("apple computer", "AAPL"),
            ("qualcomm", "QCOM"),
            ("google", "GOOG"),
            ("cisco systems", "CSCO"),
            ("intel", "INTC"),
            ("amgen", "AMGN"),
            ("oracle", "ORCL"),
            ("starbucks", "SBUX"),
            ("comcast", "CMCSA"),
        ],
        2007: [
            ("apple computer", "AAPL"),
            ("microsoft", "MSFT"),
            ("qualcomm", "QCOM"),
            ("google", "GOOG"),
            ("cisco systems", "CSCO"),
            ("research in motion", "RIMM"),
            ("intel", "INTC"),
            ("oracle", "ORCL"),
            ("ebay", "EBAY"),
            ("gilead sciences", "GILD"),
        ],
        2008: [
            ("apple", "AAPL"),
            ("qualcomm", "QCOM"),
            ("microsoft", "MSFT"),
            ("google", "GOOG"),
            ("cisco systems", "CSCO"),
            ("oracle", "ORCL"),
            ("gilead sciences", "GILD"),
            ("research in motion", "RIMM"),
            ("intel", "INTC"),
            ("amgen", "AMGN"),
        ],
        2009: [
            ("apple", "AAPL"),
            ("qualcomm", "QCOM"),
            ("microsoft", "MSFT"),
            ("google", "GOOG"),
            ("cisco systems", "CSCO"),
            ("oracle", "ORCL"),
            ("gilead sciences", "GILD"),
            ("intel", "INTC"),
            ("research in motion", "RIMM"),
            ("teva pharmaceutical industries", "TEVA"),
        ],
        2010: [
            ("apple", "AAPL"),
            ("qualcomm", "QCOM"),
            ("google", "GOOG"),
            ("microsoft", "MSFT"),
            ("oracle", "ORCL"),
            ("amazon", "AMZN"),
            ("cisco systems", "CSCO"),
            ("teva pharmaceutical industries", "TEVA"),
            ("intel", "INTC"),
            ("gilead sciences", "GILD"),
        ],
    }
    assert {year: [value for _, _, value in rows] for year, rows in es.TOP10.items()} == {
        2003: [
            2022752120,
            1226465098,
            949407624,
            779561761,
            759749733,
            617066299,
            573589268,
            501158446,
            495873629,
            487979978,
        ],
        2004: [
            1833314973,
            1357945537,
            803096452,
            798099276,
            774575074,
            592443732,
            586337720,
            579072804,
            475962100,
            445095405,
        ],
        2005: [
            1432856408,
            1333188912,
            937211521,
            794381302,
            692007007,
            628360976,
            618327489,
            455598036,
            428688678,
            423222044,
        ],
        2006: [
            1203775048,
            1176412636,
            929623224,
            687990506,
            643909909,
            533318616,
            500970870,
            497914860,
            459482413,
            452472453,
        ],
        2007: [
            2181170201,
            1112735899,
            999383666,
            941333511,
            841023269,
            646397630,
            619358637,
            540463953,
            462913575,
            436556338,
        ],
        2008: [
            1857626420,
            1124883914,
            1109346176,
            770634525,
            629765649,
            578225334,
            546184730,
            510237336,
            487729050,
            386615484,
        ],
        2009: [
            2551390786,
            993383660,
            874551876,
            804070360,
            534296372,
            480073429,
            456861345,
            422565010,
            422468508,
            410980196,
        ],
        2010: [
            4202892231,
            1015399656,
            916427258,
            847840959,
            654113927,
            542859200,
            519429544,
            449121149,
            435837585,
            341867716,
        ],
    }
    assert es.TOTALS == {
        2003: 20061689164,
        2004: 20376214952,
        2005: 19590693619,
        2006: 17957728469,
        2007: 20559646460,
        2008: 18091801491,
        2009: 17093021127,
        2010: 21232784912,
    }
    assert es.ROWS == {
        2003: 103,
        2004: 102,
        2005: 102,
        2006: 101,
        2007: 101,
        2008: 102,
        2009: 101,
        2010: 102,
    }
    assert es.IGNORE == frozenset({(2003, "net increase decrease", 483_150_000)})
    assert es.LISTS == {year: tuple(i for _, i, _ in rows) for year, rows in es.TOP10.items()}
    assert es.FOREIGN == ("RIMM", "TEVA")
    assert es.TOP5_IDS == ("MSFT", "INTC", "CSCO", "QCOM", "AMGN", "EBAY", "AAPL", "GOOG", "ORCL")
    assert set(es.TOP5_IDS) == {i for ids in es.LISTS.values() for i in ids[:5]}
    us = {i for ids in es.LISTS.values() for i in ids} - set(es.FOREIGN)
    assert us == set(es.ISSUERS)


def test_the_issuers_are_the_configured_ones() -> None:
    assert es.ISSUERS == {
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
    assert es.spec_problems(ee.load_spec()) == []


def test_spec_problems_name_each_mismatch() -> None:
    spec = ee.load_spec()
    issuers = [
        i.model_copy(update={"fmp_ticker": None}) if i.id == "MSFT" else i for i in spec.issuers
    ]
    assert es.spec_problems(spec.model_copy(update={"issuers": tuple(issuers)})) == [
        "the configured issuers are not the module's"
    ]
    assert es.spec_problems(spec.model_copy(update={"foreign": spec.foreign[:1]})) == [
        "the configured foreign issuers are not the module's"
    ]
    documents = spec.holdings_documents[1:]
    assert es.spec_problems(spec.model_copy(update={"holdings_documents": documents})) == [
        "the configured holdings years are not the module's"
    ]


def test_the_reading_texts_are_fixed_before_the_run() -> None:
    assert es.HOLDS_SENTENCE == "QQQ 的坏日子扎堆在大权重股财报后第一天，躲开的代价测不出为正"
    assert es.REASONS == {"spread": "不集中", "premium": "要付财报溢价"}
    assert es.FAMILY_SENTENCE == (
        "财报冲击：慢规则覆盖不到，按日程躲{reason}"
        "（C {c}，p {p}，差 {diff}，区间 {low} 到 {high}）；"
        "2004–2010 只看得出约 2 倍以上的集中，2 倍时约三成判得出（合成模拟，推断，上限约五成）"
    )
    assert es.NOT_AGAIN == "不再用 2004–2010 问"
    assert es.SHIFT_SENTENCE == "多半是财报季，不只是那一天"
    assert es.TOO_FEW_SENTENCE == "too_few：|E| < 80 或 |B| × f < 4，停，families.md 不记"
    assert es.LINE_NAMES == {
        "line1": "线 1：C ≥ 2",
        "line2": "线 2：季内打乱 p ≤ 0.05",
        "line3": "线 3：差的 Welch 95% 区间下沿 ≤ 0",
    }
    assert len(es.LIMITS) == 8 and "8-K 接收时间是发布时间的上界" in es.LIMITS


def test_the_imported_long_history_is_unchanged_and_nothing_is_fetched() -> None:
    assert hashlib.sha256(Path(qlh.__file__).read_bytes()).hexdigest() == QLH_SHA256
    source = MODULE.read_text(encoding="utf-8")
    assert "ee.is_results_filing(" in source and "ee.parse_acceptance(" in source
    for word in ("requests", "sec_filings", "fetch_snapshot", "ledger", "urlopen", "FMP_API_KEY"):
        assert word not in source, word


# --- holdings ------------------------------------------------------------------------------------


def test_clean_drops_tags_unescapes_and_collapses_spaces() -> None:
    assert es.clean("<td>Apple&#160;Inc.&nbsp;*</td>\n  <b>x</b>") == "Apple Inc. * x"
    assert es.clean("AT&amp;T\xa0Inc.") == "AT&T Inc."


def test_html_rows_split_on_tr_and_cells_on_td_and_th() -> None:
    text = (
        "<style>tr { x }</style><script>'<tr><td>No</td></tr>'</script>"
        "<tr><th>Name</th><TH>Shares</TH></tr><tr><td> Intel&#160;Corp. </td><td></td>"
        "<td>1,000</td></TR >"
    )
    assert es.cells(text, True) == [["Name", "Shares"], ["Intel Corp.", "1,000"], []]


def test_txt_rows_split_on_dot_leaders_and_two_or_more_spaces() -> None:
    text = "Intel Corp. ........ 1,000   $ 25,000\nA B  C\n\n  Dell, Inc.*..2"
    assert es.cells(text, False) == [
        ["Intel Corp.", "1,000", "$ 25,000"],
        ["A B", "C"],
        [],
        ["Dell, Inc.*..2"],
    ]


def test_holdings_read_a_name_then_the_last_two_numbers() -> None:
    assert es.holdings(["Intel Corp.", "1,000", "$ 25,000"]) == [("Intel Corp.", 1000, 25000)]
    two = ["Dell Inc.", "12,345", "678,901", "Cisco Systems", "(1,234)", "5678", "$", "$9,012"]
    assert es.holdings(two) == [("Dell Inc.", 12345, 678901), ("Cisco Systems", 5678, 9012)]
    assert es.holdings(["Only one", "1,234", "Next", "999"]) == []
    assert es.holdings(["1,234", "5,678", "Late Name"]) == []
    assert es.holdings(["Big Co", "1,000,000", "2,000,000", "3,000,000"]) == [
        ("Big Co", 2000000, 3000000)
    ]


def test_issuer_keys_merge_share_classes_and_company_words() -> None:
    assert es.issuer_key("Comcast Corp., Class A*") == "comcast"
    assert es.issuer_key("Comcast Corp., Special Class A") == "comcast"
    assert es.issuer_key("Amazon.com, Inc.") == "amazon"
    assert es.issuer_key("Apple Computer, Inc.") == "apple computer"
    assert es.issuer_key("Liberty Media Corp., Series A") == "liberty media"
    assert es.issuer_key("Teva Pharmaceutical Industries Ltd., Sponsored ADR") == (
        "teva pharmaceutical industries"
    )
    assert es.issuer_key("Net increase (decrease)") == "net increase decrease"
    assert es.issuer_key("The Co. Inc") == "thecoinc"
    assert es.DROP == frozenset(
        "class cl series ser special spl adr ads sponsored spon inc incorporated corp corporation "
        "co company ltd limited plc the com new common ord shs nv sa ag".split()
    )


def test_a_document_keeps_holdings_and_drops_the_rest() -> None:
    rows = [
        ("Keep Me Inc.*", 1_000_000, 50_000_000),
        ("Keep Me Inc.", 1_000_000, 50_000_000),
        ("Keep Me Two | ", 2_000_000, 50_000_000),
        ("AB 12", 1_000_000, 50_000_000),
        ("Cash Equivalents", 1_000_000, 50_000_000),
        ("Small Holding", 100_000, 999_999),
        ("Cheap Holding", 3_000_000, 1_400_000),
        ("Dear Holding", 1_000, 5_100_000),
        ("Edge Low", 4_000_000, 2_000_000),
        ("Edge High", 1_000, 5_000_000),
        ("Huge Holding", 1_000_000, 90_000_000),
        ("Just Below", 1_000_000, 89_999_999),
        ("Keep Me Inc", 1_000_000, 60_000_000),
        ("Exact Million", 1_000_000, 1_000_000),
    ]
    text = "\n".join(f"{name} ..... {shares:,}   ${value:,}" for name, shares, value in rows)
    text += "\nZero Shares Inc ..... 0   5,000,000\nTotal Investments .... $ 300,000,000\n"
    text += "Total Investments .... 1,000,000,000\n"
    parsed = es.parse_document("x.txt", text.encode("utf-8"))
    assert parsed.total == 300_000_000
    assert parsed.rows == (
        ("Just Below", 1_000_000, 89_999_999),
        ("Keep Me Inc", 1_000_000, 60_000_000),
        ("Keep Me Inc", 1_000_000, 50_000_000),
        ("Keep Me Two", 2_000_000, 50_000_000),
        ("Edge High", 1_000, 5_000_000),
        ("Edge Low", 4_000_000, 2_000_000),
        ("Exact Million", 1_000_000, 1_000_000),
    )
    with pytest.raises(es.TableError, match="no total investments row"):
        es.parse_document("x.txt", b"Keep Me Inc ..... 1,000,000   50,000,000\n")


def test_a_document_is_read_as_html_by_its_name_and_as_utf8() -> None:
    html_bytes = (
        "<tr><td>Café Holding Inc.</td><td>1,000,000</td><td>50,000,000</td></tr>"
        "<tr><td>Total Investments</td><td>1,000,000,000</td></tr>"
    ).encode()
    parsed = es.parse_document("a.HTM", html_bytes)
    assert parsed.rows == (("Café Holding Inc", 1_000_000, 50_000_000),)
    with pytest.raises(es.TableError, match="no total investments row"):
        es.parse_document("a.txt", html_bytes)
    broken = es.parse_document("a.htm", html_bytes.replace(b"\xc3\xa9", b"\xe9"))
    assert broken.rows == (("Caf� Holding Inc", 1_000_000, 50_000_000),)


BILLION = 1_000_000_000


def _small(rows: list[tuple[str, int, int]], total: int = BILLION) -> dict[int, tuple[str, bytes]]:
    return {2009: ("s.htm", _htm(rows, total))}


def _top(big: int, middle: int = 200_000_000) -> dict[int, tuple[tuple[str, str, int], ...]]:
    return {2009: (("big", "MSFT", big), ("middle", "INTC", middle))}


def _lists(rows: list[tuple[str, int, int]], count: int, **kwargs: Any) -> Any:
    options = {"top10": _top(300_000_000), "rows": {2009: count}, "totals": {2009: BILLION}}
    return es.holdings_lists(_small(rows), **{**options, **kwargs})


def test_the_list_merges_every_share_class_and_compares_with_the_pinned_top() -> None:
    rows = [
        ("Big Corp., Class A", 1_000_000, 200_000_000),
        ("Big Corp., Class B", 1_000_000, 100_000_000),
        ("Middle Inc.", 1_000_000, 200_000_000),
        ("Other Co., Class A", 1_000_000, 100_000_000),
        ("Other Co., Class B", 1_000_000, 49_999_999),
        ("Rest Aaa", 1_000_000, 125_000_000),
        ("Rest Aab", 1_000_000, 125_000_000),
        ("Rest Aac", 1_000_000, 100_000_001),
    ]
    lists, report = _lists(rows, 8)
    assert lists == {2009: ("MSFT", "INTC")}
    assert report == [
        {
            "fiscal_year": 2009,
            "document": "s.htm",
            "rows": 8,
            "total": BILLION,
            "sum_share": 1.0,
            "same_key": [{"key": "big", "rows": 2}, {"key": "other", "rows": 2}],
            "class_rows": 4,
            "top10": ["big", "middle"],
        }
    ]
    # A company outside the pinned list whose classes add up past the second name.
    rows[-1] = ("Other Co., Class C", 1_000_000, 100_000_001)
    with pytest.raises(es.TableError) as caught:
        _lists(rows, 8)
    assert str(caught.value) == "fiscal year 2009: the merged top ten is not the pinned list"
    with pytest.raises(es.TableError, match="the merged top ten"):
        _lists(rows[:7] + [("Rest Aac", 1_000_000, 100_000_001)], 8, top10=_top(200_000_000))


TABLE = [
    ("Big Corp", 1_000_000, 250_000_000),
    ("Middle Inc", 1_000_000, 200_000_000),
    *((f"Rest {_letters(k)}", 1_000_000, 110_000_000 + k) for k in range(5)),
]


@pytest.mark.parametrize(
    ("count", "document_total", "pinned_total", "problem"),
    [
        (6, BILLION, BILLION, "the parsed row count is not the probe's"),
        (7, BILLION, BILLION + 1, "total investments is not the pinned total"),
        (7, 1_500_000_000, 1_500_000_000, "the parsed rows do not add up to the total"),
    ],
)
def test_the_list_refuses_a_count_a_total_or_a_sum(
    count: int, document_total: int, pinned_total: int, problem: str
) -> None:
    options = {"top10": _top(250_000_000), "rows": {2009: count}, "totals": {2009: pinned_total}}
    with pytest.raises(es.TableError) as caught:
        es.holdings_lists(_small(TABLE, document_total), **options)
    assert str(caught.value) == f"fiscal year 2009: {problem}"


def test_the_list_needs_every_year() -> None:
    options = {"rows": {2009: 7}, "totals": {2009: BILLION}}
    lists, _ = es.holdings_lists(_small(TABLE), top10=_top(250_000_000), **options)
    assert lists == {2009: ("MSFT", "INTC")}
    with pytest.raises(es.TableError) as caught:
        es.holdings_lists(_small(TABLE), top10={**_top(250_000_000), 2010: ()}, **options)
    assert str(caught.value) == "fiscal year 2010: no holdings document"
    with pytest.raises(es.TableError) as caught:
        es.holdings_lists(
            {2009: ("s.htm", b"<tr><td>Big Corp</td></tr>")}, top10=_top(1), **options
        )
    assert str(caught.value) == "fiscal year 2009: no total investments row"


@pytest.mark.parametrize(
    ("big", "last", "ok"),
    [
        (250_000_000, 100_000_000, True),
        (250_000_000, 70_000_000, True),
        (250_000_000, 69_999_999, False),
        (280_000_000, 100_000_000, True),
        (280_000_000, 100_000_001, False),
    ],
)
def test_the_sum_share_bounds_are_inclusive(big: int, last: int, ok: bool) -> None:
    rows = [("Big Corp", 1_000_000, big), ("Middle Inc", 1_000_000, 200_000_000)]
    rows += [(f"Rest {_letters(k)}", 1_000_000, 50_000_000) for k in range(9)]
    rows += [("Last Holding", 1_000_000, last)]
    options = {"top10": _top(big), "rows": {2009: 12}, "totals": {2009: BILLION}}
    if ok:
        es.holdings_lists(_small(rows), **options)
    else:
        with pytest.raises(es.TableError, match="do not add up"):
            es.holdings_lists(_small(rows), **options)


def test_the_ignore_row_counts_as_a_row_but_never_enters_the_list() -> None:
    rows = [
        ("Big Corp", 1_000_000, 250_000_000),
        ("Net increase (decrease)", 107_000_000, 250_000_000),
        ("Middle Inc", 1_000_000, 200_000_000),
        ("Tail One", 1_000_000, 150_000_000),
        ("Tail Two", 1_000_000, 150_000_000 - 1),
    ]
    top = _top(250_000_000)
    with pytest.raises(es.TableError, match="the merged top ten"):
        _lists(rows, 5, top10=top)
    near = frozenset({(2009, "net increase decrease", 250_000_001)})
    with pytest.raises(es.TableError, match="the merged top ten"):
        _lists(rows, 5, top10=top, ignore=near)
    ignore = frozenset({(2009, "net increase decrease", 250_000_000)})
    lists, report = _lists(rows, 5, top10=top, ignore=ignore)
    assert lists == {2009: ("MSFT", "INTC")}
    assert (report[0]["rows"], report[0]["sum_share"]) == (5, (BILLION - 1) / BILLION)
    assert report[0]["same_key"] == [] and report[0]["class_rows"] == 0


def test_the_eight_schedules_give_the_pinned_lists() -> None:
    lists, report = es.holdings_lists(DOCUMENTS)
    assert lists == es.LISTS
    assert [r["fiscal_year"] for r in report] == list(range(2003, 2011))
    assert [r["rows"] for r in report] == [103, 102, 102, 101, 101, 102, 101, 102]
    assert [r["document"] for r in report] == list(DOCS.values())
    assert all(r["same_key"] == [] for r in report)
    assert [r["class_rows"] for r in report] == [2, 2, 1, 2, 1, 1, 1, 1]
    assert report[7]["top10"] == [key for key, _, _ in es.TOP10[2010]]
    assert all(0.999 < r["sum_share"] < 1.001 for r in report)


# --- events --------------------------------------------------------------------------------------


def test_results_filings_are_8ks_with_item_2_02_or_12() -> None:
    rows = [
        _filing("MSFT", "2005-01-20 16:05", items="2.02, 9.01"),
        _filing("INTC", "2005-01-18 16:05", items="12"),
        _filing("CSCO", "2005-01-19 16:05", form="8-K/A", items="2.02"),
        _filing("QCOM", "2005-01-19 16:06", items="1.01,2.020"),
        _filing("AMGN", "2005-01-19 16:07", items=""),
        _filing("EBAY", "2005-01-19 16:08", items="7.01, 12 "),
    ]
    found = es.results(_filings(rows))
    assert found["id"].tolist() == ["INTC", "EBAY", "MSFT"]
    assert found.columns.tolist() == ["id", "accession", "accepted"]
    assert found["accepted"].tolist() == [
        pd.Timestamp("2005-01-18 21:05", tz="UTC"),
        pd.Timestamp("2005-01-19 21:08", tz="UTC"),
        pd.Timestamp("2005-01-20 21:05", tz="UTC"),
    ]
    stranger = _filings([{**rows[0], "cik": 1234567}])
    with pytest.raises(es.TableError, match="not one of the configured issuers"):
        es.results(stranger)


def _days(*eastern: str, calendar: pd.DatetimeIndex = CAL) -> list[Any]:
    stamps = pd.Series(
        [pd.Timestamp(t).tz_localize(EASTERN).tz_convert("UTC") for t in eastern],
        index=range(len(eastern)),
    )
    return [
        None if pd.isna(day) else day.date().isoformat() for day in es.event_days(stamps, calendar)
    ]


def test_the_event_day_is_the_first_close_after_the_acceptance() -> None:
    assert _days(
        "2008-01-15 15:59:59",
        "2008-01-15 16:00:00",
        "2008-01-15 08:00:00",
        "2008-01-18 17:00:00",
        "2008-01-19 10:00:00",
        "2003-12-31 16:30:00",
        "2010-12-31 15:00:00",
        "2010-12-31 16:00:01",
    ) == [
        "2008-01-15",
        "2008-01-16",
        "2008-01-15",
        "2008-01-21",
        "2008-01-21",
        "2004-01-02",
        "2010-12-31",
        None,
    ]


def test_the_close_is_16_00_eastern_across_daylight_saving() -> None:
    # 2007-03-11 starts daylight saving: 20:30Z is 15:30 EST on Friday, 16:30 EDT on Monday.
    stamps = pd.Series(pd.to_datetime(["2007-03-09T20:30:00Z", "2007-03-12T20:30:00Z"], utc=True))
    days = es.event_days(stamps, CAL)
    assert [d.date().isoformat() for d in days] == ["2007-03-09", "2007-03-13"]
    closes = es.trading_closes(pd.DatetimeIndex(["2007-03-09", "2007-03-12", "2007-11-05"]))
    assert list(closes) == [
        pd.Timestamp("2007-03-09 21:00", tz="UTC"),
        pd.Timestamp("2007-03-12 20:00", tz="UTC"),
        pd.Timestamp("2007-11-05 21:00", tz="UTC"),
    ]


def test_the_list_year_runs_from_october_to_september() -> None:
    assert [
        es.list_year(pd.Timestamp(d))
        for d in ("2004-01-02", "2004-09-30", "2004-10-01", "2010-12-31")
    ] == [
        2003,
        2003,
        2004,
        2010,
    ]


def _events(rows: list[dict[str, Any]], top: int = 5) -> pd.DataFrame:
    return es.events(es.results(_filings(rows)), CAL, W, es.LISTS, top)


def test_events_take_the_list_of_the_event_day_not_of_the_acceptance() -> None:
    rows = [
        _filing("MSFT", "2003-12-31 16:30"),
        _filing("MSFT", "2003-12-31 15:00"),
        _filing("AMGN", "2004-09-30 15:00"),
        _filing("AMGN", "2004-09-30 16:30"),
        _filing("EBAY", "2004-09-30 15:00"),
        _filing("EBAY", "2004-09-30 16:30"),
        _filing("MSFT", "2010-12-31 15:00"),
        _filing("MSFT", "2010-12-31 16:30"),
        _filing("ORCL", "2010-09-30 16:30"),
        _filing("ORCL", "2010-09-30 15:00"),
    ]
    found = _events(rows)
    assert found.columns.tolist() == [
        "id",
        "rank",
        "list_year",
        "accession",
        "accepted",
        "event_day",
    ]
    assert [
        (r["id"], r["rank"], r["list_year"], r["event_day"].date().isoformat())
        for r in found.to_dict("records")
    ] == [
        ("MSFT", 1, 2003, "2004-01-02"),
        ("AMGN", 5, 2003, "2004-09-30"),
        ("EBAY", 3, 2004, "2004-10-01"),
        ("ORCL", 5, 2010, "2010-10-01"),
        ("MSFT", 4, 2010, "2010-12-31"),
    ]
    assert found["accession"].tolist() == [rows[i]["accessionNumber"] for i in (0, 2, 5, 8, 6)]


def test_foreign_issuers_are_skipped_not_replaced() -> None:
    rows = [_filing(i, "2008-04-15 16:05") for i in ("GILD", "INTC", "AMGN", "ORCL", "EBAY")]
    found = _events(rows, top=10)
    assert sorted(zip(found["id"], found["rank"], strict=True)) == [
        ("EBAY", 9),
        ("GILD", 10),
        ("INTC", 7),
        ("ORCL", 8),
    ]
    assert _events(rows, top=3).empty and _events(rows, top=5).empty


def test_events_are_ordered_by_day_then_acceptance_and_one_day_counts_once() -> None:
    rows = [
        _filing("QCOM", "2008-07-16 16:30"),
        _filing("MSFT", "2008-07-17 16:05"),
        _filing("GOOG", "2008-07-17 16:01"),
        _filing("AAPL", "2008-07-21 16:30"),
        _filing("MSFT", "2008-07-17 17:00", items="12"),
    ]
    found = _events(rows)
    assert found["id"].tolist() == ["QCOM", "GOOG", "MSFT", "MSFT", "AAPL"]
    assert list(es.days_of(found)) == list(
        pd.DatetimeIndex(["2008-07-17", "2008-07-18", "2008-07-22"])
    )


def test_the_two_subsets() -> None:
    rows = [
        _filing("MSFT", "2005-01-20 16:00:00"),
        _filing("MSFT", "2005-02-09 09:29:59"),
        _filing("MSFT", "2005-04-21 09:30:00"),
        _filing("INTC", "2005-01-11 15:59:59"),
        _filing("INTC", "2005-01-12 23:59:59"),
        _filing("INTC", "2005-04-13 00:00:00"),
        _filing("QCOM", "2005-03-31 16:30"),
        _filing("QCOM", "2005-03-31 17:30"),
    ]
    found = _events(rows)
    quarter = es.first_per_quarter(found)
    assert [(r["id"], r["event_day"].date().isoformat()) for r in quarter.to_dict("records")] == [
        ("INTC", "2005-01-11"),
        ("MSFT", "2005-01-21"),
        ("QCOM", "2005-04-01"),
        ("INTC", "2005-04-13"),
        ("MSFT", "2005-04-21"),
    ]
    assert quarter["accession"].tolist()[2] == rows[6]["accessionNumber"]
    after = es.after_close(found)
    assert sorted(after["accession"]) == sorted(
        rows[i]["accessionNumber"] for i in (0, 1, 4, 5, 6, 7)
    )
    assert after.columns.tolist() == found.columns.tolist()


# --- measures ------------------------------------------------------------------------------------


def test_the_bad_days_are_the_lowest_ceil_share_with_ties_to_the_earlier_day() -> None:
    assert [
        es.bad_count(n, s)
        for n, s in ((1763, 0.05), (1760, 0.05), (30, 0.1), (41, 0.05), (40, 0.025), (100, 0.07))
    ] == [
        89,
        88,
        3,
        3,
        1,
        7,
    ]
    days = W[:40]
    returns = pd.Series(np.linspace(0.01, 0.05, 40), index=days)
    returns[days[[30, 5, 17]]] = -0.02
    returns[days[9]] = -0.03
    assert list(es.bad_days(returns, 0.05)) == [days[5], days[9]]
    assert list(es.bad_days(returns, 0.1)) == [days[5], days[9], days[17], days[30]]
    assert list(es.bad_days(returns)) == list(es.bad_days(returns, 0.05))


def test_wilson_is_the_score_interval() -> None:
    z = 1.959963984540054
    for hits, n in ((15, 89), (0, 12), (12, 12), (7, 92)):
        p = hits / n
        center = (p + z * z / (2 * n)) / (1 + z * z / n)
        half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
        assert es.wilson(hits, n) == pytest.approx((center - half, center + half), rel=1e-12)
    assert es.wilson(0, 12)[0] == pytest.approx(0.0, abs=1e-15)


def test_concentration_is_the_hit_share_over_the_event_share() -> None:
    window, bad = W[:100], W[[1, 3, 5, 7, 9]]
    days = W[[1, 3, 50, 60]]
    found = es.concentration(days, window, bad)
    f, share = 4 / 100, 2 / 5
    low, high = es.wilson(2, 5)
    assert found == {
        "events": 4,
        "window": 100,
        "bad": 5,
        "hits": 2,
        "f": f,
        "share": share,
        "c": pytest.approx(share / f),
        "wilson_low": pytest.approx(low / f),
        "wilson_high": pytest.approx(high / f),
    }
    empty = es.concentration(pd.DatetimeIndex([]), window, bad)
    assert (empty["c"], empty["wilson_low"], empty["wilson_high"], empty["hits"]) == (
        None,
        None,
        None,
        0,
    )
    with pytest.raises(es.TableError, match="outside the window"):
        es.concentration(W[[1, 150]], window, bad)


def test_welch_is_the_mean_difference_with_sample_variances() -> None:
    window = W[:12]
    returns = pd.Series(
        [0.01, -0.02, 0.03, 0.0, 0.02, -0.01, 0.015, -0.03, 0.005, 0.01, -0.005, 0.02], index=window
    )
    days, bad = window[[1, 4, 7]], window[[1, 7]]
    e, o = returns[days], returns.drop(days)
    diff = e.mean() - o.mean()
    se = math.sqrt(e.var(ddof=1) / 3 + o.var(ddof=1) / 9)
    found = es.welch(returns, days, bad)
    assert found == {
        "n_e": 3,
        "n_o": 9,
        "mean_e": pytest.approx(e.mean()),
        "mean_o": pytest.approx(o.mean()),
        "median_e": pytest.approx(0.02 if False else e.median()),
        "median_o": pytest.approx(o.median()),
        "diff": pytest.approx(diff),
        "se": pytest.approx(se),
        "low": pytest.approx(diff - 1.96 * se),
        "high": pytest.approx(diff + 1.96 * se),
        "point_le": True,
        "log_sum_e": pytest.approx(float(np.log1p(e).sum())),
        "mean_e_good": pytest.approx(0.02),
        "mean_o_good": pytest.approx(o.mean()),
    }
    assert "mean_e_good" not in es.welch(returns, days)
    even = pd.Series([0.01, -0.01, 0.01, -0.01, 0.0, 0.0], index=window[:6])
    assert es.welch(even, window[[0, 1]])["point_le"] is True
    assert es.welch(returns, window[[1]]) is None
    assert es.welch(returns, window[1:]) is None
    assert es.welch(returns, window[2:]) is not None


def test_the_shuffle_draw_is_pinned() -> None:
    window = W[:200]
    pools = es.quarters(window)
    assert sorted(pools) == [(2004, 1), (2004, 2), (2004, 3), (2004, 4)]
    assert pools[(2004, 1)][0] == pd.Timestamp("2004-01-02") and len(pools[(2004, 1)]) == 64
    days = pd.DatetimeIndex([window[150], window[3], window[64], window[65], window[66]])
    got = es.shuffle_draw(days, pools, np.random.default_rng(20261030))
    rng = np.random.default_rng(20261030)
    picks = []
    for day in sorted(days):
        pool = [d for d in window if (d.year, d.quarter) == (day.year, day.quarter)]
        picks.append(pool[int(rng.integers(0, len(pool)))])
    assert list(got) == sorted(set(picks))
    assert [d.date().isoformat() for d in got] == SHUFFLE_PINNED


# The first draw of default_rng(20261030) over those five days, sorted by day before drawing.
SHUFFLE_PINNED = ["2004-03-10", "2004-04-06", "2004-05-13", "2004-05-26", "2004-07-20"]


def test_the_shuffle_p_counts_draws_at_or_above_the_actual_c() -> None:
    window = W[:250]
    returns = _returns(3, calendar=window)
    bad = es.bad_days(returns)
    days = pd.DatetimeIndex(sorted({bad[0], *window[[10, 70, 130, 190, 240]].difference(bad)}))
    found = es.shuffle(days, window, bad, draws=60, seed=11)
    actual = es.concentration(days, window, bad)["c"]
    rng, pools = np.random.default_rng(11), es.quarters(window)
    values = np.array(
        [es.concentration(es.shuffle_draw(days, pools, rng), window, bad)["c"] for _ in range(60)]
    )
    ge, lt, eq = (
        int((values >= actual).sum()),
        int((values < actual).sum()),
        int((values == actual).sum()),
    )
    assert found == {
        "draws": 60,
        "seed": 11,
        "actual": actual,
        "ge": ge,
        "p": (1 + ge) / 61,
        "mid_rank": (lt + 0.5 * eq) / 60,
        "q05": pytest.approx(np.quantile(values, 0.05)),
        "q50": pytest.approx(np.quantile(values, 0.5)),
        "q95": pytest.approx(np.quantile(values, 0.95)),
        "passes": (1 + ge) / 61 <= 0.05,
    }
    assert 0 < eq and ge < 60
    default = es.shuffle(days, window, bad, draws=20)
    assert default["seed"] == 20261030


def test_shifts_move_along_w_and_drop_what_leaves_it() -> None:
    window = W[:30]
    days = window[[0, 3, 27, 29]]
    assert list(es.shifted(days, window, -1)) == list(window[[2, 26, 28]])
    assert list(es.shifted(days, window, 5)) == list(window[[5, 8]])
    rows = es.shift_rows(days, window, window[[2, 8, 26]])
    assert [(r["shift"], r["events"], r["hits"]) for r in rows] == [
        (-5, 2, 0),
        (-1, 3, 2),
        (1, 3, 0),
        (5, 2, 1),
    ]
    assert (
        rows[1]["c"]
        == es.concentration(es.shifted(days, window, -1), window, window[[2, 8, 26]])["c"]
    )


def test_each_year_has_its_own_event_share() -> None:
    returns = _returns(5, calendar=W)
    bad = es.bad_days(returns)
    days = pd.DatetimeIndex(sorted(set(bad[::3]) | set(W[::40])))
    rows = es.year_rows(days, W, bad)
    assert [r["year"] for r in rows] == list(range(2004, 2011))
    for row in rows:
        w, e, b = (x[x.year == row["year"]] for x in (W, days, bad))
        assert (row["window"], row["events"], row["bad"]) == (len(w), len(e), len(b))
        assert row["hits"] == len(e.intersection(b))
        expected = None if not len(b) or not len(e) else (row["hits"] / len(b)) / (len(e) / len(w))
        assert row["c"] == (None if expected is None else pytest.approx(expected))
    empty = es.year_rows(W[W.year == 2004][:3], W, bad[bad.year != 2004])
    assert empty[0]["c"] is None and empty[1]["c"] is None


def test_stock_returns_divide_by_the_previous_qqq_trading_day() -> None:
    rows = [
        _filing("MSFT", "2004-01-02 08:00"),
        _filing("GOOG", "2008-07-17 16:05"),
        _filing("EBAY", "2006-01-18 16:05"),
        _filing("ORCL", "2010-12-31 09:00"),
    ]
    found = _events(rows, top=10)
    prices = _prices(missing=("2006-01-19",))
    stock = es.stock_returns(found, prices, CAL)
    table = prices.set_index(["ticker", "date"])["adj_close"]

    def change(ticker: str, day: str, before: str) -> float:
        return table[(ticker, pd.Timestamp(day))] / table[(ticker, pd.Timestamp(before))] - 1

    by_id = dict(zip(found["id"], stock, strict=True))
    assert by_id["MSFT"] == pytest.approx(change("MSFT", "2004-01-02", "2003-12-31"))
    assert by_id["GOOG"] == pytest.approx(change("GOOG", "2008-07-18", "2008-07-17"))
    assert math.isnan(by_id["EBAY"])
    assert by_id["ORCL"] == pytest.approx(change("ORCL", "2010-12-31", "2010-12-30"))
    early = pd.DataFrame(
        {
            "id": ["MSFT", "MSFT"],
            "rank": [1, 1],
            "list_year": [2003, 2003],
            "accession": ["a", "b"],
            "accepted": pd.to_datetime(["2003-09-01 14:00", "2003-09-02 14:00"], utc=True),
            "event_day": [CAL[0], CAL[1]],
        }
    )
    first = es.stock_returns(early, prices, CAL)
    assert math.isnan(first.iloc[0])
    assert first.iloc[1] == pytest.approx(change("MSFT", str(CAL[1].date()), str(CAL[0].date())))
    dell = es.stock_returns(_events([_filing("DELL", "2004-02-12 16:05")], top=10), prices, CAL)
    assert dell.isna().all() and len(dell) == 1


def test_issuer_rows_count_months_in_the_top_five() -> None:
    found = BUILT.sets["top5"]
    bad = es.bad_days(BUILT.returns)
    rows = es.issuer_rows(found, BUILT.stock, bad, es.LISTS)
    assert [(r["id"], r["months"]) for r in rows] == [
        ("MSFT", 84),
        ("INTC", 33),
        ("CSCO", 69),
        ("QCOM", 84),
        ("AMGN", 21),
        ("EBAY", 12),
        ("AAPL", 63),
        ("GOOG", 51),
        ("ORCL", 3),
    ]
    for row in rows:
        mine = found[found["id"] == row["id"]]
        values = BUILT.stock[mine.index].dropna()
        assert row["events"] == len(mine) and row["returns"] == len(values)
        assert row["missing"] == len(mine) - len(values)
        assert row["bad_events"] == int(mine["event_day"].isin(bad).sum())
        assert row["median"] == (pytest.approx(values.median()) if len(values) else None)
        assert row["crashes"] == int((values <= -0.08).sum())


def test_crash_days_need_both_the_stock_and_qqq_and_count_a_day_once() -> None:
    rows = [
        _filing("MSFT", "2008-01-24 16:05"),
        _filing("GOOG", "2008-04-17 16:05"),
        _filing("QCOM", "2008-04-23 16:05"),
        _filing("CSCO", "2008-05-13 16:05"),
        _filing("AAPL", "2008-05-13 16:30"),
    ]
    found = _events(rows)
    stock = pd.Series([-0.08, -0.20, -0.079, -0.15, -0.10], index=found.index)
    returns = pd.Series(0.001, index=W)
    returns[pd.Timestamp("2008-01-25")] = -0.01
    returns[pd.Timestamp("2008-04-18")] = -0.0099
    returns[pd.Timestamp("2008-04-24")] = -0.03
    returns[pd.Timestamp("2008-05-14")] = -0.05
    assert list(es.crash_days(found, stock, returns)) == list(
        pd.DatetimeIndex(["2008-01-25", "2008-05-14"])
    )


def test_the_follow_up_compounds_from_the_next_day_and_drops_late_ends() -> None:
    returns = pd.Series(0.01, index=W)
    last = len(W) - 1
    days = W[[0, last - 5, last - 4]]
    assert es.follow(returns, days, 5) == pytest.approx([1.01**5 - 1, 1.01**5 - 1])
    assert es.follow(returns, days, 63) == pytest.approx([1.01**63 - 1])
    returns[W[1]] = -0.5
    assert es.follow(returns, W[[0]], 2) == pytest.approx([0.5 * 1.01 - 1])
    assert es.stats([]) == {
        "n": 0,
        "mean": None,
        "median": None,
        "min": None,
        "q25": None,
        "q75": None,
        "max": None,
    }
    assert es.stats([4.0, 1.0, 2.0, 3.0]) == {
        "n": 4,
        "mean": 2.5,
        "median": 2.5,
        "min": 1.0,
        "q25": 1.75,
        "q75": 3.25,
        "max": 4.0,
    }


def test_the_crash_table_prints_counts_only_under_five_and_cuts_both_groups() -> None:
    issuers = ("MSFT", "INTC", "CSCO", "QCOM", "AMGN")
    rows = [_filing(issuer, f"2004-0{k + 2}-1{k} 16:05") for k, issuer in enumerate(issuers)]
    rows.append(_filing("AAPL", "2010-12-01 16:05"))
    found = _events(rows)
    stock = pd.Series(-0.1, index=found.index)
    returns = pd.Series(0.001, index=W)
    crash_days = es.days_of(found)
    returns[crash_days] = -0.02
    others = W[[300, 600, len(W) - 3]]
    returns[others] = -0.015
    table = es.crash_table(found, stock, returns, crash_days)
    assert table["events"] == 6 and table["control_days"] == 3
    assert table["days"] == [d.date().isoformat() for d in crash_days]
    by_horizon = {r["horizon"]: r for r in table["horizons"]}
    assert by_horizon[5]["crash"]["n"] == 6 and by_horizon[63]["crash"]["n"] == 5
    assert by_horizon[5]["control"]["n"] == 2 and by_horizon[63]["control"]["n"] == 2
    assert by_horizon[5]["crash"] == es.stats(es.follow(returns, crash_days, 5))
    short = es.crash_table(found.iloc[:4], stock.iloc[:4], returns, crash_days)
    assert [r["crash"] for r in short["horizons"]] == [{"n": 4}, {"n": 4}, {"n": 4}]
    assert short["horizons"][0]["control"]["n"] == 2
    five = es.crash_table(found.iloc[:5], stock.iloc[:5], returns, crash_days)
    assert five["events"] == 5 and five["horizons"][0]["crash"]["n"] == 5
    assert five["horizons"][0]["crash"]["mean"] == pytest.approx(1.001**5 - 1)


# --- the reading ---------------------------------------------------------------------------------


def _main(c: float = 2.0, events: int = 120, bad: int = 89, window: int = 1762) -> dict[str, Any]:
    return {"events": events, "bad": bad, "window": window, "f": events / window, "c": c}


@pytest.mark.parametrize(
    ("main", "p", "low", "verdict", "failed"),
    [
        (_main(), 0.05, 0.0, "holds", []),
        (_main(c=1.9999999999), 0.01, -0.01, "not_holds", ["line1"]),
        (_main(), 0.0500001, -0.01, "not_holds", ["line2"]),
        (_main(), 0.01, 1e-12, "not_holds", ["line3"]),
        (_main(c=1.5), 0.3, 0.002, "not_holds", ["line1", "line2", "line3"]),
        (_main(events=79), 0.01, -0.01, "too_few", []),
        (_main(events=80, window=1780), 0.01, -0.01, "holds", []),
        (_main(events=80, window=1781), 0.01, -0.01, "too_few", []),
    ],
)
def test_the_verdict(
    main: dict[str, Any], p: float, low: float, verdict: str, failed: list[str]
) -> None:
    found = {"diff": 0.0001, "low": low, "high": 0.004}
    decision = es.judge(main, {"p": p}, found)
    assert (decision["verdict"], decision["failed"]) == (verdict, failed)
    if verdict == "too_few":
        assert decision == {
            "verdict": "too_few",
            "lines": None,
            "failed": [],
            "sentence": es.TOO_FEW_SENTENCE,
        }
        assert es.judge(main, None, None) == decision
    else:
        assert decision["lines"] == {
            name: name not in failed for name in ("line1", "line2", "line3")
        }


def test_the_sentences_name_which_line_failed() -> None:
    found = {"diff": 0.00012345, "low": -0.0031, "high": 0.00335}
    spread = es.judge(_main(c=1.234567), {"p": 0.04321}, found)
    assert spread["sentence"] == (
        "财报冲击：慢规则覆盖不到，按日程躲不集中"
        "（C 1.23，p 0.043，差 +0.012%，区间 -0.310% 到 +0.335%）；"
        "2004–2010 只看得出约 2 倍以上的集中，2 倍时约三成判得出（合成模拟，推断，上限约五成）"
    )
    premium = es.judge(_main(c=2.5), {"p": 0.001}, {**found, "low": 0.0002})
    assert premium["sentence"].startswith(
        "财报冲击：慢规则覆盖不到，按日程躲要付财报溢价"
        "（C 2.50，p 0.001，差 +0.012%，区间 +0.020% 到"
    )
    both = es.judge(_main(c=2.5), {"p": 0.2}, {**found, "low": 0.0002})
    assert "按日程躲不集中（" in both["sentence"]
    holds = es.judge(_main(c=2.5), {"p": 0.001}, found)
    assert holds["sentence"] == es.HOLDS_SENTENCE


def test_the_shift_reading_is_any_shifted_c_at_or_above_c_minus_0_3() -> None:
    rows = [{"c": None}, {"c": 1.0}, {"c": 1.69}]
    assert es.shift_note(2.0, rows) is False
    assert es.shift_note(2.0, [*rows, {"c": 1.71}]) is True
    assert es.shift_note(2.0, [{"c": 2.0 - 0.3}]) is True
    assert es.shift_note(1.0, [{"c": 3.0}]) is True


def test_the_sensitivity_rows() -> None:
    bad = es.bad_days(BUILT.returns)
    found = es.sensitivity(BUILT, bad)
    assert list(found) == [
        "top3",
        "top10",
        "first_per_quarter",
        "after_close",
        "bad_0.025",
        "bad_0.1",
    ]
    for name in ("top3", "top10", "first_per_quarter", "after_close"):
        days = es.days_of(BUILT.sets[name])
        welch = es.welch(BUILT.returns, days)
        assert found[name] == {
            **es.concentration(days, BUILT.window, bad),
            "welch_low": None if welch is None else welch["low"],
        }
    for share in (0.025, 0.1):
        assert found[f"bad_{share}"] == es.concentration(
            TOP5_DAYS, BUILT.window, es.bad_days(BUILT.returns, share)
        )


CHECK_KEYS = {
    "run_id",
    "owner_decision",
    "earnings_snapshot_id",
    "qqq_snapshot_id",
    "qqq_column",
    "sources",
    "window",
    "bad_days",
    "filings",
    "sets",
    "expected_hits",
    "too_few",
    "per_list",
    "hours",
    "evening_share",
    "missing_prices",
    "holdings",
}
CHECK_FORBIDDEN = (
    "bad_days",
    "concentration",
    "welch",
    "shuffle",
    "shift_rows",
    "year_rows",
    "crash_table",
    "sensitivity",
)


def test_check_counts_and_never_reads_a_return_or_a_price_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in CHECK_FORBIDDEN:
        monkeypatch.setattr(es, name, _forbidden)
    summary = es.check_summary(BUILT)
    other = _inputs(returns=_returns(99), snapshot=_snapshot(prices=_prices(scale=3.0)))
    assert es.check_summary(es.build(other)) == summary
    assert set(summary) == CHECK_KEYS
    numbers = json.dumps(summary)
    for key in ('"c"', '"hits"', '"wilson', '"mean', '"median', '"p"'):
        assert key not in numbers


def test_check_counts_on_the_synthetic_season() -> None:
    summary = es.check_summary(BUILT)
    top5 = BUILT.sets["top5"]
    assert summary["window"] == {"first": "2004-01-02", "last": "2010-12-31", "days": len(W)}
    assert summary["bad_days"] == math.ceil(0.05 * len(W) - 1e-9)
    assert summary["filings"] == {"rows": len(_season()), "results": len(_season()) - 2}
    for name in es.SETS:
        days = BUILT.sets[name]["event_day"]
        assert summary["sets"][name] == {
            "rows": len(days),
            "days": days.nunique(),
            "repeated_days": int((days.value_counts() > 1).sum()),
        }
    assert summary["sets"]["top5"]["rows"] == 140
    assert summary["expected_hits"] == pytest.approx(summary["bad_days"] * days_share(top5))
    assert summary["too_few"] is False
    hours = pd.DatetimeIndex(top5["accepted"]).tz_convert(EASTERN).hour
    assert summary["hours"] == {f"{h:02d}": int((hours == h).sum()) for h in range(24)}
    assert summary["evening_share"] == pytest.approx(float((hours >= 16).mean()))
    assert summary["missing_prices"] == int(BUILT.stock.isna().sum())
    per_list = {(r["list_year"], r["id"]): r for r in summary["per_list"]}
    assert len(per_list) == 75 and ("RIMM" not in {i for _, i in per_list})
    assert per_list[(2003, "DELL")] == {"list_year": 2003, "rank": 6, "id": "DELL", "events": 1}
    assert per_list[(2003, "MSFT")]["events"] == 3
    assert per_list[(2010, "ORCL")]["events"] == 1
    # GILD's filing of 2008-04-17 falls in the list of 2007-09-30 (rank 10), not of 2008.
    assert per_list[(2007, "GILD")]["events"] == 1
    assert per_list[(2008, "GILD")]["events"] == 0
    assert summary["holdings"] == es.holdings_lists(DOCUMENTS)[1]
    assert summary["sources"] == SOURCES


def days_share(found: pd.DataFrame) -> float:
    return found["event_day"].nunique() / len(W)


def test_too_few_counts_the_days_and_the_expected_hits() -> None:
    few = _season()[:60]
    built = es.build(_inputs(snapshot=_snapshot(filings=few)))
    summary = es.check_summary(built)
    assert summary["sets"]["top5"]["days"] < 80 and summary["too_few"] is True
    # 80 filings on 79 days in an 81-day window: |B| x f = 5 x 79 / 81 is over 4, the days are not.
    rows = [_filing("MSFT", f"{day.date()} 10:00") for day in W[:79]]
    rows.append(_filing("INTC", f"{W[0].date()} 11:00"))
    short = _returns()[lambda r: r.index < W[81]]
    summary = es.check_summary(es.build(_inputs(returns=short, snapshot=_snapshot(filings=rows))))
    assert summary["sets"]["top5"] == {"rows": 80, "days": 79, "repeated_days": 1}
    assert summary["expected_hits"] > 4 and summary["too_few"] is True


def test_a_day_with_three_events_is_one_repeated_day() -> None:
    rows = [
        _filing("MSFT", "2008-01-24 16:05"),
        _filing("AAPL", "2008-01-24 16:30"),
        _filing("GOOG", "2008-01-24 17:00"),
        _filing("QCOM", "2008-04-23 16:05"),
    ]
    summary = es.check_summary(es.build(_inputs(snapshot=_snapshot(filings=rows))))
    assert summary["sets"]["top5"] == {"rows": 4, "days": 2, "repeated_days": 1}


def test_the_read_has_every_part_and_dumps_as_json(results: dict[str, Any]) -> None:
    result, rows = results["base"]
    assert set(result) == {
        "run_id",
        "owner_decision",
        "earnings_snapshot_id",
        "qqq_snapshot_id",
        "check",
        "main",
        "shuffle",
        "welch",
        "shifts",
        "years",
        "issuers",
        "crashes",
        "sensitivity",
        "decision",
    }
    assert result["check"] == es.check_summary(BUILT)
    bad = es.bad_days(BUILT.returns)
    assert result["main"] == es.concentration(TOP5_DAYS, W, bad)
    assert result["welch"] == es.welch(BUILT.returns, TOP5_DAYS, bad)
    assert result["shuffle"] == es.shuffle(TOP5_DAYS, W, bad, draws=1000, seed=20261030)
    assert result["shifts"] == es.shift_rows(TOP5_DAYS, W, bad)
    assert result["years"] == es.year_rows(TOP5_DAYS, W, bad)
    assert result["issuers"] == es.issuer_rows(BUILT.sets["top5"], BUILT.stock, bad, es.LISTS)
    assert result["crashes"] == es.crash_table(
        BUILT.sets["top5"], BUILT.stock, BUILT.returns, TOP5_DAYS
    )
    assert result["sensitivity"] == es.sensitivity(BUILT, bad)
    decision = result["decision"]
    assert {k: v for k, v in decision.items() if k != "shift_note"} == es.judge(
        result["main"], result["shuffle"], result["welch"]
    )
    assert decision["shift_note"] == es.shift_note(result["main"]["c"], result["shifts"])
    json.dumps(result, allow_nan=False)
    assert rows[0] == {
        "id": BUILT.sets["top5"]["id"][0],
        "rank": int(BUILT.sets["top5"]["rank"][0]),
        "list_year": 2003,
        "accession": BUILT.sets["top5"]["accession"][0],
        "accepted_utc": "2004-01-12T21:05:00Z",
        "accepted_eastern": "2004-01-12T16:05:00-05:00",
        "event_day": "2004-01-13",
    }
    assert len(rows) == 140 and all(set(r) == set(rows[0]) for r in rows)


def test_an_event_on_every_bad_day_holds(results: dict[str, Any]) -> None:
    result, _ = results["holds"]
    assert result["decision"]["verdict"] == "holds"
    assert (
        result["main"]["c"] >= 2 and result["shuffle"]["p"] <= 0.05 and result["welch"]["low"] <= 0
    )
    assert result["shuffle"]["p"] == 1 / 1001


def test_too_few_stops_after_the_main_count() -> None:
    result, rows = es.read(_inputs(snapshot=_snapshot(filings=_season()[:60])))
    assert result["decision"] == {**es.judge(result["main"], None, None), "shift_note": None}
    for key in ("shuffle", "welch", "shifts", "years", "issuers", "crashes", "sensitivity"):
        assert result[key] is None
    assert len(rows) == result["check"]["sets"]["top5"]["rows"]


def test_data_after_2010_never_matter(
    monkeypatch: pytest.MonkeyPatch, results: dict[str, Any]
) -> None:
    later = pd.bdate_range("2011-01-03", "2011-06-30")
    daily = pd.DataFrame({es.QQQ_COLUMN: pd.concat([_returns(), pd.Series(np.nan, index=later)])})
    daily.iloc[-3, 0] = np.inf
    daily.iloc[-2, 0] = -0.9
    assert es.qqq_series(daily).equals(_returns())
    late_prices = pd.concat(
        [_prices(), pd.DataFrame({"ticker": "MSFT", "date": later, "adj_close": -1.0})],
        ignore_index=True,
    )
    late_filings = _season() + [
        _filing("MSFT", "2011-01-20 16:05"),
        _filing("AAPL", "2010-12-31 16:00"),
    ]
    inputs = _inputs(
        returns=es.qqq_series(daily), snapshot=_snapshot(filings=late_filings, prices=late_prices)
    )
    result, rows = es.read(inputs)
    base, base_rows = results["base"]
    assert rows == base_rows
    assert {k: v for k, v in result.items() if k != "check"} == {
        k: v for k, v in base.items() if k != "check"
    }
    assert result["check"]["filings"] == {
        "rows": len(late_filings),
        "results": len(late_filings) - 2,
    }


def test_the_qqq_series_is_cut_then_checked() -> None:
    frame = pd.DataFrame(
        {es.QQQ_COLUMN: [0.01, np.nan, 0.02]},
        index=pd.to_datetime(["2010-12-30", "2010-12-31", "2011-01-03"]),
    )
    assert es.qqq_series(frame).to_dict() == {pd.Timestamp("2010-12-30"): 0.01}
    with pytest.raises(es.TableError, match="has no etf_reference_return column"):
        es.qqq_series(frame.rename(columns={es.QQQ_COLUMN: "qqq_proxy_return"}))
    twice = pd.DataFrame(
        {es.QQQ_COLUMN: [0.01, 0.02]}, index=pd.to_datetime(["2010-12-30", "2010-12-30"])
    )
    with pytest.raises(es.TableError, match="unique and increasing"):
        es.qqq_series(twice)
    bad = pd.DataFrame(
        {es.QQQ_COLUMN: [0.01, np.inf]}, index=pd.to_datetime(["2010-12-29", "2010-12-30"])
    )
    with pytest.raises(es.TableError, match="not finite"):
        es.qqq_series(bad)


# --- the report and the files --------------------------------------------------------------------


def test_report_sentences(results: dict[str, Any]) -> None:
    holds = es.render({**results["holds"][0], "provenance": {"commit": "abc123", "branch": "main"}})
    assert "## 判定：holds" in holds and es.HOLDS_SENTENCE + "。" in holds
    assert es.HOLDS_NULL in holds and "按日程降仓" in holds and "abc123" in holds
    for text in (
        E1,
        es.QQQ_SNAPSHOT_ID,
        es.OWNER_DECISION,
        "第 6 期章程第 24 节",
        "不进账本",
        *es.LIMITS,
    ):
        assert text in holds, text
    base = results["base"][0]
    text = es.render(base)
    if base["decision"]["verdict"] == "not_holds":
        assert base["decision"]["sentence"] + "。" in text and es.NOT_AGAIN in text
        for name in base["decision"]["failed"]:
            assert es.LINE_NAMES[name] in text
    assert ("多半是财报季，不只是那一天" in text) == bool(base["decision"]["shift_note"])
    not_holds = {
        **base,
        "decision": {**es.judge(_main(c=1.2), {"p": 0.4}, base["welch"]), "shift_note": True},
    }
    text = es.render(not_holds)
    assert "## 判定：not_holds" in text and "线 1：C ≥ 2；线 2：季内打乱 p ≤ 0.05" in text
    assert "不再用 2004–2010 问" in text and "多半是财报季，不只是那一天" in text
    assert "约三成" in text and "上限约五成" in text
    few, _ = es.read(_inputs(snapshot=_snapshot(filings=_season()[:60])))
    text = es.render(few)
    assert "## 判定：too_few" in text and es.TOO_FEW_SENTENCE in text and "## 每年" not in text


def test_files(tmp_path: Path, results: dict[str, Any]) -> None:
    result, rows = results["base"]
    es.write_check(result["check"], tmp_path / "c")
    assert (tmp_path / "c" / es.CHECK).read_text(encoding="utf-8") == (
        json.dumps(result["check"], ensure_ascii=False, indent=1, sort_keys=True) + "\n"
    )
    es.write_result(result, rows, tmp_path / "r")
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [es.EVENTS, es.SUMMARY, es.REPORT]
    )
    lines = (tmp_path / "r" / es.EVENTS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == rows
    assert json.loads((tmp_path / "r" / es.SUMMARY).read_text(encoding="utf-8")) == result
    assert (tmp_path / "r" / es.REPORT).read_text(encoding="utf-8") == es.render(result)


def test_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, results: dict[str, Any]) -> None:
    monkeypatch.setattr(es, "load_inputs", lambda: BASE)
    monkeypatch.setattr(es, "git_provenance", lambda root: {"commit": "abc123", "branch": "main"})
    runner = CliRunner()
    with pytest.MonkeyPatch.context() as patch:
        for name in CHECK_FORBIDDEN:
            patch.setattr(es, name, _forbidden)
        out = runner.invoke(es.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    check = results["base"][0]["check"]
    assert (
        f"earnings_shock_table: W {check['window']['days']} 天；前 5 的事件 "
        f"{check['sets']['top5']['rows']} 个、{check['sets']['top5']['days']} 天；名单 8 年都对上"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [es.CHECK]
    out = runner.invoke(es.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    result = results["base"][0]
    assert f"earnings_shock_table: 判定 {result['decision']['verdict']}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [es.CHECK, es.EVENTS, es.SUMMARY, es.REPORT]
    )
    written = json.loads((tmp_path / "r" / es.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**result, "provenance": {"commit": "abc123", "branch": "main"}}
    assert (tmp_path / "r" / es.CHECK).read_bytes() == (tmp_path / "c" / es.CHECK).read_bytes()


def test_commands_print_only_a_summary_when_they_fail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def broken() -> es.Inputs:
        raise ValueError("invalid literal for int() with base 10: '4321.8765'")

    monkeypatch.setattr(es, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(es.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "earnings_shock_table: 数据载入失败：ValueError" in out.output
        assert "4321.8765" not in out.output and not (tmp_path / command).exists()

    def table_error() -> es.Inputs:
        raise es.TableError(f"earnings snapshot {E1}: unchecked\nsecond line")

    monkeypatch.setattr(es, "load_inputs", table_error)
    out = CliRunner().invoke(es.app, ["check", "--out", str(tmp_path / "t")])
    assert out.exit_code == 1 and f"数据载入失败：earnings snapshot {E1}: unchecked" in out.output
    monkeypatch.setattr(es, "load_inputs", lambda: BASE)
    monkeypatch.setattr(es, "results", lambda filings: float("4321.87x"))
    for command in ("check", "run"):
        out = CliRunner().invoke(es.app, [command, "--out", str(tmp_path / f"b{command}")])
        assert out.exit_code == 1
        assert "earnings_shock_table: 建数据失败：ValueError" in out.output
        assert "4321.87x" not in out.output and not (tmp_path / f"b{command}").exists()
    monkeypatch.setattr(
        es,
        "results",
        lambda filings: (_ for _ in ()).throw(
            es.TableError("a filing's CIK is not one of the configured issuers")
        ),
    )
    out = CliRunner().invoke(es.app, ["run", "--out", str(tmp_path / "x")])
    assert "建数据失败：a filing's CIK is not one of the configured issuers" in out.output


def test_load_inputs_reads_the_two_pinned_snapshots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asked: list[tuple[str, Any]] = []
    snapshot = _snapshot()

    def load_snapshot(root: Path, snapshot_id: str) -> SimpleNamespace:
        asked.append(("ern", (root, snapshot_id)))
        return snapshot

    def resolve(root: Path, snapshot_id: str | None = None) -> Path:
        asked.append(("dir", (root, snapshot_id)))
        return tmp_path / str(snapshot_id)

    later = pd.bdate_range("2011-01-03", "2011-01-31")
    daily = pd.DataFrame(
        {es.QQQ_COLUMN: pd.concat([_returns(), pd.Series(0.5, index=later)]), "vix": 20.0}
    )

    def load_daily(directory: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        asked.append(("daily", directory))
        return daily, {"status": "WARN", "series_csv_sha256": "f" * 64, "snapshot_id": "ignored"}

    monkeypatch.setattr(ee, "load_snapshot", load_snapshot)
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", resolve)
    monkeypatch.setattr(qlh, "load_daily", load_daily)
    inputs = es.load_inputs(tmp_path / "root", tmp_path / "qlh")
    assert asked == [
        ("ern", (tmp_path / "root", E1)),
        ("dir", (tmp_path / "qlh", "qlh-20261013-0b0df53cb1a2")),
        ("daily", tmp_path / "qlh-20261013-0b0df53cb1a2"),
    ]
    assert inputs.snapshot is snapshot and inputs.qqq.equals(_returns())
    assert inputs.sources == {
        "earnings": {
            "snapshot_id": E1,
            "filings": len(snapshot.filings),
            "prices": len(snapshot.prices),
            "documents": 8,
        },
        "qqq": {
            "snapshot_id": "qlh-20261013-0b0df53cb1a2",
            "check_status": "WARN",
            "series_csv_sha256": "f" * 64,
            "first": "2003-09-01",
            "last": "2010-12-31",
            "days": len(CAL),
        },
    }


def test_load_inputs_stops_on_a_wrong_config_or_a_refused_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    spec = ee.load_spec()
    monkeypatch.setattr(ee, "load_spec", lambda *a, **k: spec.model_copy(update={"foreign": ()}))
    with pytest.raises(es.TableError, match="foreign issuers"):
        es.load_inputs(tmp_path, tmp_path)
    monkeypatch.setattr(ee, "load_spec", lambda *a, **k: spec)

    def refused(root: Path, snapshot_id: str) -> None:
        raise ValueError("earnings snapshot is unchecked or failed\nmore")

    monkeypatch.setattr(ee, "load_snapshot", refused)
    with pytest.raises(es.TableError) as caught:
        es.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == f"earnings snapshot {E1}: earnings snapshot is unchecked or failed"
    monkeypatch.setattr(ee, "load_snapshot", lambda root, snapshot_id: _snapshot())

    def missing(root: Path, snapshot_id: str | None = None) -> Path:
        raise FileNotFoundError("snapshot qlh-20261013-0b0df53cb1a2 has no manifest")

    monkeypatch.setattr(qlh, "resolve_snapshot_dir", missing)
    with pytest.raises(es.TableError) as caught:
        es.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == (
        "QQQ snapshot qlh-20261013-0b0df53cb1a2: snapshot qlh-20261013-0b0df53cb1a2 has no manifest"
    )
