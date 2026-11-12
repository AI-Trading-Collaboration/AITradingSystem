"""Acceptance tests for the earnings-shock table read once on QQQ after 2010 (TRADING-2565-130).

Written before the module (the loop thread's tests, pi's module), from requirement v1.1 in the
project files (`knowledge/claude/research-review/`
`earnings-shock-after-2010-requirements-2026-11-10.md`, sections 2.1, 2.3, 2.5, 3, 4, 6 and 7),
which keeps every rule of TRADING-2565-99 and changes only the window, the lists, the parse of
three layouts, the `cost` word and two print-only parts. Every input is synthetic: no snapshot, no
QQQ return and no price is read.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_data import earnings_events as ee
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import earnings_shock_after_2010 as ea
from ai_trading_system.research_probes import earnings_shock_table as es
from ai_trading_system.research_probes import structure_shift_periods as ssp

# E1: the earnings snapshot the research session fetched on the PC after TRADING-2565-129.
E1 = "ern-97c76c70e3a0"
OD = "owner_decision:TRADING-2565:2026-11-10:earnings_shock_after_2010"
PROBES = PROJECT_ROOT / "src" / "ai_trading_system" / "research_probes"
MODULE = PROBES / "earnings_shock_after_2010.py"
# Imported, never changed: their bytes on main c38afdc8.
IMPORTED = {
    "earnings_shock_table.py": "22d12cb4a98304876c78b11831ba1f647f3af35a4a4b67fddb334323351281ed",
    "structure_shift_periods.py": (
        "7153f30da86fdd531e05c1e339c399fc41d52321594c19a469af93bdf062f0b8"
    ),
}
SPEC = ee.load_spec(PROJECT_ROOT / "config" / "research" / "research_earnings_after_2010.yaml")
EASTERN = "America/New_York"
FIRST, LAST = pd.Timestamp("2011-01-03"), pd.Timestamp("2026-10-12")
ROW_BY_ROW = {"0001193125-16-806695", "0001193125-18-001208", "0001193125-18-355790"}


# --- the synthetic QQQ calendar: weekdays from 2010-09-01, with 149 days of W taken out so that W
# has the 3,967 days of the real snapshot -------------------------------------------------------


def _calendar() -> pd.DatetimeIndex:
    days = pd.bdate_range("2010-09-01", "2026-10-12")
    keep = set(pd.to_datetime(KEEP))
    inside = [day for day in days if day >= FIRST]
    removed: list[pd.Timestamp] = []
    pos = 20
    while len(removed) < 149:
        if inside[pos] not in keep:
            removed.append(inside[pos])
        pos += 27
    return days.difference(pd.DatetimeIndex(removed))


# Days the tests name; never taken out of the synthetic calendar.
KEEP = (
    "2010-12-31",
    "2011-01-03",
    "2011-09-30",
    "2011-10-03",
    "2015-10-01",
    "2015-10-07",
    "2016-01-28",
    "2022-10-26",
    "2022-10-27",
    "2026-09-30",
    "2026-10-09",
    "2026-10-12",
)
CAL = _calendar()
W = CAL[(CAL >= FIRST) & (CAL <= LAST)]
assert len(W) == 3967 and W[0] == FIRST and W[-1] == LAST
# The sixteen merged top tens printed by the third read-only probe (names-after-2010.md v2).
PINNED: dict[int, list[tuple[str, str, int]]] = {
    2010: [
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
    ],
    2011: [
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
    ],
    2012: [
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
    ],
    2013: [
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
    ],
    2014: [
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
    ],
    2015: [
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
    ],
    2016: [
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
    ],
    2017: [
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
    ],
    2018: [
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
    ],
    2019: [
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
    ],
    2020: [
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
    ],
    2021: [
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
    ],
    2022: [
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
    ],
    2023: [
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
    ],
    2024: [
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
    ],
    2025: [
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
    ],
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
    2010: 21232784912,
    2011: 21485139915,
    2012: 34622078740,
    2013: 38231088980,
    2014: 42033212910,
    2015: 36804988670,
    2016: 39515775492,
    2017: 52162471046,
    2018: 73980573140,
    2019: 75028000046,
    2020: 135683471517,
    2021: 182439642506,
    2022: 147749119851,
    2023: 196527434884,
    2024: 294370101471,
    2025: 385205839095,
}
HOLDINGS = {
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
ISSUERS = {
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
TOP5 = (
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
# Schedule names that give each pinned issuer key; two-class issuers get two rows.
NAMES = {
    "apple": ["Apple, Inc."],
    "qualcomm": ["QUALCOMM, Inc."],
    "google": ["Google, Inc., Class A*", "Google, Inc., Class C*"],
    "alphabet": ["Alphabet, Inc., Class A*", "Alphabet, Inc., Class C*"],
    "microsoft": ["Microsoft Corp."],
    "oracle": ["Oracle Corp."],
    "amazon": ["Amazon.com, Inc.*"],
    "cisco systems": ["Cisco Systems, Inc."],
    "teva pharmaceutical industries": ["Teva Pharmaceutical Industries Ltd. ADR"],
    "intel": ["Intel Corp."],
    "gilead sciences": ["Gilead Sciences, Inc.*"],
    "amgen": ["Amgen, Inc."],
    "comcast": ["Comcast Corp., Class A", "Comcast Corp., Class A"],
    "kraft foods": ["Kraft Foods, Inc., Class A"],
    "facebook": ["Facebook, Inc., Class A*"],
    "kraft heinz": ["Kraft Heinz Co. (The)"],
    "celgene": ["Celgene Corp.*"],
    "nvidia": ["NVIDIA Corp."],
    "netflix": ["Netflix, Inc.*"],
    "pepsico": ["PepsiCo, Inc."],
    "adobe": ["Adobe, Inc.*"],
    "paypal holdings": ["PayPal Holdings, Inc.*"],
    "tesla": ["Tesla, Inc.*"],
    "meta platforms": ["Meta Platforms, Inc., Class A"],
    "costco wholesale": ["Costco Wholesale Corp."],
    "broadcom": ["Broadcom, Inc."],
    "palantir technologies": ["Palantir Technologies, Inc., Class A*"],
}
# Issuers with two rows in a year's schedule (the probe's same-key rows).
TWO_ROWS = {(y, "google") for y in (2014, 2015)} | {(y, "alphabet") for y in range(2016, 2026)}
TWO_ROWS |= {(2015, "comcast")}
BOOKING = ("Booking Holdings, Inc.", 669_539, 3_615_021_837)
BILLION = 1_000_000_000


# --- builders ------------------------------------------------------------------------------------


def _letters(k: int) -> str:
    return "".join(chr(ord("a") + (k // 26**i) % 26) for i in (2, 1, 0)).capitalize()


def _htm(rows: list[tuple[str, int, int]], total: int, *, shares_first: bool) -> bytes:
    parts = [
        "<html><head><style>td { color: red; }</style>",
        "<script>var x = '<tr><td>Phantom Holding Inc.</td><td>1,000,000</td>"
        "<td>50,000,000</td></tr>';</script></head><body>",
        "<p>SCHEDULE OF INVESTMENTS</p><p>September&#160;30</p><table>",
        "<tr><th>Common Stocks</th><th>Shares</th><th>Value</th></tr>",
        "<tr><td>Internet Software &amp; Services</td><td>12,345,678,901</td></tr>",
    ]
    for name, shares, value in rows:
        cells = [f"<font>{name.replace('&', '&amp;')}</font>", f"{shares:,}"]
        if shares_first:
            cells.reverse()
        parts.append(
            "<tr>"
            + "".join(f"<td>{c}</td>" for c in cells)
            + f"<td>$</td><td>{value:,}&nbsp;</td></TR>"
        )
    if shares_first:
        parts.append(
            "<tr><td>Total Investments (Cost $41,613,978,571)&#8212;99.9%</td>"
            f"<td>{total:,}</td></tr>"
        )
        parts.append("<tr><td>Total investments, at cost</td><td>41,613,978,571</td></tr>")
    else:
        parts.append(
            f"<tr><td><b>Total Investments</b> (Cost&#160;$15,000,000,000)</td><td>$</td>"
            f"<td>{total:,}</td></tr>"
        )
    return "\n".join(parts + ["</table></body></html>"]).encode("utf-8")


def _schedule(year: int) -> list[tuple[str, int, int]]:
    pinned: list[tuple[str, int, int]] = []
    for key, _, value in PINNED[year]:
        if (year, key) in TWO_ROWS:
            first = value * 3 // 5
            names = NAMES[key]
            pinned += [
                (names[0], first // 40, first),
                (names[1], (value - first) // 40, value - first),
            ]
        else:
            pinned.append((NAMES[key][0], value // 40, value))
    fillers = ROWS[year] - len(pinned)
    rest = TOTALS[year] - sum(row[2] for row in pinned)
    base = rest // fillers
    filler = [
        (f"Filler{_letters(k)} Inc.*", (base + 1000 * k) // 50, base + 1000 * k)
        for k in range(fillers)
    ]
    if year >= 2023:
        filler[0] = ("CoStar Group, Inc.", filler[0][1], filler[0][2])
    assert max(row[2] for row in filler) < PINNED[year][-1][2]
    return pinned + filler


def _document(year: int) -> tuple[str, bytes]:
    accession, name = HOLDINGS[year]
    rows = _schedule(year)
    if year == 2025:
        rows = rows[:50] + [BOOKING, ("Tiny Priced Inc.", 10, 5_000_000)] + rows[50:]
    return name, _htm(rows, TOTALS[year], shares_first=accession in ROW_BY_ROW)


DOCUMENTS = {year: _document(year) for year in HOLDINGS}
ACCESSIONS = {year: accession for year, (accession, _) in HOLDINGS.items()}
COUNTER = iter(range(1, 10**6))


def _sec_time(eastern: str) -> str:
    stamp = pd.Timestamp(eastern).tz_localize(EASTERN).tz_convert("UTC")
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _filing(
    issuer: str,
    eastern: str,
    *,
    form: str = "8-K",
    items: str = "2.02,9.01",
    cik: int | None = None,
    accession: str | None = None,
) -> dict[str, Any]:
    return {
        "cik": ISSUERS[issuer][0] if cik is None else cik,
        "accessionNumber": accession or f"0000000000-26-{next(COUNTER):06d}",
        "form": form,
        "filingDate": eastern[:10],
        "acceptanceDateTime": _sec_time(eastern),
        "items": items,
    }


def _filings(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(ee.FILING_COLUMNS))


def _goog_cik(eastern: str) -> int:
    return 1288776 if eastern < "2015-10-01" else 1652044


def _season() -> list[dict[str, Any]]:
    """Each of the thirteen top-5 companies reports once a quarter after the close."""

    rows = []
    for year in range(2010, 2027):
        for month in (1, 4, 7, 10):
            for k, issuer in enumerate(TOP5):
                eastern = f"{year}-{month:02d}-{2 + 2 * k:02d} 16:05"
                if "2010-10-01" <= eastern <= "2026-10-12":
                    cik = _goog_cik(eastern) if issuer == "GOOG" else None
                    rows.append((issuer, eastern, "8-K", "2.02,9.01", cik))
    # Google and Alphabet both file the 2015-10 results; the event day counts once.
    rows.append(("GOOG", "2015-10-06 16:06", "8-K", "2.02,9.01", 1288776))
    rows.append(("INTC", "2012-01-19 16:10", "8-K/A", "2.02,9.01", None))
    rows.append(("COST", "2022-10-06 16:20", "8-K", "2.02", None))
    rows.append(("MSFT", "2016-07-21 16:30", "8-K", "8.01", None))
    return [
        _filing(
            issuer, eastern, form=form, items=items, cik=cik, accession=f"0000000001-26-{k:06d}"
        )
        for k, (issuer, eastern, form, items, cik) in enumerate(rows, start=1)
    ]


def _returns(seed: int = 7, *, calendar: pd.DatetimeIndex = CAL) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(0.0004, 0.015, len(calendar)), index=calendar, name="r")


NO_PRICES = pd.DataFrame({"ticker": [], "date": [], "adj_close": []})


def _snapshot(
    filings: list[dict[str, Any]] | None = None,
    documents: dict[int, tuple[str, bytes]] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        snapshot_id=E1,
        spec=SPEC,
        entities=(),
        filings=_filings(_season() if filings is None else filings),
        documents=DOCUMENTS if documents is None else documents,
        prices=NO_PRICES,
    )


SOURCES = {"earnings": {"snapshot_id": E1}, "qqq": {"snapshot_id": "qlh-20261013-0b0df53cb1a2"}}
READ = {
    "window_id": "qqq_after_2010_owner_print",
    "role": "owner_print_only",
    "read_policy": "once_per_owner_decision",
    "start": "2011-01-03",
    "end": "latest",
    "owner_decision": OD,
    "reads": 1,
}


def _inputs(returns: pd.Series | None = None, snapshot: SimpleNamespace | None = None) -> ea.Inputs:
    return ea.Inputs(
        snapshot=_snapshot() if snapshot is None else snapshot,
        qqq=_returns() if returns is None else returns,
        sources=SOURCES,
        window_read=READ,
    )


def _crashing(returns: pd.Series, days: pd.DatetimeIndex, value: float = -0.06) -> pd.Series:
    out = returns.copy()
    out[days] = value
    return out


BASE = _inputs()
BUILT = ea.build(BASE)
TOP5_DAYS = es.days_of(BUILT.sets["top5"])
# Every event day is one of QQQ's worst days: all three lines pass.
HOLDS = _inputs(returns=_crashing(_returns(), TOP5_DAYS))


@pytest.fixture(scope="module")
def results() -> dict[str, tuple[dict[str, Any], list[dict[str, Any]]]]:
    return {"base": ea.read(BASE), "holds": ea.read(HOLDS)}


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("check must not compute this")


# --- the design ----------------------------------------------------------------------------------


def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert ea.RUN_ID == "TRADING-2565-130"
    assert ea.OWNER_DECISION == OD == SPEC.owner_decision
    assert ea.EARNINGS_SNAPSHOT_ID == E1
    assert ea.QQQ_SNAPSHOT_ID == es.QQQ_SNAPSHOT_ID == "qlh-20261013-0b0df53cb1a2"
    assert ea.QQQ_COLUMN == es.QQQ_COLUMN == "etf_reference_return"
    assert ea.WINDOW_ID == ssp.QQQ_WINDOW_ID == SPEC.window_id == "qqq_after_2010_owner_print"
    assert (
        ea.SPEC_PATH == PROJECT_ROOT / "config" / "research" / "research_earnings_after_2010.yaml"
    )
    assert (ea.FIRST_DAY, ea.LAST_DAY, ea.WINDOW_DAYS, ea.LAST_LIST) == (FIRST, LAST, 3967, 2025)
    assert pd.Timestamp(SPEC.data_end) == LAST and pd.Timestamp(SPEC.filing_start) == pd.Timestamp(
        "2010-10-01"
    )
    # The rules of TRADING-2565-99, unchanged and read from it.
    assert (es.TOP, es.SENSITIVE_TOPS, es.BAD_SHARE, es.SENSITIVE_BAD) == (
        5,
        (3, 10),
        0.05,
        (0.025, 0.1),
    )
    assert (es.DRAWS, es.SEED, es.SHIFTS, es.SHIFT_NOTE) == (1000, 20261030, (-5, -1, 1, 5), 0.3)
    assert (es.LINE_C, es.P_LINE, es.TOO_FEW_EVENTS, es.TOO_FEW_EXPECTED) == (2.0, 0.05, 80, 4)
    assert (es.MIN_VALUE, es.PRICE_RANGE, es.MAX_SHARE, es.SUM_RANGE) == (
        1_000_000,
        (0.5, 5000.0),
        0.3,
        (0.97, 1.03),
    )
    assert ea.VERDICTS == ("too_few", "holds", "not_holds")
    assert ea.PRICED_OUT_MIN == 100_000_000
    assert (ea.CHECK, ea.EVENTS, ea.SUMMARY, ea.REPORT) == (
        "earnings_after_2010_check.json",
        "earnings_after_2010_events.jsonl",
        "earnings_after_2010_table.json",
        "earnings_after_2010_table.md",
    )


def test_the_pinned_list_is_the_probe_table() -> None:
    assert {year: list(rows) for year, rows in ea.TOP10.items()} == PINNED
    assert ea.ROWS == ROWS and ea.TOTALS == TOTALS and ea.HOLDINGS == HOLDINGS
    assert ea.ROW_BY_ROW == frozenset(ROW_BY_ROW)
    assert ea.LISTS == {year: tuple(i for _, i, _ in rows) for year, rows in PINNED.items()}
    assert ea.FOREIGN == ("TEVA",)
    assert ea.TOP5_IDS == TOP5
    assert set(TOP5) == {i for ids in ea.LISTS.values() for i in ids[:5]}
    assert {i for ids in ea.LISTS.values() for i in ids} - {"TEVA"} == set(ISSUERS)
    assert {d.fiscal_year: (d.accession, d.document) for d in SPEC.holdings_documents} == HOLDINGS


def test_the_issuers_are_the_configured_ones() -> None:
    assert ea.ISSUERS == ISSUERS
    assert ea.spec_problems(SPEC) == []
    assert ea.spec_problems(ee.load_spec(ea.SPEC_PATH)) == []


def test_spec_problems_name_each_mismatch() -> None:
    def issuers(**change: Any) -> Any:
        return tuple(i.model_copy(update=change) if i.id == "GOOG" else i for i in SPEC.issuers)

    issuers_problem = ["the configured issuers are not the module's"]
    assert ea.spec_problems(SPEC.model_copy(update={"issuers": issuers(ciks=(1652044,))})) == (
        issuers_problem
    )
    assert ea.spec_problems(SPEC.model_copy(update={"issuers": issuers(fmp_ticker="GOOG")})) == (
        issuers_problem
    )
    assert ea.spec_problems(SPEC.model_copy(update={"issuers": SPEC.issuers[:-1]})) == (
        issuers_problem
    )
    assert ea.spec_problems(SPEC.model_copy(update={"foreign": ()})) == [
        "the configured foreign issuers are not the module's"
    ]
    documents = list(SPEC.holdings_documents)
    documents[6] = documents[6].model_copy(update={"accession": "0001193125-16-806696"})
    assert ea.spec_problems(SPEC.model_copy(update={"holdings_documents": tuple(documents)})) == [
        "the configured holdings documents are not the module's"
    ]
    window = ["the configured window is not the module's"]
    for change in (
        {"data_end": pd.Timestamp("2026-10-13").date()},
        {"filing_start": pd.Timestamp("2010-09-30").date()},
        {"owner_decision": "owner_decision:TRADING-2565:2026-11-03:structure_shift_other_periods"},
        {"window_id": None},
        {"price_start": pd.Timestamp("2010-10-01").date()},
    ):
        assert ea.spec_problems(SPEC.model_copy(update=change)) == window, change


def test_the_reading_texts_are_fixed_before_the_run() -> None:
    assert ea.HOLDS_SENTENCE == (
        "2004–2010、2011–2026 两段都看到 QQQ 的坏日子扎堆在大权重股财报后第一天，"
        "躲开的代价测不出为正"
    )
    assert ea.FAMILY_SENTENCE == (
        "财报冲击：2011–2026 没过（{lines}；C {c}，p {p}，差 {diff}，区间 {low} 到 {high}）；"
        "这一族的历史证据收掉"
    )
    assert ea.TOO_FEW_SENTENCE == es.TOO_FEW_SENTENCE
    assert ea.SHIFT_SENTENCE == es.SHIFT_SENTENCE == "多半是财报季，不只是那一天"
    assert ea.SHIFT_HOLDS == "照平移读是 not_holds"
    assert ea.NOT_AGAIN == "不再用别的历史段问这件事"
    assert ea.HOLDS_NEXT == (
        "当天告诉 Baker；11-27 续期卡写这一句，连同平移对照；第 7 期第 3.4 题的两条账本线照常记"
    )
    assert ea.NOT_HOLDS_NEXT == "续期卡问第 7 期第 3.4 题的两条按财报日程降仓的账本线还留不留"
    assert ea.LINE_NAMES == es.LINE_NAMES
    assert len(ea.LIMITS) == 12
    for text in (
        "8-K 接收时间是发布时间的上界",
        "快照止于 2026-10-12",
        "33.8%–47.0%",
        "2020-03、2022 年的坏日子成堆，是宏观下跌",
        "同一天几家一起发",
        "不是没看过的历史",
        "TEVA 2011-01..09 跳过",
    ):
        assert any(text in limit for limit in ea.LIMITS), text


def test_the_imported_modules_are_unchanged_and_nothing_is_fetched() -> None:
    for name, digest in IMPORTED.items():
        assert hashlib.sha256((PROBES / name).read_bytes()).hexdigest() == digest, name
    source = MODULE.read_text(encoding="utf-8")
    assert "earnings_shock_table as es" in source
    for word in (
        "requests",
        "fetch_snapshot",
        "ledger",
        "urlopen",
        "FMP_API_KEY",
        "adj_close",
        "stock_returns",
        "crash",
    ):
        assert word not in source, word


# --- holdings ------------------------------------------------------------------------------------


def test_one_row_one_holding_reads_the_words_then_the_last_two_numbers() -> None:
    assert ea.row_holdings(["37,765,277", "Apple, Inc.", "4,269,364,565"]) == [
        ("Apple, Inc.", 37_765_277, 4_269_364_565)
    ]
    assert ea.row_holdings(["Apple, Inc.", "37,765,277", "$", "4,269,364,565"]) == [
        ("Apple, Inc.", 37_765_277, 4_269_364_565)
    ]
    assert ea.row_holdings(["1,000", "Alpha", "2,000", "Beta", "3,000,000"]) == [
        ("Alpha Beta", 2000, 3_000_000)
    ]
    assert ea.row_holdings(["Internet Software", "12,345,678,901"]) == []
    assert ea.row_holdings(["1,000", "2,000"]) == []
    # The 2003-2010 rule reads nothing from a shares-first row.
    assert es.holdings(["37,765,277", "Apple, Inc.", "4,269,364,565"]) == []


def test_cost_is_skipped_only_as_a_whole_word() -> None:
    rows = [
        ("Costco Wholesale Corp.", 1_000_000, 90_000_000),
        ("CoStar Group, Inc.", 1_000_000, 80_000_000),
        ("Accost Holdings", 1_000_000, 70_000_000),
        ("Cost of investments", 1_000_000, 60_000_000),
        ("Low Cost Airline", 1_000_000, 50_000_000),
        ("Cash Equivalents", 1_000_000, 40_000_000),
        ("Net assets", 1_000_000, 30_000_000),
    ]
    parsed = ea.parse_document(
        "x.htm", _htm(rows, 1_000_000_000, shares_first=False), row_by_row=False
    )
    assert [name for name, _, _ in parsed.rows] == [
        "Costco Wholesale Corp",
        "CoStar Group, Inc",
        "Accost Holdings",
    ]
    assert ea.SKIP.search("COST of goods") and not ea.SKIP.search("Costco")
    assert es.SKIP.search("Costco")


def test_the_two_layouts_are_read_by_the_rule_named_for_them() -> None:
    rows = [("Alpha Corp.", 1_000_000, 50_000_000), ("Beta Inc.", 2_000_000, 40_000_000)]
    shares_first = _htm(rows, BILLION, shares_first=True)
    parsed = ea.parse_document("d.htm", shares_first, row_by_row=True)
    assert parsed.total == BILLION
    assert parsed.rows == (
        ("Alpha Corp", 1_000_000, 50_000_000),
        ("Beta Inc", 2_000_000, 40_000_000),
    )
    assert ea.parse_document("d.htm", shares_first, row_by_row=False).rows == ()
    groups = _htm(rows, BILLION, shares_first=False)
    assert ea.parse_document("d.htm", groups, row_by_row=False).rows == parsed.rows
    assert ea.parse_document("d.htm", groups, row_by_row=True).rows == parsed.rows
    with pytest.raises(ea.TableError, match="no total investments row"):
        ea.parse_document(
            "d.htm", b"<tr><td>Alpha</td><td>1,000</td><td>50,000</td></tr>", row_by_row=True
        )


def test_a_row_of_30_percent_of_the_total_or_more_is_not_a_holding() -> None:
    rows = [("Huge Corp.", 1_000_000, 300_000_000), ("Large Corp.", 1_000_000, 299_999_999)]
    for shares_first in (False, True):
        parsed = ea.parse_document(
            "x.htm", _htm(rows, BILLION, shares_first=shares_first), row_by_row=shares_first
        )
        assert parsed.rows == (("Large Corp", 1_000_000, 299_999_999),)


def test_rows_dropped_by_the_price_rule_are_printed_from_100_million() -> None:
    rows = [
        ("Keep Me Inc.", 1_000_000, 50_000_000),
        BOOKING,
        ("Edge Priced Inc.", 10_000, 100_000_000),
        ("Small Priced Inc.", 10_000, 99_999_999),
        ("Penny Priced Inc.", 1_000_000_000, 200_000_000),
    ]
    parsed = ea.parse_document(
        "x.htm", _htm(rows, 20_000_000_000, shares_first=False), row_by_row=False
    )
    assert parsed.rows == (("Keep Me Inc", 1_000_000, 50_000_000),)
    assert parsed.priced_out == (
        ("Booking Holdings, Inc", 669_539, 3_615_021_837),
        ("Penny Priced Inc", 1_000_000_000, 200_000_000),
        ("Edge Priced Inc", 10_000, 100_000_000),
    )


def _small(
    rows: list[tuple[str, int, int]], total: int = BILLION, *, shares_first: bool = False
) -> Any:
    return {2016: ("s.htm", _htm(rows, total, shares_first=shares_first))}


def _top(big: int, middle: int = 200_000_000) -> dict[int, tuple[tuple[str, str, int], ...]]:
    return {2016: (("big", "AAPL", big), ("middle", "MSFT", middle))}


def _lists(rows: list[tuple[str, int, int]], count: int, **kwargs: Any) -> Any:
    shares_first = kwargs.pop("shares_first", False)
    accession = "0001193125-16-806695" if shares_first else "0000000000-16-000001"
    options = {
        "accessions": {2016: accession},
        "top10": _top(300_000_000),
        "rows": {2016: count},
        "totals": {2016: BILLION},
    }
    return ea.holdings_lists(_small(rows, shares_first=shares_first), **{**options, **kwargs})


TABLE = [
    ("Big Corp., Class A", 1_000_000, 200_000_000),
    ("Big Corp., Class C", 1_000_000, 100_000_000),
    ("Middle Inc.", 1_000_000, 200_000_000),
    ("Rest Aaa", 1_000_000, 175_000_000),
    ("Rest Aab", 1_000_000, 175_000_000),
    ("Rest Aac", 1_000_000, 150_000_000),
]


def test_the_list_merges_every_share_class_and_reads_the_layout_by_accession() -> None:
    for shares_first in (False, True):
        lists, report = _lists(TABLE, 6, shares_first=shares_first)
        assert lists == {2016: ("AAPL", "MSFT")}
        assert report == [
            {
                "fiscal_year": 2016,
                "accession": "0001193125-16-806695" if shares_first else "0000000000-16-000001",
                "document": "s.htm",
                "rule": "row_by_row" if shares_first else "groups",
                "rows": 6,
                "total": BILLION,
                "sum_share": 1.0,
                "same_key": [{"key": "big", "rows": 2}],
                "class_rows": 2,
                "priced_out": [],
                "top10": ["big", "middle"],
            }
        ]
    # A shares-first schedule read by the 2003-2010 rule finds no row.
    with pytest.raises(ea.TableError, match="the parsed row count is not the probe's"):
        _lists(TABLE, 6, shares_first=True, accessions={2016: "0000000000-16-000001"})
    # A company outside the pinned list whose classes add up past the second name.
    rows = TABLE[:5] + [("Rest Aaa, Class B", 1_000_000, 150_000_000)]
    with pytest.raises(ea.TableError) as caught:
        _lists(rows, 6)
    assert str(caught.value) == "fiscal year 2016: the merged top ten is not the pinned list"


@pytest.mark.parametrize(
    ("count", "document_total", "pinned_total", "problem"),
    [
        (5, BILLION, BILLION, "the parsed row count is not the probe's"),
        (6, BILLION, BILLION + 1, "total investments is not the pinned total"),
        (6, 1_500_000_000, 1_500_000_000, "the parsed rows do not add up to the total"),
    ],
)
def test_the_list_refuses_a_count_a_total_or_a_sum(
    count: int, document_total: int, pinned_total: int, problem: str
) -> None:
    with pytest.raises(ea.TableError) as caught:
        ea.holdings_lists(
            _small(TABLE, document_total),
            accessions={2016: "0000000000-16-000001"},
            top10=_top(300_000_000),
            rows={2016: count},
            totals={2016: pinned_total},
        )
    assert str(caught.value) == f"fiscal year 2016: {problem}"


def test_the_list_needs_every_year_and_its_accession() -> None:
    options = {"rows": {2016: 6}, "totals": {2016: BILLION}}
    accessions = {2016: "0000000000-16-000001"}
    lists, _ = ea.holdings_lists(
        _small(TABLE), accessions=accessions, top10=_top(300_000_000), **options
    )
    assert lists == {2016: ("AAPL", "MSFT")}
    with pytest.raises(ea.TableError) as caught:
        ea.holdings_lists(
            _small(TABLE), accessions=accessions, top10={**_top(300_000_000), 2017: ()}, **options
        )
    assert str(caught.value) == "fiscal year 2017: no holdings document"
    with pytest.raises(ea.TableError) as caught:
        ea.holdings_lists(_small(TABLE), accessions={}, top10=_top(300_000_000), **options)
    assert str(caught.value) == "fiscal year 2016: no holdings document"
    with pytest.raises(ea.TableError) as caught:
        ea.holdings_lists(
            {2016: ("s.htm", b"<tr><td>Big Corp</td></tr>")},
            accessions=accessions,
            top10=_top(1),
            **options,
        )
    assert str(caught.value) == "fiscal year 2016: no total investments row"


@pytest.mark.parametrize(
    ("last", "ok"),
    [
        (120_000_000, True),
        (119_999_999, False),
        (180_000_000, True),
        (180_000_001, False),
    ],
)
def test_the_sum_share_stays_inside_0_97_and_1_03(last: int, ok: bool) -> None:
    rows = [("Big Corp", 1_000_000, 250_000_000), ("Middle Inc", 1_000_000, 200_000_000)]
    rows += [(f"Rest {_letters(k)}", 1_000_000, 100_000_000) for k in range(4)]
    rows += [("Last Holding", 1_000_000, last)]
    if ok:
        _lists(rows, 7, top10=_top(250_000_000))
    else:
        with pytest.raises(ea.TableError, match="do not add up"):
            _lists(rows, 7, top10=_top(250_000_000))


def test_the_sixteen_schedules_give_the_pinned_lists() -> None:
    lists, report = ea.holdings_lists(DOCUMENTS)
    assert lists == ea.LISTS
    assert [r["fiscal_year"] for r in report] == list(range(2010, 2026))
    assert [r["rows"] for r in report] == list(ROWS.values())
    assert [r["total"] for r in report] == list(TOTALS.values())
    assert [r["accession"] for r in report] == [a for a, _ in HOLDINGS.values()]
    assert [r["document"] for r in report] == [d for _, d in HOLDINGS.values()]
    assert [r["rule"] for r in report] == ["groups"] * 6 + ["row_by_row"] * 3 + ["groups"] * 7
    assert all(0.999 < r["sum_share"] < 1.001 for r in report)
    same = {r["fiscal_year"]: r["same_key"] for r in report}
    assert same[2013] == [] and same[2014] == [{"key": "google", "rows": 2}]
    assert same[2015] == [{"key": "comcast", "rows": 2}, {"key": "google", "rows": 2}]
    assert same[2025] == [{"key": "alphabet", "rows": 2}]
    priced = {r["fiscal_year"]: r["priced_out"] for r in report}
    assert priced[2025] == [
        {"name": "Booking Holdings, Inc", "shares": 669_539, "value": 3_615_021_837}
    ]
    assert all(priced[year] == [] for year in range(2010, 2025))
    assert report[12]["top10"] == [key for key, _, _ in PINNED[2022]]
    # The pinned Costco rows survive the whole-word skip; the old skip list breaks the 2022 list.
    assert "costco wholesale" in report[12]["top10"]


# --- events --------------------------------------------------------------------------------------


def test_results_map_every_cik_of_an_issuer_and_refuse_a_stranger() -> None:
    rows = [
        _filing("GOOG", "2015-07-16 16:05", cik=1288776),
        _filing("GOOG", "2016-01-28 16:05", cik=1652044),
        _filing("MSFT", "2015-01-20 16:05", items="12"),
        _filing("CSCO", "2015-01-19 16:05", form="8-K/A", items="2.02"),
        _filing("QCOM", "2015-01-19 16:06", items="1.01,2.020"),
        _filing("COST", "2015-01-19 16:07", items="7.01, 2.02 "),
    ]
    found = ea.results(_filings(rows))
    assert found["id"].tolist() == ["COST", "MSFT", "GOOG", "GOOG"]
    assert found.columns.tolist() == ["id", "accession", "accepted"]
    assert found["accepted"].tolist()[1] == pd.Timestamp("2015-01-20 21:05", tz="UTC")
    for cik in (1234567, 1441634, 1649338):
        with pytest.raises(ea.TableError, match="not one of the configured issuers"):
            ea.results(_filings([_filing("AVGO", "2015-01-20 16:05", cik=cik)]))


def test_the_last_list_runs_to_the_end_of_the_window() -> None:
    assert [
        ea.list_year(pd.Timestamp(d))
        for d in (
            "2011-01-03",
            "2011-09-30",
            "2011-10-03",
            "2025-09-30",
            "2025-10-01",
            "2026-09-30",
            "2026-10-12",
        )
    ] == [2010, 2010, 2011, 2024, 2025, 2025, 2025]


def _events(rows: list[dict[str, Any]], top: int = 5) -> pd.DataFrame:
    return ea.events(ea.results(_filings(rows)), CAL, W, ea.LISTS, top)


def test_events_take_the_list_of_the_event_day_and_stay_inside_w() -> None:
    rows = [
        _filing("AAPL", "2010-12-31 16:30"),
        _filing("AAPL", "2010-12-31 15:00"),
        _filing("ORCL", "2011-09-30 16:30"),
        _filing("ORCL", "2011-09-30 15:00"),
        _filing("META", "2015-09-30 16:30"),
        _filing("NVDA", "2026-10-09 16:30"),
        _filing("MSFT", "2026-10-12 15:00"),
        _filing("MSFT", "2026-10-12 16:30"),
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
        ("AAPL", 1, 2010, "2011-01-03"),
        ("ORCL", 5, 2010, "2011-09-30"),
        ("ORCL", 3, 2011, "2011-10-03"),
        ("META", 5, 2015, "2015-10-01"),
        ("NVDA", 1, 2025, "2026-10-12"),
        ("MSFT", 2, 2025, "2026-10-12"),
    ]
    assert found["accession"].tolist() == [rows[i]["accessionNumber"] for i in (0, 3, 2, 4, 5, 6)]


def test_teva_is_skipped_not_replaced() -> None:
    rows = [_filing(i, "2011-04-19 16:05") for i in ("AMZN", "CSCO", "INTC", "GILD")]
    found = _events(rows, top=10)
    assert sorted(zip(found["id"], found["rank"], strict=True)) == [
        ("AMZN", 6),
        ("CSCO", 7),
        ("GILD", 10),
        ("INTC", 9),
    ]
    assert _events(rows, top=5).empty


def test_two_ciks_on_one_day_are_one_event_day() -> None:
    rows = [
        _filing("GOOG", "2015-10-06 16:05", cik=1652044),
        _filing("GOOG", "2015-10-06 16:06", cik=1288776),
        _filing("AAPL", "2015-10-06 17:00"),
    ]
    found = _events(rows)
    assert found["id"].tolist() == ["GOOG", "GOOG", "AAPL"]
    assert list(es.days_of(found)) == [pd.Timestamp("2015-10-07")]


# --- the measures --------------------------------------------------------------------------------


def test_the_shift_table_adds_the_days_landing_back_in_e() -> None:
    window = W[:30]
    days = window[[0, 1, 3, 27, 29]]
    bad = window[[2, 8, 26]]
    rows = ea.shift_rows(days, window, bad)
    assert [(r["shift"], r["events"], r["hits"], r["back_in_e"]) for r in rows] == [
        (-5, 2, 0, 0),
        (-1, 4, 2, 1),
        (1, 4, 1, 1),
        (5, 3, 1, 0),
    ]
    assert [{k: v for k, v in r.items() if k != "back_in_e"} for r in rows] == es.shift_rows(
        days, window, bad
    )


def test_each_year_from_2011_to_2026_has_its_own_event_share() -> None:
    returns = _returns(5, calendar=W)
    bad = es.bad_days(returns)
    days = pd.DatetimeIndex(sorted(set(bad[::3]) | set(W[::40])))
    rows = ea.year_rows(days, W, bad)
    assert [r["year"] for r in rows] == list(range(2011, 2027))
    for row in rows:
        w, e, b = (x[x.year == row["year"]] for x in (W, days, bad))
        assert (row["window"], row["events"], row["bad"]) == (len(w), len(e), len(b))
        assert row["hits"] == len(e.intersection(b))
        expected = None if not len(b) or not len(e) else (row["hits"] / len(b)) / (len(e) / len(w))
        assert row["c"] == (None if expected is None else pytest.approx(expected))
    assert sum(r["window"] for r in rows) == 3967


def test_issuer_rows_count_months_and_print_no_stock_return() -> None:
    found = BUILT.sets["top5"]
    bad = es.bad_days(BUILT.returns)
    rows = ea.issuer_rows(found, bad, ea.LISTS)
    assert [(r["id"], r["months"]) for r in rows] == [
        ("AAPL", 190),
        ("MSFT", 190),
        ("GOOG", 178),
        ("AMZN", 132),
        ("QCOM", 9),
        ("ORCL", 33),
        ("INTC", 24),
        ("CSCO", 12),
        ("GILD", 12),
        ("META", 84),
        ("TSLA", 24),
        ("NVDA", 37),
        ("AVGO", 25),
    ]
    for row in rows:
        mine = found[found["id"] == row["id"]]
        assert set(row) == {"id", "months", "events", "bad_events"}
        assert row["events"] == len(mine)
        assert row["bad_events"] == int(mine["event_day"].isin(bad).sum())


def test_each_day_of_e_and_b_names_its_issuers_and_qqq_return() -> None:
    rows = [
        _filing("MSFT", "2022-10-25 16:05"),
        _filing("GOOG", "2022-10-25 16:10"),
        _filing("AAPL", "2022-10-26 16:30"),
        _filing("AMZN", "2022-10-26 16:01"),
        _filing("TSLA", "2016-01-27 16:05"),
    ]
    found = _events(rows)
    low = {
        pd.Timestamp("2022-10-26"): -0.022,
        pd.Timestamp("2022-10-27"): -0.031,
        pd.Timestamp("2016-01-28"): -0.02,
    }
    returns = pd.Series([low.get(day, 0.001) for day in W], index=W)
    bad = pd.DatetimeIndex(["2016-01-28", "2022-10-26", "2022-10-27"])
    assert ea.hit_rows(found, returns, bad) == [
        {"day": "2022-10-26", "ids": ["GOOG", "MSFT"], "qqq_return": -0.022},
        {"day": "2022-10-27", "ids": ["AAPL", "AMZN"], "qqq_return": -0.031},
    ]
    assert ea.hit_rows(found, returns, pd.DatetimeIndex([])) == []


def _main(c: float = 2.0, events: int = 300, bad: int = 199, window: int = 3967) -> dict[str, Any]:
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
        (_main(events=80, window=3980), 0.01, -0.01, "holds", []),
        (_main(events=80, window=3981), 0.01, -0.01, "too_few", []),
    ],
)
def test_the_verdict(
    main: dict[str, Any], p: float, low: float, verdict: str, failed: list[str]
) -> None:
    found = {"diff": 0.0001, "low": low, "high": 0.004}
    decision = ea.judge(main, {"p": p}, found)
    assert (decision["verdict"], decision["failed"]) == (verdict, failed)
    if verdict == "too_few":
        assert decision == {
            "verdict": "too_few",
            "lines": None,
            "failed": [],
            "sentence": es.TOO_FEW_SENTENCE,
        }
        assert ea.judge(main, None, None) == decision
    else:
        assert decision["lines"] == {
            name: name not in failed for name in ("line1", "line2", "line3")
        }


def test_the_sentences_name_which_line_failed() -> None:
    found = {"diff": 0.00012345, "low": -0.0031, "high": 0.00335}
    one = ea.judge(_main(c=1.234567), {"p": 0.04321}, found)
    assert one["sentence"] == (
        "财报冲击：2011–2026 没过（线 1；C 1.23，p 0.043，差 +0.012%，区间 -0.310% 到 +0.335%）；"
        "这一族的历史证据收掉"
    )
    two = ea.judge(_main(c=2.5), {"p": 0.2}, {**found, "low": 0.0002})
    assert two["sentence"].startswith("财报冲击：2011–2026 没过（线 2、线 3；C 2.50，p 0.200，")
    holds = ea.judge(_main(c=2.5), {"p": 0.001}, found)
    assert holds["sentence"] == ea.HOLDS_SENTENCE


def test_the_sensitivity_rows() -> None:
    bad = es.bad_days(BUILT.returns)
    found = ea.sensitivity(BUILT, bad)
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


# --- check ---------------------------------------------------------------------------------------


CHECK_KEYS = {
    "run_id",
    "owner_decision",
    "earnings_snapshot_id",
    "qqq_snapshot_id",
    "qqq_column",
    "window_read",
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
    "holdings",
}
CHECK_FORBIDDEN = (
    "bad_days",
    "concentration",
    "welch",
    "shuffle",
    "shift_rows",
    "year_rows",
    "sensitivity",
    "hit_rows",
    "issuer_rows",
)


def _forbid(patch: pytest.MonkeyPatch) -> None:
    for name in CHECK_FORBIDDEN:
        patch.setattr(es, name, _forbidden, raising=False)
        patch.setattr(ea, name, _forbidden, raising=False)


def test_check_counts_and_never_reads_a_return(monkeypatch: pytest.MonkeyPatch) -> None:
    _forbid(monkeypatch)
    summary = ea.check_summary(BUILT)
    other = _inputs(returns=_returns(99))
    assert ea.check_summary(ea.build(other)) == summary
    assert set(summary) == CHECK_KEYS
    numbers = json.dumps(summary)
    for key in ('"c"', '"hits"', '"wilson', '"mean', '"median', '"p"', "qqq_return"):
        assert key not in numbers


def test_check_counts_on_the_synthetic_season() -> None:
    summary = ea.check_summary(BUILT)
    top5 = BUILT.sets["top5"]
    assert summary["window"] == {"first": "2011-01-03", "last": "2026-10-12", "days": 3967}
    assert summary["bad_days"] == 199
    assert summary["filings"] == {"rows": len(_season()), "results": len(_season()) - 2}
    for name in ea.SETS:
        days = BUILT.sets[name]["event_day"]
        assert summary["sets"][name] == {
            "rows": len(days),
            "days": days.nunique(),
            "repeated_days": int((days.value_counts() > 1).sum()),
        }
    # 63 quarters of five, three of the 2025 list in 2026-10, and Google's second CIK in 2015-10.
    assert summary["sets"]["top5"]["rows"] == 63 * 5 + 3 + 1
    assert summary["expected_hits"] == pytest.approx(199 * top5["event_day"].nunique() / 3967)
    assert summary["too_few"] is False
    hours = pd.DatetimeIndex(top5["accepted"]).tz_convert(EASTERN).hour
    assert summary["hours"] == {f"{h:02d}": int((hours == h).sum()) for h in range(24)}
    assert summary["evening_share"] == pytest.approx(float((hours >= 16).mean()))
    per_list = {(r["list_year"], r["id"]): r for r in summary["per_list"]}
    assert len(per_list) == 159 and "TEVA" not in {i for _, i in per_list}
    assert per_list[(2010, "INTC")] == {"list_year": 2010, "rank": 9, "id": "INTC", "events": 3}
    assert per_list[(2015, "GOOG")]["events"] == 5
    assert per_list[(2025, "AAPL")]["events"] == 5
    # Costco's 2022-10-06 filing is a top-10 event of the 2022 list (rank 9).
    assert per_list[(2022, "COST")]["events"] == 1
    assert summary["holdings"] == ea.holdings_lists(DOCUMENTS)[1]
    assert summary["window_read"] == READ and summary["sources"] == SOURCES


def test_too_few_counts_the_days_and_the_expected_hits() -> None:
    few = [row for row in _season() if row["filingDate"] < "2013-10-01"]
    summary = ea.check_summary(ea.build(_inputs(snapshot=_snapshot(filings=few))))
    assert summary["sets"]["top5"]["days"] < 80 and summary["too_few"] is True


# --- the read ------------------------------------------------------------------------------------


RESULT_KEYS = {
    "run_id",
    "owner_decision",
    "earnings_snapshot_id",
    "qqq_snapshot_id",
    "window_read",
    "check",
    "main",
    "shuffle",
    "welch",
    "shifts",
    "years",
    "issuers",
    "hits",
    "sensitivity",
    "decision",
}


def test_the_read_has_every_part_and_dumps_as_json(results: dict[str, Any]) -> None:
    result, rows = results["base"]
    assert set(result) == RESULT_KEYS
    assert result["window_read"] == READ
    assert result["check"] == ea.check_summary(BUILT)
    bad = es.bad_days(BUILT.returns)
    assert len(bad) == 199
    assert result["main"] == es.concentration(TOP5_DAYS, W, bad)
    assert result["welch"] == es.welch(BUILT.returns, TOP5_DAYS, bad)
    assert result["shuffle"] == es.shuffle(TOP5_DAYS, W, bad, draws=1000, seed=20261030)
    assert result["shifts"] == ea.shift_rows(TOP5_DAYS, W, bad)
    assert result["years"] == ea.year_rows(TOP5_DAYS, W, bad)
    assert result["issuers"] == ea.issuer_rows(BUILT.sets["top5"], bad, ea.LISTS)
    assert result["hits"] == ea.hit_rows(BUILT.sets["top5"], BUILT.returns, bad)
    assert len(result["hits"]) == result["main"]["hits"]
    assert result["sensitivity"] == ea.sensitivity(BUILT, bad)
    decision = result["decision"]
    assert {k: v for k, v in decision.items() if k != "shift_note"} == ea.judge(
        result["main"], result["shuffle"], result["welch"]
    )
    assert decision["shift_note"] == es.shift_note(result["main"]["c"], result["shifts"])
    json.dumps(result, allow_nan=False)
    first = BUILT.sets["top5"].iloc[0]
    assert rows[0] == {
        "id": first["id"],
        "rank": int(first["rank"]),
        "list_year": 2010,
        "accession": first["accession"],
        "accepted_utc": first["accepted"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "accepted_eastern": first["accepted"].tz_convert(EASTERN).isoformat(),
        "event_day": first["event_day"].date().isoformat(),
    }
    assert len(rows) == 319 and all(set(r) == set(rows[0]) for r in rows)


def test_an_event_on_every_bad_day_holds(results: dict[str, Any]) -> None:
    result, _ = results["holds"]
    assert result["decision"]["verdict"] == "holds"
    assert result["main"]["hits"] == 199 and result["shuffle"]["p"] == 1 / 1001
    assert result["welch"]["low"] <= 0
    assert all(row["qqq_return"] == -0.06 for row in result["hits"])


def test_too_few_stops_after_the_main_count() -> None:
    few = [row for row in _season() if row["filingDate"] < "2013-10-01"]
    result, rows = ea.read(_inputs(snapshot=_snapshot(filings=few)))
    assert result["decision"] == {**ea.judge(result["main"], None, None), "shift_note": None}
    for key in ("shuffle", "welch", "shifts", "years", "issuers", "hits", "sensitivity"):
        assert result[key] is None
    assert len(rows) == result["check"]["sets"]["top5"]["rows"]


def test_data_outside_the_window_never_matter(results: dict[str, Any]) -> None:
    later = pd.bdate_range("2026-10-13", "2026-12-31")
    daily = pd.DataFrame({ea.QQQ_COLUMN: pd.concat([_returns(), pd.Series(np.nan, index=later)])})
    daily.iloc[-3, 0] = np.inf
    daily.iloc[-2, 0] = -0.9
    assert ea.qqq_series(daily).equals(_returns())
    earlier = _returns().copy()
    earlier[earlier.index < FIRST] = -0.5
    late_filings = _season() + [
        _filing("MSFT", "2026-10-12 16:05"),
        _filing("AAPL", "2010-12-30 16:30"),
        _filing("NVDA", "2026-10-20 16:05"),
    ]
    result, rows = ea.read(_inputs(returns=earlier, snapshot=_snapshot(filings=late_filings)))
    base, base_rows = results["base"]
    assert rows == base_rows
    assert {k: v for k, v in result.items() if k != "check"} == {
        k: v for k, v in base.items() if k != "check"
    }
    assert result["check"]["filings"] == {
        "rows": len(late_filings),
        "results": len(late_filings) - 2,
    }


def test_the_qqq_series_is_cut_at_the_last_close_then_checked() -> None:
    frame = pd.DataFrame(
        {ea.QQQ_COLUMN: [0.01, np.nan, 0.02, 0.03]},
        index=pd.to_datetime(["2010-12-30", "2010-12-31", "2026-10-12", "2026-10-13"]),
    )
    assert ea.qqq_series(frame).to_dict() == {
        pd.Timestamp("2010-12-30"): 0.01,
        pd.Timestamp("2026-10-12"): 0.02,
    }
    with pytest.raises(ea.TableError, match="has no etf_reference_return column"):
        ea.qqq_series(frame.rename(columns={ea.QQQ_COLUMN: "qqq_proxy_return"}))
    twice = pd.DataFrame(
        {ea.QQQ_COLUMN: [0.01, 0.02]}, index=pd.to_datetime(["2016-12-30", "2016-12-30"])
    )
    with pytest.raises(ea.TableError, match="unique and increasing"):
        ea.qqq_series(twice)
    bad = pd.DataFrame(
        {ea.QQQ_COLUMN: [0.01, np.inf]}, index=pd.to_datetime(["2016-12-29", "2016-12-30"])
    )
    with pytest.raises(ea.TableError, match="not finite"):
        ea.qqq_series(bad)


# --- the report and the files --------------------------------------------------------------------


def test_report_sentences(results: dict[str, Any]) -> None:
    held = results["holds"][0]
    holds = ea.render(
        {
            **held,
            "decision": {**held["decision"], "shift_note": True},
            "provenance": {"commit": "abc123", "branch": "main"},
        }
    )
    assert "## 判定：holds" in holds and ea.HOLDS_SENTENCE + "。" in holds
    assert ea.HOLDS_NEXT in holds and ea.SHIFT_HOLDS in holds and ea.SHIFT_SENTENCE in holds
    for text in (
        E1,
        ea.QQQ_SNAPSHOT_ID,
        OD,
        "qqq_after_2010_owner_print",
        "读一次",
        "不进账本",
        "## E ∩ B 的每一天",
        "back_in_e",
        "前 5 的 13 家",
        "abc123",
        *ea.LIMITS,
    ):
        assert text in holds, text
    assert held["hits"] and all(hit["day"] in holds for hit in held["hits"])
    base = results["base"][0]
    plain = ea.render({**held, "decision": {**held["decision"], "shift_note": False}})
    assert ea.SHIFT_HOLDS not in plain and ea.SHIFT_SENTENCE not in plain
    not_holds = {
        **base,
        "decision": {**ea.judge(_main(c=1.2), {"p": 0.4}, base["welch"]), "shift_note": True},
    }
    text = ea.render(not_holds)
    assert "## 判定：not_holds" in text and "线 1：C ≥ 2；线 2：季内打乱 p ≤ 0.05" in text
    assert not_holds["decision"]["sentence"] + "。" in text
    assert ea.NOT_AGAIN in text and ea.NOT_HOLDS_NEXT in text and ea.SHIFT_SENTENCE in text
    assert ea.SHIFT_HOLDS not in text and ea.HOLDS_NEXT not in text
    few = [row for row in _season() if row["filingDate"] < "2013-10-01"]
    result, _ = ea.read(_inputs(snapshot=_snapshot(filings=few)))
    text = ea.render(result)
    assert "## 判定：too_few" in text and es.TOO_FEW_SENTENCE in text and "## 每年" not in text


def test_files(tmp_path: Path, results: dict[str, Any]) -> None:
    result, rows = results["base"]
    ea.write_check(result["check"], tmp_path / "c")
    assert (tmp_path / "c" / ea.CHECK).read_text(encoding="utf-8") == (
        json.dumps(result["check"], ensure_ascii=False, indent=1, sort_keys=True) + "\n"
    )
    ea.write_result(result, rows, tmp_path / "r")
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [ea.EVENTS, ea.SUMMARY, ea.REPORT]
    )
    lines = (tmp_path / "r" / ea.EVENTS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == rows
    assert json.loads((tmp_path / "r" / ea.SUMMARY).read_text(encoding="utf-8")) == result
    assert (tmp_path / "r" / ea.REPORT).read_text(encoding="utf-8") == ea.render(result)


def test_commands_read_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, results: dict[str, Any]
) -> None:
    monkeypatch.setattr(ea, "load_inputs", lambda: BASE)
    monkeypatch.setattr(ea, "git_provenance", lambda root: {"commit": "abc123", "branch": "main"})
    runner = CliRunner()
    with pytest.MonkeyPatch.context() as patch:
        _forbid(patch)
        out = runner.invoke(ea.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    check = results["base"][0]["check"]
    assert (
        f"earnings_shock_after_2010: W 3967 天；前 5 的事件 "
        f"{check['sets']['top5']['rows']} 个、{check['sets']['top5']['days']} 天；名单 16 年都对上"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [ea.CHECK]
    first = (tmp_path / "c" / ea.CHECK).read_bytes()
    again = runner.invoke(ea.app, ["check", "--out", str(tmp_path / "c")])
    assert again.exit_code == 1 and "拒绝覆盖" in again.output
    assert (tmp_path / "c" / ea.CHECK).read_bytes() == first
    out = runner.invoke(ea.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    result = results["base"][0]
    assert f"earnings_shock_after_2010: 判定 {result['decision']['verdict']}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [ea.CHECK, ea.EVENTS, ea.SUMMARY, ea.REPORT]
    )
    written = json.loads((tmp_path / "r" / ea.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**result, "provenance": {"commit": "abc123", "branch": "main"}}
    assert (tmp_path / "r" / ea.CHECK).read_bytes() == first
    # A second read is refused before anything is loaded, whichever of its files is there.
    monkeypatch.setattr(ea, "load_inputs", _forbidden)
    for name in (ea.CHECK, ea.EVENTS, ea.SUMMARY, ea.REPORT):
        directory = tmp_path / f"again-{name}"
        directory.mkdir()
        (directory / name).write_text("{}", encoding="utf-8")
        again = runner.invoke(ea.app, ["run", "--out", str(directory)])
        assert again.exit_code == 1 and "拒绝再读" in again.output, name
        assert sorted(p.name for p in directory.iterdir()) == [name]


def test_commands_print_only_a_summary_when_they_fail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def broken() -> ea.Inputs:
        raise ValueError("invalid literal for int() with base 10: '4321.8765'")

    monkeypatch.setattr(ea, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(ea.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "earnings_shock_after_2010: 数据载入失败：ValueError" in out.output
        assert "4321.8765" not in out.output and not (tmp_path / command).exists()

    def table_error() -> ea.Inputs:
        raise ea.TableError(f"earnings snapshot {E1}: unchecked\nsecond line")

    monkeypatch.setattr(ea, "load_inputs", table_error)
    out = CliRunner().invoke(ea.app, ["check", "--out", str(tmp_path / "t")])
    assert out.exit_code == 1 and f"数据载入失败：earnings snapshot {E1}: unchecked" in out.output
    monkeypatch.setattr(ea, "load_inputs", lambda: BASE)
    monkeypatch.setattr(ea, "results", lambda filings: float("4321.87x"))
    for command in ("check", "run"):
        out = CliRunner().invoke(ea.app, [command, "--out", str(tmp_path / f"b{command}")])
        assert out.exit_code == 1
        assert "earnings_shock_after_2010: 建数据失败：ValueError" in out.output
        assert "4321.87x" not in out.output and not (tmp_path / f"b{command}").exists()


# --- loading -------------------------------------------------------------------------------------


def _daily() -> pd.DataFrame:
    later = pd.bdate_range("2026-10-13", "2026-10-30")
    return pd.DataFrame(
        {ea.QQQ_COLUMN: pd.concat([_returns(), pd.Series(0.5, index=later)]), "vix": 20.0}
    )


def _loaders(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    snapshot: SimpleNamespace | None = None,
    daily: pd.DataFrame | None = None,
) -> list[tuple[str, Any]]:
    asked: list[tuple[str, Any]] = []

    def load_spec(path: Path) -> Any:
        asked.append(("spec", path))
        return SPEC

    def load_snapshot(root: Path, snapshot_id: str) -> SimpleNamespace:
        asked.append(("ern", (root, snapshot_id)))
        return _snapshot() if snapshot is None else snapshot

    def resolve(root: Path, snapshot_id: str | None = None) -> Path:
        asked.append(("dir", (root, snapshot_id)))
        return tmp_path / str(snapshot_id)

    def load_daily(directory: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        asked.append(("daily", directory))
        frame = _daily() if daily is None else daily
        return frame, {"status": "WARN", "series_csv_sha256": "f" * 64, "snapshot_id": "ignored"}

    monkeypatch.setattr(ee, "load_spec", load_spec)
    monkeypatch.setattr(ee, "load_snapshot", load_snapshot)
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", resolve)
    monkeypatch.setattr(qlh, "load_daily", load_daily)
    return asked


def test_load_inputs_reads_the_registered_window_and_the_two_pinned_snapshots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    asked = _loaders(monkeypatch, tmp_path)
    inputs = ea.load_inputs(tmp_path / "root", tmp_path / "qlh")
    assert asked == [
        ("spec", ea.SPEC_PATH),
        ("ern", (tmp_path / "root", E1)),
        ("dir", (tmp_path / "qlh", "qlh-20261013-0b0df53cb1a2")),
        ("daily", tmp_path / "qlh-20261013-0b0df53cb1a2"),
    ]
    assert inputs.window_read == READ == ea.window_read()
    assert inputs.qqq.equals(_returns())
    assert inputs.sources == {
        "earnings": {"snapshot_id": E1, "filings": len(_season()), "prices": 0, "documents": 16},
        "qqq": {
            "snapshot_id": "qlh-20261013-0b0df53cb1a2",
            "check_status": "WARN",
            "series_csv_sha256": "f" * 64,
            "first": "2010-09-01",
            "last": "2026-10-12",
            "days": len(CAL),
        },
    }


def test_load_inputs_stops_on_a_changed_window_registry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _loaders(monkeypatch, tmp_path)

    def changed(loaded: Any) -> dict[str, Any]:
        raise ValueError(
            "窗口 qqq_after_2010_owner_print 的登记和模块不一致：('2011-01-03',)\nmore"
        )

    monkeypatch.setattr(ssp, "registered_qqq_window", changed)
    with pytest.raises(ea.TableError) as caught:
        ea.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == (
        "window qqq_after_2010_owner_print: 窗口 qqq_after_2010_owner_print 的登记和模块不一致："
        "('2011-01-03',)"
    )


def test_load_inputs_stops_on_a_wrong_config_snapshot_or_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _loaders(monkeypatch, tmp_path)
    monkeypatch.setattr(ee, "load_spec", lambda path: SPEC.model_copy(update={"foreign": ()}))
    with pytest.raises(ea.TableError, match="foreign issuers"):
        ea.load_inputs(tmp_path, tmp_path)
    monkeypatch.setattr(ee, "load_spec", lambda path: SPEC)
    late = SimpleNamespace(
        **{
            **vars(_snapshot()),
            "spec": SPEC.model_copy(update={"data_end": LAST.date().replace(day=13)}),
        }
    )
    _loaders(monkeypatch, tmp_path, snapshot=late)
    with pytest.raises(ea.TableError) as caught:
        ea.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == f"earnings snapshot {E1}: the configured window is not the module's"
    priced = SimpleNamespace(
        **{
            **vars(_snapshot()),
            "prices": pd.DataFrame({"ticker": ["AAPL"], "date": [W[0]], "adj_close": [1.0]}),
        }
    )
    _loaders(monkeypatch, tmp_path, snapshot=priced)
    with pytest.raises(ea.TableError, match="has stock prices"):
        ea.load_inputs(tmp_path, tmp_path)

    def refused(root: Path, snapshot_id: str) -> None:
        raise ValueError("earnings snapshot is unchecked or failed\nmore")

    _loaders(monkeypatch, tmp_path)
    monkeypatch.setattr(ee, "load_snapshot", refused)
    with pytest.raises(ea.TableError) as caught:
        ea.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == f"earnings snapshot {E1}: earnings snapshot is unchecked or failed"
    _loaders(monkeypatch, tmp_path)

    def missing(root: Path, snapshot_id: str | None = None) -> Path:
        raise FileNotFoundError("snapshot qlh-20261013-0b0df53cb1a2 has no manifest")

    monkeypatch.setattr(qlh, "resolve_snapshot_dir", missing)
    with pytest.raises(ea.TableError) as caught:
        ea.load_inputs(tmp_path, tmp_path)
    assert str(caught.value) == (
        "QQQ snapshot qlh-20261013-0b0df53cb1a2: snapshot qlh-20261013-0b0df53cb1a2 has no manifest"
    )
    short = _daily().drop(index=[W[100]])
    missing_last = _daily().drop(index=[LAST])
    # The same count of days, one weekend day in place of the first.
    moved = _daily().drop(index=[FIRST])
    moved.loc[pd.Timestamp("2015-01-03")] = 0.001
    moved = moved.sort_index()
    for daily in (short, missing_last, _daily().drop(index=[FIRST]), moved):
        _loaders(monkeypatch, tmp_path, daily=daily)
        with pytest.raises(ea.TableError, match="the window is not the 3,967 pinned days"):
            ea.load_inputs(tmp_path, tmp_path)
