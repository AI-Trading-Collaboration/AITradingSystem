"""Invariants of the earnings event intake (TRADING-2565-98).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-30:earnings_shock_table``: Baker chose to make the period 6
earnings-shock print-only table now, and its card said the 8-K data intake takes his approval once).
Every value below is synthetic; no test reads a real snapshot or downloads anything.

71. The intake stays inside QQQ's development window. A spec cannot end after 2010-12-31, and a
    snapshot holds only 8-K and 8-K/A rows filed from filing_start to data_end, prices from
    price_start to data_end and the QQQ trust's holdings documents byte for byte. Later rows,
    other forms and submission pages outside the window never reach a file, and no price request
    asks for a day after data_end.
72. A snapshot is trusted only as written. Every CIK must name its issuer (by its name or a former
    name) and every request must work, or the fetch writes nothing. The id is the content address
    of the manifest. The check fails a changed byte, a filing outside the window, a repeated
    accession, another form, a results 8-K whose acceptance time is not a UTC time, a missing
    holdings document or one without its markers, and a price that is not positive, repeated,
    outside the window, of an unknown ticker or missing for a ticker; it warns on a price gap.
    load_snapshot refuses an unchecked or failed snapshot and a changed file. Results 8-Ks are
    counted by their Eastern hour. Reports and printed lines hold no data value.
73. The fetch command sends SEC_USER_AGENT with every SEC request after a pause and never stores
    or prints it, and refuses to run without it. The settings file names the seventeen issuers,
    the two foreign ones and the eight holdings documents.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.data.market_data import PriceRequest
from ai_trading_system.research_data import earnings_events as ee

SUBMISSIONS = "https://data.sec.gov/submissions/"
ARCHIVES = "https://www.sec.gov/Archives/edgar/data/1067839/"
LEAKED_PRICE = 4321.8765
COLUMNS = (
    "accessionNumber",
    "filingDate",
    "form",
    "acceptanceDateTime",
    "items",
    "primaryDocument",
)


def _spec(**changes: Any) -> ee.EarningsSpec:
    raw: dict[str, Any] = {
        "schema_version": "research_earnings.v1",
        "filing_start": "2003-01-01",
        "data_end": "2010-12-31",
        "price_start": "2003-09-01",
        "issuers": [
            {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha corp", "fmp_ticker": "ALPH"},
            {"id": "BETA", "ciks": [222], "entity_pattern": "^old beta", "fmp_ticker": None},
        ],
        "foreign": [{"id": "FRGN", "foreign": True}],
        "holdings_documents": [
            {
                "fiscal_year": 2009,
                "filing_date": "2010-01-15",
                "accession": "0001104659-10-001821",
                "document": "a09-31668_2n30b2.htm",
                "form": "N-30B-2",
            },
            {
                "fiscal_year": 2010,
                "filing_date": "2010-12-30",
                "accession": "0001104659-10-064790",
                "document": "a10-18275_1n30b2.txt",
                "form": "N-30B-2",
            },
        ],
    }
    raw.update(changes)
    return ee.EarningsSpec.model_validate(raw)


def _columns(rows: list[tuple[str, ...]]) -> dict[str, list[str]]:
    return {name: [row[i] for row in rows] for i, name in enumerate(COLUMNS)}


ALPHA_RECENT = [
    ("0000000111-11-000001", "2011-02-01", "8-K", "2011-02-01T21:05:00.000Z", "2.02,9.01", "a.htm"),
    ("0000000111-10-000009", "2010-12-30", "8-K", "2010-12-30T21:05:00.000Z", "2.02,9.01", "b.htm"),
    ("0000000111-10-000008", "2010-11-01", "10-Q", "2010-11-01T20:00:00.000Z", "", "c.htm"),
    ("0000000111-09-000007", "2009-07-20", "8-K/A", "2009-07-20T13:00:00.000Z", "2.02", "d.htm"),
    (
        "0000000111-09-000006",
        "2009-07-16",
        "8-K",
        "2009-07-16T20:10:00.000Z",
        "2.02, 9.01",
        "e.htm",
    ),
]
ALPHA_PAGE = [
    ("0000000111-08-000005", "2008-10-20", "8-K", "2008-10-20T12:00:00.000Z", "7.01", "f.htm"),
    ("0000000111-04-000004", "2004-04-15", "8-K", "2004-04-15T11:30:00.000Z", "12", "g.htm"),
    ("0000000111-02-000003", "2002-12-01", "8-K", "2002-12-01T21:00:00.000Z", "12", "h.htm"),
]
ALPHA_OLD_PAGE = [
    ("0000000111-01-000002", "2001-04-15", "8-K", "2001-04-15T21:00:00.000Z", "5", "i.htm"),
]
BETA_RECENT = [
    ("0000000222-05-000001", "2005-01-20", "8-K", "2005-01-20T12:00:00Z", "2.02", "j.htm"),
]
KEPT = {
    "0000000111-10-000009",
    "0000000111-09-000007",
    "0000000111-09-000006",
    "0000000111-08-000005",
    "0000000111-04-000004",
    "0000000222-05-000001",
}
HTML_DOCUMENT = (
    b"<html><body><p>SCHEDULE&nbsp;OF <b>INVESTMENTS</b></p>"
    b"<p>September&#160;30,<br>\n2009</p><table>"
    b"<tr><td>Alpha Corp</td><td>1,000,000</td><td>50,000,000</td></tr>"
    b"<tr><td>Total Investments</td><td>60,000,000</td></tr></table></body></html>\r\n"
)
TEXT_DOCUMENT = (
    b"Schedule of Investments\r\nSeptember 30,\r\n   2010\r\n"
    b"Alpha Corp\x92s shares ........ 1,000,000   50,000,000\r\nTotal Investments  60,000,000\r\n"
)


def _sec_pages(**changes: bytes) -> dict[str, bytes]:
    alpha = {
        "cik": "0000000111",
        "name": "ALPHA CORP",
        "formerNames": [],
        "filings": {
            "recent": _columns(ALPHA_RECENT),
            "files": [
                {
                    "name": "CIK0000000111-submissions-001.json",
                    "filingFrom": "2002-06-01",
                    "filingTo": "2008-12-31",
                },
                {
                    "name": "CIK0000000111-submissions-002.json",
                    "filingFrom": "1996-01-01",
                    "filingTo": "2001-12-31",
                },
            ],
        },
    }
    beta = {
        "cik": "0000000222",
        "name": "BETA HOLDINGS INC",
        "formerNames": [
            {"name": "OLD BETA COMMUNICATIONS", "from": "1999-01-01", "to": "2009-01-01"}
        ],
        "filings": {"recent": _columns(BETA_RECENT), "files": []},
    }
    pages = {
        f"{SUBMISSIONS}CIK0000000111.json": json.dumps(alpha).encode(),
        f"{SUBMISSIONS}CIK0000000111-submissions-001.json": json.dumps(
            _columns(ALPHA_PAGE)
        ).encode(),
        f"{SUBMISSIONS}CIK0000000111-submissions-002.json": json.dumps(
            _columns(ALPHA_OLD_PAGE)
        ).encode(),
        f"{SUBMISSIONS}CIK0000000222.json": json.dumps(beta).encode(),
        f"{ARCHIVES}000110465910001821/a09-31668_2n30b2.htm": HTML_DOCUMENT,
        f"{ARCHIVES}000110465910064790/a10-18275_1n30b2.txt": TEXT_DOCUMENT,
    }
    pages.update(changes)
    return pages


class FakeSec:
    def __init__(self, pages: dict[str, bytes], fail: str | None = None) -> None:
        self.pages = pages
        self.fail = fail
        self.calls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if url == self.fail or url not in self.pages:
            raise RuntimeError("HTTP 404")
        return self.pages[url]


def _price(day: pd.Timestamp) -> float:
    return (
        LEAKED_PRICE
        if day == pd.Timestamp("2006-06-15")
        else 50.0 + (day - pd.Timestamp("2003-01-01")).days / 100
    )


class FakePrices:
    """Serves ten days on either side of each request, as a careless vendor might."""

    def __init__(self, drop: tuple[str, str] | None = None, price: Any = _price) -> None:
        self.requests: list[PriceRequest] = []
        self.drop = drop
        self.price = price
        self.fail = False

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("FMP down")
        days = pd.bdate_range(
            pd.Timestamp(request.start) - pd.Timedelta(days=10),
            pd.Timestamp(request.end) + pd.Timedelta(days=10),
        )
        if self.drop is not None:
            days = days[(days < pd.Timestamp(self.drop[0])) | (days > pd.Timestamp(self.drop[1]))]
        return pd.DataFrame(
            {
                "ticker": request.tickers[0],
                "date": days.strftime("%Y-%m-%d"),
                "adj_close": [self.price(day) for day in days],
            }
        )


def _directory(root: Path, snapshot: str) -> Path:
    return root / "data" / "raw" / "research" / "earnings_events" / snapshot


def _fetch(
    root: Path,
    spec: ee.EarningsSpec | None = None,
    sec: FakeSec | None = None,
    prices: FakePrices | None = None,
) -> str:
    return ee.fetch_snapshot(
        spec or _spec(),
        root,
        sec_get=sec or FakeSec(_sec_pages()),
        price_provider=prices or FakePrices(),
    )


def _files(root: Path) -> list[Path]:
    return [path for path in root.rglob("*") if path.is_file()]


def _rewrite(root: Path, snapshot: str, name: str, payload: bytes | None) -> str:
    """A self-consistent snapshot whose file ``name`` is ``payload`` (None drops it): the check
    must judge the content, not only the checksums."""

    directory = _directory(root, snapshot)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    entries = []
    for entry in manifest["files"]:
        if entry["name"] != name:
            entries.append(entry)
        elif payload is not None:
            changed = {
                **entry,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
            }
            if "rows" in entry:
                changed["rows"] = payload.count(b"\n") - 1
            entries.append(changed)
    manifest["files"] = entries
    body = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    new_id = "ern-" + hashlib.sha256(body).hexdigest()[:12]
    target = _directory(root, new_id)
    target.mkdir(parents=True)
    (target / "holdings").mkdir()
    for entry in entries:
        source = payload if entry["name"] == name else (directory / entry["name"]).read_bytes()
        (target / entry["name"]).write_bytes(source)
    (target / "manifest.json").write_bytes(body + b"\n")
    return new_id


# 71 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "changes",
    [
        {"data_end": "2011-01-03"},
        {"data_end": "2021-12-31", "price_start": "2003-09-01"},
        {"price_start": "2011-01-03"},
        {"filing_start": "2004-01-01", "price_start": "2003-09-01"},
        {
            "holdings_documents": [
                {
                    "fiscal_year": 2010,
                    "filing_date": "2011-01-28",
                    "accession": "0001104659-11-000001",
                    "document": "x.htm",
                    "form": "485BPOS",
                }
            ]
        },
        {
            "holdings_documents": [
                {
                    "fiscal_year": 2010,
                    "filing_date": "2010-09-30",
                    "accession": "0001104659-10-000001",
                    "document": "x.htm",
                    "form": "N-30B-2",
                }
            ]
        },
        {
            "holdings_documents": [
                {
                    "fiscal_year": 2009,
                    "filing_date": "2010-01-15",
                    "accession": "0001104659-10-001821",
                    "document": "../x.htm",
                    "form": "N-30B-2",
                }
            ]
        },
        {
            "issuers": [
                {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha"},
                {"id": "BETA", "ciks": [111], "entity_pattern": "^beta"},
            ]
        },
        {
            "issuers": [
                {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha"},
                {"id": "ALPH", "ciks": [222], "entity_pattern": "^beta"},
            ]
        },
        {
            "issuers": [
                {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha", "fmp_ticker": "AAA"},
                {"id": "BETA", "ciks": [222], "entity_pattern": "^beta", "fmp_ticker": "AAA"},
            ]
        },
        {"foreign": [{"id": "ALPH", "foreign": True}]},
        {"surprise": 1},
    ],
)
def test_a_spec_stays_inside_the_development_window(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _spec(**changes)


def test_the_snapshot_keeps_the_window_only(tmp_path: Path) -> None:
    sec = FakeSec(_sec_pages())
    prices = FakePrices()
    snapshot = _fetch(tmp_path, sec=sec, prices=prices)
    assert ee.check_snapshot(tmp_path, snapshot)["status"] == "PASS"
    loaded = ee.load_snapshot(tmp_path, snapshot)
    assert set(loaded.filings["accessionNumber"]) == KEPT
    row = loaded.filings.set_index("accessionNumber").loc["0000000111-09-000006"]
    assert (int(row["cik"]), row["form"], row["filingDate"]) == (111, "8-K", "2009-07-16")
    assert (row["acceptanceDateTime"], row["items"]) == ("2009-07-16T20:10:00.000Z", "2.02, 9.01")
    assert set(loaded.filings["form"]) == {"8-K", "8-K/A"}
    assert loaded.documents[2009] == ("a09-31668_2n30b2.htm", HTML_DOCUMENT)
    assert loaded.documents[2010] == ("a10-18275_1n30b2.txt", TEXT_DOCUMENT)
    assert f"{SUBMISSIONS}CIK0000000111-submissions-002.json" not in sec.calls
    assert f"{SUBMISSIONS}CIK0000000111-submissions-001.json" in sec.calls
    assert {request.tickers[0] for request in prices.requests} == {"ALPH"}
    assert min(request.start for request in prices.requests) == date(2003, 9, 1)
    assert max(request.end for request in prices.requests) == date(2010, 12, 31)
    days = pd.DatetimeIndex(loaded.prices["date"])
    expected = pd.bdate_range("2003-09-01", "2010-12-31")
    assert days.equals(expected)
    assert set(loaded.prices["ticker"]) == {"ALPH"}
    assert loaded.prices["adj_close"].tolist() == [_price(day) for day in expected]
    for path in _files(tmp_path):
        payload = path.read_bytes()
        for late in (
            b"0000000111-11-000001",
            b"2011-02-01",
            b"2011-01-0",
            b"0000000111-02-000003",
            b"0000000111-10-000008",
        ):
            assert late not in payload, (path.name, late)


def test_an_empty_window_is_an_empty_list_not_an_error(tmp_path: Path) -> None:
    pages = _sec_pages()
    beta = json.loads(pages[f"{SUBMISSIONS}CIK0000000222.json"])
    beta["filings"]["recent"] = _columns(
        [("0000000222-11-000002", "2011-03-01", "8-K", "2011-03-01T21:00:00.000Z", "2.02", "k.htm")]
    )
    pages[f"{SUBMISSIONS}CIK0000000222.json"] = json.dumps(beta).encode()
    snapshot = _fetch(tmp_path, sec=FakeSec(pages))
    assert ee.check_snapshot(tmp_path, snapshot)["status"] == "PASS"
    loaded = ee.load_snapshot(tmp_path, snapshot)
    assert 222 not in set(loaded.filings["cik"])
    assert [entity["cik"] for entity in loaded.entities] == [111, 222]


# 72 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [
        "a CIK that names another company",
        "a failed submissions page",
        "a failed holdings document",
        "a failed price request",
    ],
)
def test_a_fetch_writes_nothing_unless_everything_worked(tmp_path: Path, case: str) -> None:
    pages = _sec_pages()
    sec = FakeSec(pages)
    prices = FakePrices()
    if case == "a CIK that names another company":
        beta = json.loads(pages[f"{SUBMISSIONS}CIK0000000222.json"])
        beta["formerNames"] = [{"name": "GAMMA SYSTEMS"}]
        pages[f"{SUBMISSIONS}CIK0000000222.json"] = json.dumps(beta).encode()
    elif case == "a failed submissions page":
        sec.fail = f"{SUBMISSIONS}CIK0000000111-submissions-001.json"
    elif case == "a failed holdings document":
        sec.fail = f"{ARCHIVES}000110465910064790/a10-18275_1n30b2.txt"
    else:
        prices.fail = True
    with pytest.raises((RuntimeError, ValueError)):
        _fetch(tmp_path, sec=sec, prices=prices)
    assert _files(tmp_path) == []


def test_the_snapshot_id_is_the_content_address(tmp_path: Path) -> None:
    first = _fetch(tmp_path / "a")
    second = _fetch(tmp_path / "b")
    assert first == second
    body = (_directory(tmp_path / "a", first) / "manifest.json").read_bytes()
    assert body.endswith(b"\n")
    assert first == "ern-" + hashlib.sha256(body[:-1]).hexdigest()[:12]
    assert (tmp_path / "a" / "data" / "raw" / "research" / "earnings_events" / "LATEST").read_text(
        "utf-8"
    ).strip() == first
    changed_rows = [(*ALPHA_RECENT[1][:3], "2010-12-30T21:06:00.000Z", *ALPHA_RECENT[1][4:])]
    pages = _sec_pages()
    alpha = json.loads(pages[f"{SUBMISSIONS}CIK0000000111.json"])
    alpha["filings"]["recent"] = _columns([ALPHA_RECENT[0], *changed_rows, *ALPHA_RECENT[2:]])
    pages[f"{SUBMISSIONS}CIK0000000111.json"] = json.dumps(alpha).encode()
    assert _fetch(tmp_path / "c", sec=FakeSec(pages)) != first
    changed_document = _sec_pages(
        **{f"{ARCHIVES}000110465910064790/a10-18275_1n30b2.txt": TEXT_DOCUMENT + b" "}
    )
    assert _fetch(tmp_path / "d", sec=FakeSec(changed_document)) != first
    shifted = FakePrices(
        price=lambda day: _price(day) + (0.001 if day == pd.Timestamp("2008-01-02") else 0.0)
    )
    assert _fetch(tmp_path / "e", prices=shifted) != first


def test_only_a_checked_unchanged_snapshot_loads(tmp_path: Path) -> None:
    snapshot = _fetch(tmp_path)
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, snapshot)
    assert ee.check_snapshot(tmp_path, snapshot)["status"] == "PASS"
    ee.load_snapshot(tmp_path, snapshot)
    document = _directory(tmp_path, snapshot) / "holdings" / "2010.txt"
    original = document.read_bytes()
    document.write_bytes(original.replace(b"60,000,000", b"60,000,001"))
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, snapshot)
    assert ee.check_snapshot(tmp_path, snapshot)["status"] == "FAIL"
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, snapshot)
    document.write_bytes(original)
    assert ee.check_snapshot(tmp_path, snapshot)["status"] == "PASS"
    ee.load_snapshot(tmp_path, snapshot)
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, "ern-" + "0" * 12)
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, "../" + snapshot)


def _filings_with(rows: list[dict[str, str]], base: bytes) -> bytes:
    frame = pd.read_csv(pd.io.common.BytesIO(base), dtype=str, keep_default_na=False)
    frame = pd.concat([frame, pd.DataFrame(rows)], ignore_index=True)
    return frame.to_csv(index=False, lineterminator="\n").encode()


def _prices_with(edit: Any, base: bytes) -> bytes:
    frame = pd.read_csv(pd.io.common.BytesIO(base), dtype={"ticker": str, "date": str})
    return edit(frame).to_csv(index=False, lineterminator="\n").encode()


ROW = {
    "cik": "111",
    "form": "8-K",
    "items": "2.02",
    "acceptanceDateTime": "2006-01-20T21:00:00.000Z",
}
FILING_CASES: dict[str, list[dict[str, str]]] = {
    "a filing after data_end": [
        {**ROW, "accessionNumber": "0000000111-11-000099", "filingDate": "2011-01-05"}
    ],
    "a filing before filing_start": [
        {**ROW, "accessionNumber": "0000000111-02-000099", "filingDate": "2002-12-31"}
    ],
    "a repeated accession": [
        {**ROW, "accessionNumber": "0000000111-10-000009", "filingDate": "2006-01-20"}
    ],
    "another form": [
        {
            **ROW,
            "accessionNumber": "0000000111-06-000099",
            "filingDate": "2006-01-20",
            "form": "10-Q",
        }
    ],
    "a results 8-K without a time": [
        {
            **ROW,
            "accessionNumber": "0000000111-06-000099",
            "filingDate": "2006-01-20",
            "acceptanceDateTime": "",
        }
    ],
    "a results 8-K with a local time": [
        {
            **ROW,
            "accessionNumber": "0000000111-06-000099",
            "filingDate": "2006-01-20",
            "acceptanceDateTime": "2006-01-20 16:00:00",
        }
    ],
    "an Item 12 8-K with an offset time": [
        {
            **ROW,
            "accessionNumber": "0000000111-06-000099",
            "filingDate": "2006-01-20",
            "items": "12",
            "acceptanceDateTime": "2006-01-20T16:00:00-05:00",
        }
    ],
    "an unknown CIK": [
        {**ROW, "cik": "999", "accessionNumber": "0000000999-06-000099", "filingDate": "2006-01-20"}
    ],
    "a CIK that is not a number": [
        {
            **ROW,
            "cik": "4321.8765",
            "accessionNumber": "0000000111-06-000099",
            "filingDate": "2006-01-20",
        }
    ],
}
PRICE_CASES: dict[str, Any] = {
    "a zero price": lambda f: f.assign(
        adj_close=f["adj_close"].where(f["date"] != "2006-06-16", 0.0)
    ),
    "a missing price value": lambda f: f.assign(
        adj_close=f["adj_close"].where(f["date"] != "2006-06-16")
    ),
    "a repeated day": lambda f: pd.concat([f, f.iloc[[100]]], ignore_index=True),
    "a day after data_end": lambda f: pd.concat(
        [f, pd.DataFrame({"ticker": ["ALPH"], "date": ["2011-01-03"], "adj_close": [99.0]})],
        ignore_index=True,
    ),
    "a day before price_start": lambda f: pd.concat(
        [pd.DataFrame({"ticker": ["ALPH"], "date": ["2003-08-29"], "adj_close": [99.0]}), f],
        ignore_index=True,
    ),
    "a ticker that is not configured": lambda f: pd.concat(
        [f, f.iloc[:3].assign(ticker="ZZZ")], ignore_index=True
    ),
    "a configured ticker without prices": lambda f: f.iloc[0:0],
    "a price that is not a number": lambda f: f.assign(
        adj_close=f["adj_close"].astype(object).where(f["date"] != "2006-06-16", "4321.87x")
    ),
    "a day that is not a date": lambda f: f.assign(
        date=f["date"].where(f["date"] != "2006-06-16", "4321.8765")
    ),
}


@pytest.mark.parametrize(
    "case",
    sorted(FILING_CASES)
    + sorted(PRICE_CASES)
    + [
        "a missing holdings document",
        "a holdings document without its year",
        "a holdings document without its total",
    ],
)
def test_the_check_fails_bad_content(
    tmp_path: Path, case: str, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshot = _fetch(tmp_path)
    directory = _directory(tmp_path, snapshot)
    if case in FILING_CASES:
        bad = _rewrite(
            tmp_path,
            snapshot,
            "filings.csv",
            _filings_with(FILING_CASES[case], (directory / "filings.csv").read_bytes()),
        )
    elif case in PRICE_CASES:
        bad = _rewrite(
            tmp_path,
            snapshot,
            "prices.csv",
            _prices_with(PRICE_CASES[case], (directory / "prices.csv").read_bytes()),
        )
    elif case == "a missing holdings document":
        bad = _rewrite(tmp_path, snapshot, "holdings/2009.htm", None)
    elif case == "a holdings document without its year":
        bad = _rewrite(
            tmp_path, snapshot, "holdings/2010.txt", TEXT_DOCUMENT.replace(b"2010", b"2011")
        )
    else:
        bad = _rewrite(
            tmp_path,
            snapshot,
            "holdings/2009.htm",
            HTML_DOCUMENT.replace(b"Total Investments", b"Total"),
        )
    report = ee.check_snapshot(tmp_path, bad)
    assert report["status"] == "FAIL"
    with pytest.raises(ValueError):
        ee.load_snapshot(tmp_path, bad)
    outcome = CliRunner().invoke(ee.app, ["check", "--snapshot", bad, "--root", str(tmp_path)])
    assert outcome.exit_code == 1
    texts = [
        capsys.readouterr().out,
        outcome.output,
        (_directory(tmp_path, bad) / "check.md").read_text("utf-8"),
        (_directory(tmp_path, bad) / "check.json").read_text("utf-8"),
    ]
    for text in texts:
        for value in ("4321.87", "99.0", "16:00:00", "21:00:00", "Alpha Corp", "60,000,000"):
            assert value not in text, (case, value)


def test_the_check_counts_results_filings_by_their_eastern_hour(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshot = _fetch(tmp_path)
    report = ee.check_snapshot(tmp_path, snapshot)
    assert report["status"] == "PASS"
    filings = report["filings"]
    # 2010-12-30 21:05Z is 16:05 EST, 2009-07-16 20:10Z is 16:10 EDT, 2004-04-15 11:30Z is 07:30
    # EDT, 2005-01-20 12:00Z is 07:00 EST; the 8-K/A and the Item 7.01 8-K are not results 8-Ks.
    assert filings["results_filings"] == 4
    assert {int(hour): count for hour, count in filings["results_eastern_hours"].items()} == {
        7: 2,
        16: 2,
    }
    years = filings["per_issuer_year"]
    assert years["ALPH"]["2009"] == {"8-K": 1, "8-K/A": 1, "2.02": 1, "12": 0}
    assert years["ALPH"]["2004"] == {"8-K": 1, "8-K/A": 0, "2.02": 0, "12": 1}
    assert years["ALPH"]["2008"] == {"8-K": 1, "8-K/A": 0, "2.02": 0, "12": 0}
    assert years["BETA"]["2005"] == {"8-K": 1, "8-K/A": 0, "2.02": 1, "12": 0}
    assert report["holdings"]["2009"]["bytes"] == len(HTML_DOCUMENT)
    assert report["prices"]["tickers"]["ALPH"]["first_date"] == "2003-09-01"
    assert report["prices"]["tickers"]["ALPH"]["last_date"] == "2010-12-31"
    printed = capsys.readouterr().out
    texts = [printed, (_directory(tmp_path, snapshot) / "check.md").read_text("utf-8")]
    for text in texts:
        for value in (
            "4321.87",
            "21:05",
            "20:10",
            "Alpha Corp",
            "60,000,000",
            "0000000111-10-000009",
        ):
            assert value not in text, value


def test_results_filings_are_8k_with_item_2_02_or_12() -> None:
    assert ee.is_results_filing("8-K", "2.02")
    assert ee.is_results_filing("8-K", "9.01, 2.02")
    assert ee.is_results_filing("8-K", "12,9")
    assert not ee.is_results_filing("8-K/A", "2.02")
    assert not ee.is_results_filing("8-K", "2.021")
    assert not ee.is_results_filing("8-K", "7.01,9.01")
    assert not ee.is_results_filing("8-K", "")
    assert ee.parse_acceptance("2004-01-15T21:05:12.000Z") == pd.Timestamp(
        "2004-01-15 21:05:12", tz="UTC"
    )
    assert ee.parse_acceptance("2004-01-15T21:05:12Z") == pd.Timestamp(
        "2004-01-15 21:05:12", tz="UTC"
    )
    for bad in (
        "",
        "2004-01-15 21:05:12",
        "2004-01-15T21:05:12-05:00",
        "2004-01-15",
        "2004-13-15T21:05:12Z",
    ):
        with pytest.raises(ValueError):
            ee.parse_acceptance(bad)


def test_a_price_gap_warns(tmp_path: Path) -> None:
    snapshot = _fetch(tmp_path, prices=FakePrices(drop=("2007-03-01", "2007-03-12")))
    report = ee.check_snapshot(tmp_path, snapshot)
    assert report["status"] == "WARN"
    assert report["prices"]["tickers"]["ALPH"]["gaps"] == 1
    ee.load_snapshot(tmp_path, snapshot)
    snapshot = _fetch(tmp_path / "short", prices=FakePrices(drop=("2007-03-01", "2007-03-07")))
    assert ee.check_snapshot(tmp_path / "short", snapshot)["status"] == "PASS"


# 73 ----------------------------------------------------------------------------------------------


AGENT = "Sentinel Research sentinel-agent@example.invalid"


class FakeResponse:
    def __init__(self, url: str, pages: dict[str, bytes]) -> None:
        self.status_code = 200 if url in pages else 404
        self.content = pages.get(url, b"")

    def raise_for_status(self) -> None:
        if self.status_code != 200:
            raise RuntimeError(f"HTTP {self.status_code}")


def _patch_network(
    monkeypatch: pytest.MonkeyPatch, pages: dict[str, bytes]
) -> dict[str, list[Any]]:
    seen: dict[str, list[Any]] = {"get": [], "sleep": []}

    def get(url: str, *args: Any, **kwargs: Any) -> FakeResponse:
        seen["get"].append((url, dict(kwargs.get("headers") or {}), kwargs.get("timeout")))
        return FakeResponse(url, pages)

    monkeypatch.setattr(ee.requests, "get", get)
    monkeypatch.setattr(ee.time, "sleep", lambda seconds: seen["sleep"].append(seconds))
    monkeypatch.setattr(ee, "load_spec", lambda *args, **kwargs: _spec())
    monkeypatch.setattr(ee, "price_provider_factory", FakePrices)
    return seen


def test_the_fetch_command_sends_the_user_agent_and_never_keeps_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _patch_network(monkeypatch, _sec_pages())
    monkeypatch.setenv("SEC_USER_AGENT", AGENT)
    outcome = CliRunner().invoke(ee.app, ["fetch", "--root", str(tmp_path)])
    assert outcome.exit_code == 0, outcome.output
    urls = [url for url, _, _ in seen["get"]]
    assert sorted(urls) == sorted(set(urls))
    assert f"{SUBMISSIONS}CIK0000000111-submissions-002.json" not in urls
    assert len(urls) == 5
    assert all(headers.get("User-Agent") == AGENT for _, headers, _ in seen["get"])
    assert all(timeout is not None for _, _, timeout in seen["get"])
    assert seen["sleep"] == [ee.SEC_PAUSE_SECONDS] * len(urls)
    assert ee.SEC_PAUSE_SECONDS >= 0.1
    assert AGENT not in outcome.output and "sentinel" not in outcome.output.lower()
    files = _files(tmp_path)
    assert any(path.name == "manifest.json" for path in files)
    for path in files:
        assert b"sentinel" not in path.read_bytes().lower(), path.name


def test_the_fetch_command_needs_the_user_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen = _patch_network(monkeypatch, _sec_pages())
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    outcome = CliRunner().invoke(ee.app, ["fetch", "--root", str(tmp_path)])
    assert outcome.exit_code == 1
    assert seen["get"] == []
    assert _files(tmp_path) == []
    monkeypatch.setenv("SEC_USER_AGENT", "   ")
    assert CliRunner().invoke(ee.app, ["fetch", "--root", str(tmp_path)]).exit_code == 1
    assert _files(tmp_path) == []


def test_a_failed_request_in_the_command_names_no_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pages = _sec_pages()
    del pages[f"{ARCHIVES}000110465910001821/a09-31668_2n30b2.htm"]
    _patch_network(monkeypatch, pages)
    monkeypatch.setenv("SEC_USER_AGENT", AGENT)
    outcome = CliRunner().invoke(ee.app, ["fetch", "--root", str(tmp_path)])
    assert outcome.exit_code == 1
    assert AGENT not in outcome.output and "sentinel" not in outcome.output.lower()
    assert _files(tmp_path) == []


def test_the_settings_file_names_the_issuers_and_documents() -> None:
    spec = ee.load_spec()
    assert (spec.filing_start, spec.data_end, spec.price_start) == (
        date(2003, 1, 1),
        date(2010, 12, 31),
        date(2003, 9, 1),
    )
    issuers = {issuer.id: (issuer.ciks, issuer.fmp_ticker) for issuer in spec.issuers}
    assert issuers == {
        "MSFT": ((789019,), "MSFT"),
        "INTC": ((50863,), "INTC"),
        "CSCO": ((858877,), "CSCO"),
        "QCOM": ((804328,), "QCOM"),
        "AMGN": ((318154,), "AMGN"),
        "EBAY": ((1065088,), "EBAY"),
        "AAPL": ((320193,), "AAPL"),
        "GOOG": ((1288776,), "GOOG"),
        "ORCL": ((1341439,), "ORCL"),
        "DELL": ((826083,), None),
        "CMCSA": ((1166691,), "CMCSA"),
        "NXTL": ((824169,), None),
        "IACI": ((891103,), None),
        "SBUX": ((829224,), "SBUX"),
        "SYMC": ((849399,), None),
        "GILD": ((882095,), "GILD"),
        "AMZN": ((1018724,), "AMZN"),
    }
    assert {issuer.id for issuer in spec.foreign} == {"RIMM", "TEVA"}
    documents = {
        d.fiscal_year: (d.filing_date.isoformat(), d.accession, d.document)
        for d in spec.holdings_documents
    }
    assert documents == {
        2003: ("2004-01-30", "0001206774-04-000023", "d13831.txt"),
        2004: ("2005-05-16", "0000930413-05-003631", "c36210.txt"),
        2005: ("2006-04-25", "0001047469-06-005571", "a2167148zn-30b_2.txt"),
        2006: ("2007-01-18", "0001104659-07-003177", "a06-24326_3n30b2.htm"),
        2007: ("2008-02-25", "0001104659-08-012645", "a08-3893_3n30b2.htm"),
        2008: ("2009-01-30", "0001104659-09-005352", "a08-28674_2n30b2.htm"),
        2009: ("2010-01-15", "0001104659-10-001821", "a09-31668_2n30b2.htm"),
        2010: ("2010-12-30", "0001104659-10-064790", "a10-18275_1n30b2.htm"),
    }
