"""Invariants of the earnings event intake after 2010 (TRADING-2565-129).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-11-10:earnings_shock_after_2010``: Baker chose to read the
period 6 earnings-shock print-only table once on QQQ after 2010, rules unchanged). Every value
below is synthetic; no test reads a real snapshot or downloads anything.

74. The intake reads QQQ after 2010 only through the registered one-time window. A spec that
    names a window names qqq_after_2010_owner_print and an owner decision, asks for no price (no
    price_start, no FMP ticker) and ends after 2010-12-31; one without a window keeps its old
    rules and its snapshots stay byte for byte what they were. Before any request, and again in
    the check, the window must be registered as the one-time owner print from 2011-01-03 to the
    latest close, the decision must stand in the TRADING-2565 requirements, and data_end must not
    be later than the last close of the latest checked QQQ long-history snapshot; otherwise the
    fetch requests and writes nothing and the check fails. Filings after data_end never reach a
    file, the price source is never built or called, and both CIKs of one issuer count and must
    each name it.
75. The settings file research_earnings_after_2010.yaml names the twenty-four issuers with their
    CIKs and name patterns, TEVA as foreign, the sixteen holdings documents, filing_start
    2010-10-01, data_end 2026-10-12 (the last close of qlh-20261013), the window and the decision;
    the registry still describes the window as the one-time owner print.
"""

from __future__ import annotations

import json
import shutil
import time
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import PriceRequest
from ai_trading_system.research_data import earnings_events as ee
from ai_trading_system.yaml_loader import safe_load_yaml_path

SUBMISSIONS = "https://data.sec.gov/submissions/"
ARCHIVES = "https://www.sec.gov/Archives/edgar/data/1067839/"
WINDOW = "qqq_after_2010_owner_print"
DECISION = "owner_decision:TRADING-2565:2026-11-10:earnings_shock_after_2010"
QLH_ID = "qlh-20261013-0b0df53cb1a2"
REGISTRY = Path("config/research/research_window_registry.yaml")
REQUIREMENTS = Path("docs/requirements/TRADING-2565_Research_Bench.md")
COLUMNS = (
    "accessionNumber",
    "filingDate",
    "form",
    "acceptanceDateTime",
    "items",
    "primaryDocument",
)
WINDOW_ENTRY = {
    "start": "2011-01-03",
    "end": "latest",
    "role": "owner_print_only",
    "read_policy": "once_per_owner_decision",
    "assets": ["QQQ"],
}


def _window_spec(**changes: Any) -> ee.EarningsSpec:
    raw: dict[str, Any] = {
        "schema_version": "research_earnings.v1",
        "filing_start": "2010-10-01",
        "data_end": "2026-10-12",
        "window_id": WINDOW,
        "owner_decision": DECISION,
        "issuers": [
            {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha corp", "fmp_ticker": None},
            {"id": "GAMA", "ciks": [333, 444], "entity_pattern": "^(gamma|omega)"},
        ],
        "foreign": [{"id": "FRGN", "foreign": True}],
        "holdings_documents": [
            {
                "fiscal_year": 2024,
                "filing_date": "2024-12-27",
                "accession": "0001193125-24-285734",
                "document": "d876223dn30b2.htm",
                "form": "N-30B-2",
            },
            {
                "fiscal_year": 2025,
                "filing_date": "2025-10-24",
                "accession": "0001193125-25-249697",
                "document": "d29525dn30b2.htm",
                "form": "N-30B-2",
            },
        ],
    }
    raw.update(changes)
    return ee.EarningsSpec.model_validate(raw)


def _columns(rows: Sequence[tuple[str, ...]]) -> dict[str, list[str]]:
    return {name: [row[i] for row in rows] for i, name in enumerate(COLUMNS)}


ALPHA_RECENT = [
    ("0000000111-26-000003", "2026-10-13", "8-K", "2026-10-13T20:05:00.000Z", "2.02,9.01", "a.htm"),
    ("0000000111-26-000002", "2026-10-12", "8-K", "2026-10-12T20:05:00.000Z", "2.02,9.01", "b.htm"),
    ("0000000111-24-000009", "2024-01-26", "10-Q", "2024-01-26T20:00:00.000Z", "", "c.htm"),
    ("0000000111-24-000008", "2024-01-25", "8-K", "2024-01-25T21:01:00.000Z", "2.02", "d.htm"),
]
ALPHA_PAGE = [
    ("0000000111-12-000007", "2012-04-19", "8-K/A", "2012-04-19T20:30:00.000Z", "2.02", "e.htm"),
    ("0000000111-10-000006", "2010-10-01", "8-K", "2010-10-01T12:00:00.000Z", "7.01", "f.htm"),
    ("0000000111-10-000005", "2010-09-30", "8-K", "2010-09-30T20:00:00.000Z", "2.02", "g.htm"),
]
ALPHA_OLD_PAGE = [
    ("0000000111-08-000004", "2008-04-15", "8-K", "2008-04-15T21:00:00.000Z", "2.02", "h.htm"),
]
GAMMA_RECENT = [
    ("0000000333-14-000001", "2014-07-17", "8-K", "2014-07-17T20:05:00.000Z", "2.02", "i.htm"),
]
OMEGA_RECENT = [
    ("0000000444-16-000001", "2016-02-01", "8-K", "2016-02-01T21:10:00.000Z", "2.02,9.01", "j.htm"),
]
KEPT = {
    "0000000111-26-000002",
    "0000000111-24-000008",
    "0000000111-12-000007",
    "0000000111-10-000006",
    "0000000333-14-000001",
    "0000000444-16-000001",
}
DROPPED = (b"0000000111-26-000003", b"2026-10-13", b"0000000111-10-000005", b"0000000111-24-000009")


def _document(year: int) -> bytes:
    return (
        b"<html><body><p>SCHEDULE OF INVESTMENTS</p><p>September 30, "
        + str(year).encode()
        + b"</p><table><tr><td>Alpha Corp</td><td>1,000,000</td><td>50,000,000</td></tr>"
        b"<tr><td>Total Investments</td><td>60,000,000</td></tr></table></body></html>\r\n"
    )


def _sec_pages(omega_name: str = "OMEGA HOLDINGS INC") -> dict[str, bytes]:
    alpha = {
        "cik": "0000000111",
        "name": "ALPHA CORP",
        "formerNames": [],
        "filings": {
            "recent": _columns(ALPHA_RECENT),
            "files": [
                {
                    "name": "CIK0000000111-submissions-001.json",
                    "filingFrom": "2009-01-01",
                    "filingTo": "2013-12-31",
                },
                {
                    "name": "CIK0000000111-submissions-002.json",
                    "filingFrom": "2001-01-01",
                    "filingTo": "2008-12-31",
                },
            ],
        },
    }
    gamma = {
        "cik": "0000000333",
        "name": "GAMMA INC.",
        "formerNames": [],
        "filings": {"recent": _columns(GAMMA_RECENT), "files": []},
    }
    omega = {
        "cik": "0000000444",
        "name": omega_name,
        "formerNames": [],
        "filings": {"recent": _columns(OMEGA_RECENT), "files": []},
    }
    return {
        f"{SUBMISSIONS}CIK0000000111.json": json.dumps(alpha).encode(),
        f"{SUBMISSIONS}CIK0000000111-submissions-001.json": json.dumps(
            _columns(ALPHA_PAGE)
        ).encode(),
        f"{SUBMISSIONS}CIK0000000111-submissions-002.json": json.dumps(
            _columns(ALPHA_OLD_PAGE)
        ).encode(),
        f"{SUBMISSIONS}CIK0000000333.json": json.dumps(gamma).encode(),
        f"{SUBMISSIONS}CIK0000000444.json": json.dumps(omega).encode(),
        f"{ARCHIVES}000119312524285734/d876223dn30b2.htm": _document(2024),
        f"{ARCHIVES}000119312525249697/d29525dn30b2.htm": _document(2025),
    }


class FakeSec:
    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if url not in self.pages:
            raise RuntimeError("HTTP 404")
        return self.pages[url]


class TrapPrices:
    """A price source that must never be asked."""

    def __init__(self) -> None:
        self.requests: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.requests.append(request)
        raise AssertionError("a window spec asked for prices")


def _write_root(
    root: Path,
    *,
    last_close: str | None = "2026-10-12",
    status: str = "PASS",
    check_id: str = QLH_ID,
    entry: dict[str, Any] | None = None,
    requirements: str | None = None,
) -> Path:
    """The registry, the requirements and the QQQ snapshot's check, as a code root holds them."""

    registry = root / REGISTRY
    registry.parent.mkdir(parents=True, exist_ok=True)
    windows = {"qqq_long_history_validation": {**WINDOW_ENTRY, "role": "validation"}}
    if entry is not None:
        windows[WINDOW] = entry
    registry.write_text(json.dumps({"windows": windows}), encoding="utf-8")
    document = root / REQUIREMENTS
    document.parent.mkdir(parents=True, exist_ok=True)
    text = f"## Owner decision\n\n- 财报：{DECISION}。由 TRADING-2565-128 登记。\n"
    document.write_text(text if requirements is None else requirements, encoding="utf-8")
    if last_close is not None:
        snapshots = root / "data" / "raw" / "research" / "qqq_long_history"
        (snapshots / QLH_ID).mkdir(parents=True, exist_ok=True)
        (snapshots / "LATEST").write_text(QLH_ID + "\n", encoding="utf-8")
        (snapshots / QLH_ID / "manifest.json").write_text(
            json.dumps({"snapshot_id": QLH_ID}), encoding="utf-8"
        )
        check = {
            "status": status,
            "snapshot_id": check_id,
            "calendar": {"first_date": "1985-10-01", "last_date": last_close},
        }
        (snapshots / QLH_ID / "check.json").write_text(json.dumps(check), encoding="utf-8")
    return root


def _good_root(root: Path) -> Path:
    return _write_root(root, entry=dict(WINDOW_ENTRY))


def _earnings_dir(root: Path) -> Path:
    return root / "data" / "raw" / "research" / "earnings_events"


def _files(root: Path) -> list[Path]:
    base = _earnings_dir(root)
    return [path for path in base.rglob("*") if path.is_file()] if base.exists() else []


def _fetch(root: Path, spec: ee.EarningsSpec | None = None, sec: FakeSec | None = None) -> str:
    return ee.fetch_snapshot(
        spec or _window_spec(), root, sec_get=sec or FakeSec(_sec_pages()), price_provider=None
    )


# 74 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "changes",
    [
        {"owner_decision": None},
        {"window_id": None},
        {"window_id": None, "owner_decision": None},
        {"window_id": "qqq_long_history_validation"},
        {"price_start": "2010-09-01"},
        {
            "issuers": [
                {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha", "fmp_ticker": "ALPH"}
            ]
        },
        {
            "data_end": "2010-12-31",
            "holdings_documents": [
                {
                    "fiscal_year": 2009,
                    "filing_date": "2010-01-15",
                    "accession": "0001104659-10-001821",
                    "document": "a09-31668_2n30b2.htm",
                    "form": "N-30B-2",
                }
            ],
        },
        {"filing_start": "2026-10-13"},
        {"owner_decision": "earnings_shock_after_2010"},
        {"owner_decision": "owner_decision:TRADING-2565:2026-11-10"},
        {
            "holdings_documents": [
                {
                    "fiscal_year": 2025,
                    "filing_date": "2026-10-13",
                    "accession": "0001193125-26-000001",
                    "document": "x.htm",
                    "form": "N-30B-2",
                }
            ]
        },
    ],
)
def test_a_window_spec_names_its_window_and_decision_and_asks_for_no_price(
    changes: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        _window_spec(**changes)


def test_a_spec_without_a_window_keeps_its_old_rules() -> None:
    _development_spec()
    for changes in (
        {"price_start": None},
        {"owner_decision": DECISION},
        {"data_end": "2011-01-03"},
        {"window_id": WINDOW, "owner_decision": DECISION, "price_start": "2003-09-01"},
    ):
        with pytest.raises(ValueError):
            _development_spec(**changes)
    assert ee.window_record(_development_spec(), Path("/nonexistent-root")) is None


def test_the_window_snapshot_keeps_the_span_and_no_price(tmp_path: Path) -> None:
    root = _good_root(tmp_path)
    sec = FakeSec(_sec_pages())
    snapshot = _fetch(root, sec=sec)
    report = ee.check_snapshot(root, snapshot)
    assert report["status"] == "PASS", report
    assert report["window"] == {
        "window_id": WINDOW,
        "owner_decision": DECISION,
        "data_end": "2026-10-12",
        "qqq_last_close": "2026-10-12",
    }
    assert report["prices"]["tickers"] == {}
    loaded = ee.load_snapshot(root, snapshot)
    assert set(loaded.filings["accessionNumber"]) == KEPT
    assert set(loaded.filings["cik"]) == {111, 333, 444}
    assert loaded.prices.empty
    assert list(loaded.prices.columns) == ["ticker", "date", "adj_close"]
    assert loaded.documents[2025] == ("d29525dn30b2.htm", _document(2025))
    assert loaded.spec.window_id == WINDOW and loaded.spec.price_start is None
    assert f"{SUBMISSIONS}CIK0000000111-submissions-001.json" in sec.calls
    assert f"{SUBMISSIONS}CIK0000000111-submissions-002.json" not in sec.calls
    directory = _earnings_dir(root) / snapshot
    assert (directory / "prices.csv").read_bytes() == b"ticker,date,adj_close\n"
    assert b"FMP" not in (directory / "fetch.json").read_bytes()
    for path in _files(root):
        for late in DROPPED:
            assert late not in path.read_bytes(), (path.name, late)
    for name in ("check.md", "check.json"):
        text = (directory / name).read_text("utf-8")
        for value in ("0000000111-26-000002", "20:05", "Alpha Corp", "60,000,000"):
            assert value not in text, (name, value)


WINDOW_CASES: dict[str, dict[str, Any]] = {
    "data_end after the QQQ snapshot's last close": {
        "entry": WINDOW_ENTRY,
        "last_close": "2026-10-09",
    },
    "no QQQ snapshot": {"entry": WINDOW_ENTRY, "last_close": None},
    "a failed QQQ check": {"entry": WINDOW_ENTRY, "status": "FAIL"},
    "a QQQ check of another snapshot": {
        "entry": WINDOW_ENTRY,
        "check_id": "qlh-20260101-000000000000",
    },
    "a QQQ check without a last close": {"entry": WINDOW_ENTRY, "last_close": "latest"},
    "an unregistered window": {"entry": None},
    "a window that ends before the span": {"entry": {**WINDOW_ENTRY, "end": "2021-02-19"}},
    "a window that starts elsewhere": {"entry": {**WINDOW_ENTRY, "start": "2011-01-04"}},
    "a window with another role": {"entry": {**WINDOW_ENTRY, "role": "validation"}},
    "a window read more than once": {"entry": {**WINDOW_ENTRY, "read_policy": "unlimited"}},
    "an unregistered decision": {"entry": WINDOW_ENTRY, "requirements": "## Owner decision\n"},
    "a longer decision only": {
        "entry": WINDOW_ENTRY,
        "requirements": f"- {DECISION}_v2 由 TRADING-2565-128 登记。\n",
    },
}


@pytest.mark.parametrize("case", sorted(WINDOW_CASES))
def test_a_window_read_needs_the_registered_window_the_decision_and_the_qqq_close(
    tmp_path: Path, case: str
) -> None:
    settings = dict(WINDOW_CASES[case])
    entry = settings.pop("entry")
    root = _write_root(tmp_path, entry=None if entry is None else dict(entry), **settings)
    with pytest.raises(ValueError):
        ee.window_record(_window_spec(), root)
    sec = FakeSec(_sec_pages())
    with pytest.raises(ValueError):
        _fetch(root, sec=sec)
    assert sec.calls == []
    assert _files(root) == []


def test_the_check_fails_a_window_snapshot_once_its_window_no_longer_holds(
    tmp_path: Path,
) -> None:
    root = _good_root(tmp_path)
    snapshot = _fetch(root)
    assert ee.check_snapshot(root, snapshot)["status"] == "PASS"
    ee.load_snapshot(root, snapshot)
    _write_root(root, entry=dict(WINDOW_ENTRY), requirements="## Owner decision\n")
    report = ee.check_snapshot(root, snapshot)
    assert report["status"] == "FAIL" and "window" in report["failed"]
    with pytest.raises(ValueError):
        ee.load_snapshot(root, snapshot)
    outcome = CliRunner().invoke(ee.app, ["check", "--snapshot", snapshot, "--root", str(root)])
    assert outcome.exit_code == 1
    _write_root(root, entry=dict(WINDOW_ENTRY), last_close="2026-10-09")
    report = ee.check_snapshot(root, snapshot)
    assert report["status"] == "FAIL" and "window" in report["failed"]
    _good_root(root)
    assert ee.check_snapshot(root, snapshot)["status"] == "PASS"


def test_both_ciks_of_one_issuer_count_and_each_must_name_it(tmp_path: Path) -> None:
    root = _good_root(tmp_path)
    snapshot = _fetch(root)
    assert ee.check_snapshot(root, snapshot)["status"] == "PASS"
    loaded = ee.load_snapshot(root, snapshot)
    assert {(entity["cik"], entity["issuer"]) for entity in loaded.entities} == {
        (111, "ALPH"),
        (333, "GAMA"),
        (444, "GAMA"),
    }
    years = ee.check_snapshot(root, snapshot)["filings"]["per_issuer_year"]["GAMA"]
    assert years["2014"]["2.02"] == 1 and years["2016"]["2.02"] == 1
    other = tmp_path / "other"
    _good_root(other)
    with pytest.raises(ValueError):
        _fetch(other, sec=FakeSec(_sec_pages(omega_name="ZETA CORP")))
    assert _files(other) == []


# A spec without a window, fetched by the code as it stood before this batch (main aa65cd8a): the
# same inputs must give the same content address.
DEVELOPMENT_SNAPSHOT_ID = "ern-4981713cb3c1"


def _development_spec(**changes: Any) -> ee.EarningsSpec:
    raw: dict[str, Any] = {
        "schema_version": "research_earnings.v1",
        "filing_start": "2003-01-01",
        "data_end": "2010-12-31",
        "price_start": "2003-09-01",
        "issuers": [
            {"id": "ALPH", "ciks": [111], "entity_pattern": "^alpha corp", "fmp_ticker": "ALPH"}
        ],
        "holdings_documents": [
            {
                "fiscal_year": 2009,
                "filing_date": "2010-01-15",
                "accession": "0001104659-10-001821",
                "document": "a09-31668_2n30b2.htm",
                "form": "N-30B-2",
            }
        ],
    }
    raw.update(changes)
    return ee.EarningsSpec.model_validate(raw)


class FixedPrices:
    def __init__(self) -> None:
        self.requests: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.requests.append(request)
        days = pd.bdate_range(request.start, request.end)
        return pd.DataFrame(
            {
                "ticker": request.tickers[0],
                "date": days.strftime("%Y-%m-%d"),
                "adj_close": [50.0 + (day - pd.Timestamp("2003-01-01")).days / 100 for day in days],
            }
        )


def test_a_spec_without_a_window_fetches_the_same_snapshot_as_before(tmp_path: Path) -> None:
    alpha = {
        "cik": "0000000111",
        "name": "ALPHA CORP",
        "formerNames": [],
        "filings": {
            "recent": _columns(
                [
                    (
                        "0000000111-09-000002",
                        "2009-07-16",
                        "8-K",
                        "2009-07-16T20:10:00.000Z",
                        "2.02",
                        "a.htm",
                    ),
                    (
                        "0000000111-04-000001",
                        "2004-04-15",
                        "8-K",
                        "2004-04-15T11:30:00.000Z",
                        "12",
                        "b.htm",
                    ),
                ]
            ),
            "files": [],
        },
    }
    pages = {
        f"{SUBMISSIONS}CIK0000000111.json": json.dumps(alpha).encode(),
        f"{ARCHIVES}000110465910001821/a09-31668_2n30b2.htm": _document(2009),
    }
    prices = FixedPrices()
    snapshot = ee.fetch_snapshot(
        _development_spec(), tmp_path, sec_get=FakeSec(pages), price_provider=prices
    )
    assert snapshot == DEVELOPMENT_SNAPSHOT_ID
    manifest = json.loads((_earnings_dir(tmp_path) / snapshot / "manifest.json").read_bytes())
    assert set(manifest["spec"]) == {
        "schema_version",
        "filing_start",
        "data_end",
        "price_start",
        "issuers",
        "foreign",
        "holdings_documents",
    }
    report = ee.check_snapshot(tmp_path, snapshot)
    assert report["status"] == "PASS" and "window" not in report
    assert prices.requests


def test_the_fetch_command_reads_the_named_settings_and_builds_no_price_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _good_root(tmp_path)
    pages = _sec_pages()
    seen: dict[str, list[Any]] = {"get": [], "spec": [], "prices": []}

    class Response:
        def __init__(self, url: str) -> None:
            self.status_code = 200 if url in pages else 404
            self.content = pages.get(url, b"")

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise RuntimeError(f"HTTP {self.status_code}")

    def get(url: str, *args: Any, **kwargs: Any) -> Response:
        seen["get"].append((url, dict(kwargs.get("headers") or {})))
        return Response(url)

    def load_spec(path: Path = ee.DEFAULT_SPEC_PATH) -> ee.EarningsSpec:
        seen["spec"].append(Path(path))
        return _window_spec()

    def no_prices() -> Any:
        seen["prices"].append(1)
        raise AssertionError("the price source was built")

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    monkeypatch.setattr(ee, "load_spec", load_spec)
    monkeypatch.setattr(ee, "price_provider_factory", no_prices)
    monkeypatch.setenv("SEC_USER_AGENT", "Sentinel Research sentinel-agent@example.invalid")
    spec_path = "config/research/research_earnings_after_2010.yaml"
    outcome = CliRunner().invoke(ee.app, ["fetch", "--spec", spec_path, "--root", str(root)])
    assert outcome.exit_code == 0, outcome.output
    assert [path.name for path in seen["spec"]] == ["research_earnings_after_2010.yaml"]
    assert seen["prices"] == []
    assert len(seen["get"]) == 6
    assert all(headers.get("User-Agent", "").startswith("Sentinel") for _, headers in seen["get"])
    assert "sentinel" not in outcome.output.lower()
    for path in _files(root):
        assert b"sentinel" not in path.read_bytes().lower(), path.name
    late = _write_root(tmp_path / "late", entry=dict(WINDOW_ENTRY), last_close="2026-10-09")
    seen["get"].clear()
    outcome = CliRunner().invoke(ee.app, ["fetch", "--spec", spec_path, "--root", str(late)])
    assert outcome.exit_code == 1
    assert seen["get"] == [] and _files(late) == []


# 75 ----------------------------------------------------------------------------------------------


def test_the_after_2010_settings_name_the_issuers_documents_and_window() -> None:
    spec = ee.load_spec(ee.AFTER_2010_SPEC_PATH)
    assert ee.AFTER_2010_SPEC_PATH == (
        PROJECT_ROOT / "config" / "research" / "research_earnings_after_2010.yaml"
    )
    assert (spec.filing_start, spec.data_end, spec.price_start) == (
        date(2010, 10, 1),
        date(2026, 10, 12),
        None,
    )
    assert (spec.window_id, spec.owner_decision) == (WINDOW, DECISION)
    issuers = {issuer.id: (issuer.ciks, issuer.entity_pattern) for issuer in spec.issuers}
    assert issuers == {
        "AAPL": ((320193,), "^apple"),
        "MSFT": ((789019,), "^microsoft"),
        "GOOG": ((1288776, 1652044), "^(google|alphabet)"),
        "AMZN": ((1018724,), "^amazon"),
        "QCOM": ((804328,), "^qualcomm"),
        "ORCL": ((1341439,), "^oracle corp"),
        "INTC": ((50863,), "^intel corp"),
        "CSCO": ((858877,), "^cisco systems"),
        "GILD": ((882095,), "^gilead"),
        "META": ((1326801,), "^(meta platforms|facebook)"),
        "TSLA": ((1318605,), "^tesla"),
        "NVDA": ((1045810,), "^nvidia"),
        "AVGO": ((1730168,), "^broadcom"),
        "AMGN": ((318154,), "^amgen"),
        "CMCSA": ((1166691,), "^comcast corp"),
        "MDLZ": ((1103982,), "^(mondelez|kraft foods)"),
        "KHC": ((1637459,), "^kraft heinz"),
        "CELG": ((816284,), "^celgene"),
        "NFLX": ((1065280,), "^netflix"),
        "PEP": ((77476,), "^pepsico"),
        "ADBE": ((796343,), "^adobe"),
        "PYPL": ((1633917,), "^paypal"),
        "COST": ((909832,), "^costco"),
        "PLTR": ((1321655,), "^palantir"),
    }
    assert all(issuer.fmp_ticker is None for issuer in spec.issuers)
    assert {issuer.id for issuer in spec.foreign} == {"TEVA"}
    documents = {
        d.fiscal_year: (d.filing_date.isoformat(), d.accession, d.document, d.form)
        for d in spec.holdings_documents
    }
    assert documents == {
        2010: ("2010-12-30", "0001104659-10-064790", "a10-18275_1n30b2.htm", "N-30B-2"),
        2011: ("2012-01-20", "0001104659-12-003105", "a11-28102_1n30b2.htm", "N-30B-2"),
        2012: ("2013-01-28", "0001104659-13-005075", "a12-24340_1n30b2.htm", "N-30B-2"),
        2013: ("2014-01-17", "0001104659-14-003107", "a13-21957_1n30b2.htm", "N-30B-2"),
        2014: ("2015-01-15", "0001104659-15-002429", "a14-24967_1n30b2.htm", "N-30B-2"),
        2015: ("2016-01-06", "0001193125-16-422287", "d95463dn30b2.htm", "N-30B-2"),
        2016: ("2016-12-29", "0001193125-16-806695", "d272217dn30b2.htm", "N-30B-2"),
        2017: ("2018-01-03", "0001193125-18-001208", "d432220dn30b2.htm", "N-30B-2"),
        2018: ("2018-12-21", "0001193125-18-355790", "d638310dn30b2.htm", "N-30B-2"),
        2019: ("2019-12-20", "0001193125-19-320290", "d813945dn30b2.htm", "N-30B-2"),
        2020: ("2020-12-23", "0001193125-20-326183", "d75380dn30b2.htm", "N-30B-2"),
        2021: ("2021-12-29", "0001193125-21-368691", "d211991dn30b2.htm", "N-30B-2"),
        2022: ("2022-12-29", "0001193125-22-313828", "d385116dn30b2.htm", "N-30B-2"),
        2023: ("2023-12-28", "0001193125-23-304771", "d538532dn30b2.htm", "N-30B-2"),
        2024: ("2024-12-27", "0001193125-24-285734", "d876223dn30b2.htm", "N-30B-2"),
        2025: ("2025-10-24", "0001193125-25-249697", "d29525dn30b2.htm", "N-30B-2"),
    }


def test_the_real_window_and_decision_are_registered(tmp_path: Path) -> None:
    entry = safe_load_yaml_path(PROJECT_ROOT / REGISTRY)["windows"][WINDOW]
    assert (str(entry["start"]), str(entry["end"]), entry["role"], entry["read_policy"]) == (
        "2011-01-03",
        "latest",
        "owner_print_only",
        "once_per_owner_decision",
    )
    _write_root(tmp_path, entry=None)
    for relative in (REGISTRY, REQUIREMENTS):
        shutil.copyfile(PROJECT_ROOT / relative, tmp_path / relative)
    spec = ee.load_spec(ee.AFTER_2010_SPEC_PATH)
    assert ee.window_record(spec, tmp_path) == {
        "window_id": WINDOW,
        "owner_decision": DECISION,
        "data_end": "2026-10-12",
        "qqq_last_close": "2026-10-12",
    }
    _write_root(tmp_path, entry=None, last_close="2026-10-09")
    for relative in (REGISTRY, REQUIREMENTS):
        shutil.copyfile(PROJECT_ROOT / relative, tmp_path / relative)
    with pytest.raises(ValueError):
        ee.window_record(spec, tmp_path)
