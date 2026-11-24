"""Invariants of the Nasdaq-100 option-strategy index intake (TRADING-2567-41).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2567:2026-11-22:option_index_first_look``: Baker reopened the QQQ options
direction and chose the free first look on three Nasdaq-100 option-strategy indices). Every value
below is synthetic; no test reads a real snapshot or downloads anything.

76. The spec fixes the three indices and their sources: BXN from Cboe's history file with FRED's
    copy as a cross-check only; NQTRI and NQCLRI from Nasdaq's files through 2021-08-20, then from
    FRED from 2021-08-23, where "." or an empty value is a closed day (TRADING-2567-42: FRED's
    current file leaves a closed day empty); no redistribution and no external model. A spec that
    names other indices, overlaps or leaves open the two parts, or repeats a name is refused.
    Files are read by their header names, quoted thousands separators, a byte-order mark and the
    four date forms the read-only probe parsed (YYYY-MM-DD, M/D/YYYY, M/D/YY and D-Mon-YYYY with
    an English month in any case, month and day in one or two digits); an error names the file
    and the row, never the cell. A file is read as UTF-8, else as Windows-1252, else as Latin-1,
    with header names trimmed and spaces after a comma skipped (TRADING-2567-43: Nasdaq's file is
    not UTF-8); the snapshot keeps the downloaded bytes and the check reads them the same way.
77. A snapshot is written only when the owner decision stands in the TRADING-2567 requirements
    and every data file downloads and parses; a methodology document that fails is recorded and
    warned, never fatal. The id is the content address of the manifest, so the same bytes give
    the same id and one changed byte another.
78. The check judges the whole series without repairs and writes only labels, counts, dates and
    PASS/WARN/FAIL. It fails a late first day, a repeated or unordered date, a value that is not
    positive, a daily move over 25%, a gap over five calendar days (2001-09-10..09-17 excepted), a
    series that ends before the QQQ snapshot, a seam whose second part does not start on the next
    trading day or whose seam-day return differs from the Nasdaq-100 total return by more than
    1.5%, a changed byte and a broken manifest. It warns on a closed-day marker on a trading day,
    a day off the QQQ calendar, a base that is not 100, a cross-check difference over 1e-4 and a
    missing document. load_snapshot reads only a checked, unchanged snapshot and splices the
    first part through its last day and the second from its first day.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from ai_trading_system.research_data import option_indices as oi
from ai_trading_system.research_data import qqq_long_history as qlh

DECISION = "owner_decision:TRADING-2567:2026-11-22:option_index_first_look"
REQUIREMENTS = Path("docs") / "requirements" / "TRADING-2567_QQQ_Research_Program_1999_Window.md"
CBOE_BXN = "https://cdn.cboe.com/api/global/us_indices/daily_prices/BXN_History.csv"
NASDAQ = "https://indexes.nasdaq.com/docs/AdditionalData_{}.csv"
FRED = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"
# BXN starts near this level so that a leaked value would be easy to see in any output.
LEAK_BASE = 4321.8765
SPLIT = {"NQTRI": ("nqtri_nasdaq", "nqtri_fred"), "NQCLRI": ("nqclri_nasdaq", "nqclri_fred")}
MONTH_NAMES = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
FRED_NAMES = ("bxn_fred", "nqtri_fred", "nqclri_fred")


# --- a synthetic world -----------------------------------------------------------------------


def _shown(day: date, style: str) -> str:
    """A date as a source file writes it, in one of the four forms the read-only probe parsed."""
    if style == "iso":
        return day.isoformat()
    if style == "us":
        return day.strftime("%m/%d/%Y")
    if style == "us2":
        return day.strftime("%m/%d/%y")
    if style == "mon":
        return f"{day.day:02d}-{MONTH_NAMES[day.month - 1]}-{day.year}"
    raise ValueError(style)


@dataclass
class World:
    calendar: pd.DatetimeIndex
    reference: pd.Series
    spec: oi.OptionIndexSpec
    rows: dict[str, list[tuple[date, str]]]
    styles: dict[str, str]
    documents: dict[str, bytes | None] = field(default_factory=dict)

    def source(self, name: str) -> oi.SourceSpec:
        return next(s for s in oi.all_sources(self.spec) if s.name == name)

    def payloads(self) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for source in oi.all_sources(self.spec):
            style = self.styles[source.name]
            lines = [f"{source.date_column},{source.value_column}"]
            for day, text in self.rows[source.name]:
                lines.append(f"{_shown(day, style)},{text}")
            out[source.url] = ("\n".join(lines) + "\n").encode("utf-8")
        for index in self.spec.indices:
            for document in index.documents:
                payload = self.documents.get(document.name, b"%PDF-1.4 synthetic\n")
                if payload is not None:
                    out[document.url] = payload
        return out


def _spec_dict(*, bxn_first: date, first: date, through: date, start: date) -> dict[str, Any]:
    raw = oi.load_spec().model_dump(mode="json")
    raw["indices"][0]["first_date"] = bxn_first.isoformat()
    for index in raw["indices"][1:]:
        index["first_date"] = first.isoformat()
        index["sources"][0]["through_date"] = through.isoformat()
        index["sources"][1]["from_date"] = start.isoformat()
    return raw


def _level_path(
    calendar: pd.DatetimeIndex, reference: pd.Series, first: date, base: float, scale: float
) -> dict[date, float]:
    days = calendar[calendar >= pd.Timestamp(first)]
    returns = reference.loc[days].to_numpy(dtype=float, copy=True)
    returns[0] = 0.0
    values = base * np.cumprod(1.0 + scale * returns)
    return {d.date(): float(v) for d, v in zip(days, values, strict=True)}


def _weekdays(start: date, end: date) -> list[date]:
    return [d.date() for d in pd.bdate_range(start, end)]


def make_world(
    *,
    start: date = date(2021, 5, 3),
    end: date = date(2021, 10, 29),
    holidays: tuple[date, ...] = (date(2021, 5, 31), date(2021, 7, 5), date(2021, 9, 6)),
    first: date = date(2021, 5, 21),
    through: date = date(2021, 8, 20),
    seam_start: date = date(2021, 8, 23),
    data_end: date = date(2021, 11, 5),
    crosscheck_from: date = date(2021, 6, 1),
) -> World:
    calendar = pd.DatetimeIndex([d for d in pd.bdate_range(start, end) if d.date() not in holidays])
    phase = np.arange(len(calendar), dtype=float)
    reference = pd.Series(0.004 * np.sin(phase + 1.0), index=calendar, name="ndx_tr_return")
    reference.iloc[0] = np.nan
    spec = oi.OptionIndexSpec.model_validate(
        _spec_dict(bxn_first=first, first=first, through=through, start=seam_start)
    )
    # The indices trade on the QQQ calendar and keep moving (on the same returns) a week past it.
    extra = pd.DatetimeIndex(
        [d for d in pd.bdate_range(end + timedelta(days=1), data_end) if d.date() not in holidays]
    )
    full = pd.DatetimeIndex(calendar.append(extra))
    extended = pd.concat([reference, pd.Series(0.001, index=extra)])
    paths = {
        "BXN": _level_path(full, extended, first, LEAK_BASE, 0.6),
        "NQTRI": _level_path(full, extended, first, 100.0, 0.8),
        "NQCLRI": _level_path(full, extended, first, 100.0, 0.5),
    }
    rows: dict[str, list[tuple[date, str]]] = {}
    styles: dict[str, str] = {}
    rows["bxn_cboe"] = [(d, f"{v:.6f}") for d, v in paths["BXN"].items()]
    styles["bxn_cboe"] = "us"
    rows["bxn_fred"] = [
        (d, f"{paths['BXN'][d]:.6f}" if d in paths["BXN"] else ".")
        for d in _weekdays(crosscheck_from, data_end)
    ]
    styles["bxn_fred"] = "iso"
    for index_id, (csv_name, fred_name) in SPLIT.items():
        path = paths[index_id]
        rows[csv_name] = [(d, f"{v:.6f}") for d, v in path.items() if d <= through]
        styles[csv_name] = "us"
        rows[fred_name] = [
            (d, f"{path[d]:.6f}" if d in path else ".") for d in _weekdays(seam_start, data_end)
        ]
        styles[fred_name] = "iso"
    return World(calendar, reference, spec, rows, styles)


class FakeGet:
    def __init__(self, payloads: dict[str, bytes], failing: tuple[str, ...] = ()) -> None:
        self.payloads = payloads
        self.failing = failing
        self.calls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        if url in self.failing or url not in self.payloads:
            raise ConnectionError("synthetic failure")
        return self.payloads[url]


def _write_decision(root: Path, text: str | None = None) -> None:
    path = root / REQUIREMENTS
    path.parent.mkdir(parents=True, exist_ok=True)
    body = f"决定号 `{DECISION}`。\n" if text is None else text
    path.write_text(body, encoding="utf-8")


def _files(root: Path) -> list[Path]:
    base = root.joinpath(*oi.SNAPSHOT_PARTS)
    return sorted(p for p in base.rglob("*") if p.is_file()) if base.exists() else []


def _fetch(root: Path, world: World, failing: tuple[str, ...] = ()) -> str:
    _write_decision(root)
    return oi.fetch_snapshot(world.spec, root, get=FakeGet(world.payloads(), failing))


def _check(root: Path, world: World, failing: tuple[str, ...] = ()) -> dict[str, Any]:
    snapshot = _fetch(root, world, failing)
    return oi.check_snapshot(root, snapshot, world.reference)


def _walk(value: Any) -> Iterator[Any]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)
    else:
        yield value


def _value(world: World, name: str, day: date) -> str:
    return next(t for d, t in world.rows[name] if d == day)


def _replace(world: World, name: str, day: date, text: str) -> None:
    world.rows[name] = [(d, text if d == day else t) for d, t in world.rows[name]]


def _scale_from(world: World, name: str, day: date, factor: float) -> None:
    world.rows[name] = [
        (d, t if d < day or t == "." else f"{float(t) * factor:.6f}") for d, t in world.rows[name]
    ]


# --- 76: the spec and the parser ---------------------------------------------------------------


def test_the_real_spec_names_the_three_indices_and_their_sources() -> None:
    spec = oi.load_spec()
    assert spec.owner_decision == DECISION
    assert spec.qlh_snapshot == "qlh-20261013-0b0df53cb1a2"
    assert spec.redistribution_allowed is False and spec.external_llm_allowed is False
    assert [i.id for i in spec.indices] == ["BXN", "NQTRI", "NQCLRI"]
    bxn, nqtri, nqclri = spec.indices
    assert (bxn.first_date, bxn.base_100_on_first_date) == (date(2009, 9, 18), False)
    assert [(s.url, s.date_column, s.value_column) for s in bxn.sources] == [
        (CBOE_BXN, "DATE", "BXN")
    ]
    assert bxn.sources[0].from_date is None and bxn.sources[0].through_date is None
    assert bxn.crosscheck is not None
    assert (bxn.crosscheck.url, bxn.crosscheck.closed_marker) == (FRED.format("NASDAQBXN"), ".")
    for index in (nqtri, nqclri):
        assert (index.first_date, index.base_100_on_first_date) == (date(2009, 6, 19), True)
        first, second = index.sources
        assert (first.url, first.date_column, first.value_column) == (
            NASDAQ.format(index.id),
            "Date",
            index.id,
        )
        assert (first.from_date, first.through_date) == (None, date(2021, 8, 20))
        assert (second.url, second.date_column, second.value_column) == (
            FRED.format("NASDAQ" + index.id),
            "observation_date",
            "NASDAQ" + index.id,
        )
        assert (second.from_date, second.through_date, second.closed_marker) == (
            date(2021, 8, 23),
            None,
            ".",
        )
        assert index.crosscheck is None
    assert {d.name for i in spec.indices for d in i.documents} == {
        "methodology_bxn",
        "methodology_nqtri",
        "methodology_nqclri",
    }


def _bad(raw: dict[str, Any], case: str) -> None:
    if case == "order":
        raw["indices"] = [raw["indices"][1], raw["indices"][0], raw["indices"][2]]
    elif case == "missing_index":
        raw["indices"] = raw["indices"][:2]
    elif case == "open_first_part":
        del raw["indices"][1]["sources"][0]["through_date"]
    elif case == "open_second_part":
        del raw["indices"][1]["sources"][1]["from_date"]
    elif case == "overlap":
        raw["indices"][1]["sources"][1]["from_date"] = "2021-08-20"
    elif case == "single_with_dates":
        raw["indices"][0]["sources"][0]["from_date"] = "2010-01-04"
    elif case == "crosscheck_with_dates":
        raw["indices"][0]["crosscheck"]["through_date"] = "2022-01-03"
    elif case == "repeated_name":
        raw["indices"][2]["sources"][1]["name"] = "nqtri_fred"
    elif case == "redistribution":
        raw["redistribution_allowed"] = True
    elif case == "external_llm":
        raw["external_llm_allowed"] = True
    elif case == "decision":
        raw["owner_decision"] = "TRADING-2567 option index first look"
    elif case == "plain_http":
        raw["indices"][0]["sources"][0]["url"] = "http://cdn.cboe.com/BXN_History.csv"
    elif case == "extra_field":
        raw["indices"][0]["note"] = "anything"


@pytest.mark.parametrize(
    "case",
    [
        "order",
        "missing_index",
        "open_first_part",
        "open_second_part",
        "overlap",
        "single_with_dates",
        "crosscheck_with_dates",
        "repeated_name",
        "redistribution",
        "external_llm",
        "decision",
        "plain_http",
        "extra_field",
    ],
)
def test_a_spec_of_another_shape_is_refused(case: str) -> None:
    raw = oi.load_spec().model_dump(mode="json")
    oi.OptionIndexSpec.model_validate(raw)
    _bad(raw, case)
    with pytest.raises(ValidationError):
        oi.OptionIndexSpec.model_validate(raw)


def _source(**changes: Any) -> oi.SourceSpec:
    base: dict[str, Any] = {
        "name": "nqtri_fred",
        "url": "https://example.test/x.csv",
        "date_column": "observation_date",
        "value_column": "V",
        "closed_marker": ".",
    }
    return oi.SourceSpec.model_validate({**base, **changes})


def test_files_are_read_by_header_name_in_file_order() -> None:
    payload = (
        '﻿extra,V,observation_date\na,"1,234.5",2021-08-24\nb,.,8/23/2021\nc,99.25,2021-08-25\n\n'
    ).encode()
    frame = oi.parse_csv(payload, _source())
    assert list(frame["date"]) == [
        pd.Timestamp("2021-08-24"),
        pd.Timestamp("2021-08-23"),
        pd.Timestamp("2021-08-25"),
    ]
    assert frame["value"].iloc[0] == 1234.5
    assert np.isnan(frame["value"].iloc[1])
    assert frame["value"].iloc[2] == 99.25
    without_marker = oi.parse_csv(b"observation_date,V\n2021-08-23,100\n", _source())
    assert list(without_marker["value"]) == [100.0]


@pytest.mark.parametrize(
    ("payload", "words"),
    [
        (b"", "empty"),
        (b"Date,Other\n2021-08-23,98765.4321\n", "column"),
        (b"observation_date,V\n2021-13-45,98765.4321\n", "row 2"),
        (b"observation_date,V\n2021-08-23,1.0\n2021-08-24,98765.4321x\n", "row 3"),
        (b"observation_date,V\n2021-08-23,inf\n", "row 2"),
        (b"observation_date,V\n2021-08-23,NA98765.4321\n", "row 2"),
        (b"observation_date,V\n2021-08-23,NA\n", "row 2"),
        (b"observation_date,V\n2021-08-23,..\n", "row 2"),
        (b"observation_date,V\n2021-08-23,#N/A\n", "row 2"),
        (b"observation_date,V\n2021/08/23,98765.4321\n", "row 2"),
        (b"observation_date,V\n23.08.2021,98765.4321\n", "row 2"),
        (b"observation_date,V\n23-Aug-21,98765.4321\n", "row 2"),
        (b"observation_date,V\n23-Agu-2021,98765.4321\n", "row 2"),
        (b"observation_date,V\n08/23/202,98765.4321\n", "row 2"),
        (b"observation_date,V\n2021-08-23\n", "row 2"),
        (b"observation_date,V\n", "row"),
        # TRADING-2567-43: a file that is not UTF-8 is read as Windows-1252, else as Latin-1, and
        # its header names, dates and values are still checked exactly.
        pytest.param(
            "observation_date,V\n2021-08-23,98765é".encode("latin-1"), "row 2", id="e-acute"
        ),
        pytest.param(b"observation_date,V\xae\n2021-08-23,98765.4321\n", "column", id="header-r"),
        pytest.param(b"observation_date,V\n2021-08-23,98765\xae\n", "row 2", id="value-r"),
        pytest.param(b"observation_date,V\n2021-08-23,98\xa0765.4321\n", "row 2", id="value-nbsp"),
        pytest.param(b"observation_date,V\n2021\x9608\x9623,98765.4321\n", "row 2", id="date-dash"),
        pytest.param(b"observation_date,V\n2021-08-23,98765.4321\x85\n", "row 2", id="value-dots"),
        pytest.param(b"observation_date,V\n2021-08-23,98765.4321\x81\n", "row 2", id="value-x81"),
        pytest.param(
            "observation_date,V\n2021-08-23,98765.4321\n".encode("utf-16"), "column", id="utf-16"
        ),
    ],
)
def test_a_bad_file_names_the_file_and_row_never_the_cell(payload: bytes, words: str) -> None:
    with pytest.raises(ValueError) as caught:
        oi.parse_csv(payload, _source())
    message = str(caught.value)
    assert "nqtri_fred" in message and words in message
    assert "98765" not in message and "2021-13-45" not in message


def test_a_closed_marker_is_a_value_error_in_a_file_without_one() -> None:
    plain = _source(closed_marker=None, date_column="Date", value_column="NQTRI")
    with pytest.raises(ValueError, match="row 2"):
        oi.parse_csv(b"Date,NQTRI\n08/20/2021,.\n", plain)


def test_a_closed_day_is_a_dot_or_an_empty_value_in_a_file_with_a_marker() -> None:
    # TRADING-2567-42: FRED's current file leaves a closed day empty; "." still reads as closed.
    payload = (
        b"observation_date,V\n2021-08-23,100\n2021-08-24,\n2021-08-25,.\n"
        b'2021-08-26, \n2021-08-27,""\n2021-08-30,101.5\n'
    )
    frame = oi.parse_csv(payload, _source())
    assert [d.date() for d in frame["date"]] == [
        date(2021, 8, 23),
        date(2021, 8, 24),
        date(2021, 8, 25),
        date(2021, 8, 26),
        date(2021, 8, 27),
        date(2021, 8, 30),
    ]
    values = frame["value"].to_numpy()
    assert values[0] == 100.0 and values[5] == 101.5
    assert np.isnan(values[1:5]).all()


@pytest.mark.parametrize("cell", ["", " ", '""'])
def test_an_empty_value_is_an_error_in_a_file_without_a_marker(cell: str) -> None:
    plain = _source(closed_marker=None, date_column="Date", value_column="NQTRI")
    with pytest.raises(ValueError, match="row 3"):
        oi.parse_csv(f"Date,NQTRI\n08/19/2021,100\n08/20/2021,{cell}\n".encode(), plain)


def test_dates_are_read_in_the_four_forms_the_probe_parsed() -> None:
    # The read-only probe parsed every dated row with YYYY-MM-DD, M/D/YYYY, M/D/YY or D-Mon-YYYY;
    # it did not print which form each file uses, so all four are read (TRADING-2567-42).
    payload = (
        b"Date,NQTRI\n06/19/2009,100\n6/22/2009,101\n06/23/09,102\n6/24/09,103\n"
        b"25-Jun-2009,104\n26-JUN-2009,105\n29-jun-2009,106\n2009-06-30,107\n1-Jul-2009,108\n"
    )
    plain = _source(closed_marker=None, date_column="Date", value_column="NQTRI")
    frame = oi.parse_csv(payload, plain)
    assert [d.date() for d in frame["date"]] == [
        date(2009, 6, 19),
        date(2009, 6, 22),
        date(2009, 6, 23),
        date(2009, 6, 24),
        date(2009, 6, 25),
        date(2009, 6, 26),
        date(2009, 6, 29),
        date(2009, 6, 30),
        date(2009, 7, 1),
    ]
    assert frame["value"].tolist() == [100.0 + i for i in range(9)]


def test_a_file_that_is_not_utf8_is_read_as_windows_1252_then_latin_1() -> None:
    # TRADING-2567-43: the second fetch stopped at "nqtri_nasdaq: not UTF-8 text" (2026-11-24). The
    # probe read that file as Latin-1 and found a header and a dated, numeric value on every row, so
    # the bytes that are not UTF-8 sit around the cells it read or in a column it did not read.
    plain = _source(closed_marker=None, date_column="Date", value_column="NQTRI")
    expected = [date(2009, 6, 19), date(2009, 6, 22), date(2009, 6, 23)]
    payloads = [
        # Windows-1252: a no-break space around a header name and a value, quotes, a dash, a mark.
        b"Date\xa0,NQTRI ,Note \x96 Nasdaq\xae\r\n06/19/2009,100,\x93a\x94\r\n"
        b"06/22/2009,\xa0101.25\xa0,caf\xe9\r\n06/23/2009,102,\x80\r\n",
        # A byte Windows-1252 leaves undefined: the file is read as Latin-1.
        b"Date,NQTRI,Note\n06/19/2009,100,\x81\n06/22/2009,101.25,\x9d\n06/23/2009,102,\xff\n",
        # A byte-order mark goes whatever the encoding turns out to be.
        b"\xef\xbb\xbfDate,NQTRI,Note\n06/19/2009,100,\x93\n06/22/2009,101.25,\n06/23/2009,102,\n",
        # UTF-8 stays UTF-8: its no-break space (two bytes) is trimmed like a space.
        "Date,NQTRI\u00a0,Note \u2013\n06/19/2009,100,\u00e9\n06/22/2009,\u00a0101.25\u00a0,\n"
        "06/23/2009,102,\n".encode(),
    ]
    for payload in payloads:
        frame = oi.parse_csv(payload, plain)
        assert [d.date() for d in frame["date"]] == expected
        assert frame["value"].tolist() == [100.0, 101.25, 102.0]


def test_spaces_after_commas_and_iso_dates_without_zeros_are_read() -> None:
    # TRADING-2567-43: read cells as the probe did, which split on commas, trimmed each cell and
    # parsed YYYY-M-D; a quoted value after ", " stays one cell.
    plain = _source(closed_marker=None, date_column="Date", value_column="NQTRI")
    payload = (
        b'Date, NQTRI, Note\n06/19/2009, "1,234.5", x\n2009-6-22, 101\n2009-06-23 , 102 \n'
        b"2009-7-1,103\n"
    )
    frame = oi.parse_csv(payload, plain)
    assert [d.date() for d in frame["date"]] == [
        date(2009, 6, 19),
        date(2009, 6, 22),
        date(2009, 6, 23),
        date(2009, 7, 1),
    ]
    assert frame["value"].tolist() == [1234.5, 101.0, 102.0, 103.0]
    for bad in (
        b"Date,NQTRI\n2009-6-31,1\n",
        b"Date,NQTRI\n2009-123-1,1\n",
        b"Date,NQTRI\n09-6-1,1\n",
    ):
        with pytest.raises(ValueError, match="row 2"):
            oi.parse_csv(bad, plain)


# --- 77: fetch -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [None, "nothing here\n", f"{DECISION}_v2 is another decision\n", "option_index_first_look\n"],
)
def test_the_fetch_needs_the_decision_in_the_requirements(tmp_path: Path, text: str | None) -> None:
    world = make_world()
    if text is not None:
        _write_decision(tmp_path, text)
    get = FakeGet(world.payloads())
    with pytest.raises(ValueError, match="owner decision"):
        oi.fetch_snapshot(world.spec, tmp_path, get=get)
    assert get.calls == []
    assert _files(tmp_path) == []


@pytest.mark.parametrize("case", ["download", "parse", "header"])
def test_a_fetch_writes_nothing_unless_every_data_file_works(tmp_path: Path, case: str) -> None:
    world = make_world()
    failing: tuple[str, ...] = ()
    if case == "download":
        failing = (FRED.format("NASDAQNQCLRI"),)
    elif case == "parse":
        _replace(world, "nqtri_nasdaq", date(2021, 6, 1), "n/a")
    else:
        # The cross-check file lacks the column the spec names.
        world.styles["bxn_fred"] = "iso"
        raw = world.spec.model_dump(mode="json")
        raw["indices"][0]["crosscheck"]["value_column"] = "NASDAQBXN_X"
        payloads = world.payloads()
        world.spec = oi.OptionIndexSpec.model_validate(raw)
        _write_decision(tmp_path)
        with pytest.raises(ValueError, match="bxn_fred"):
            oi.fetch_snapshot(world.spec, tmp_path, get=FakeGet(payloads))
        assert _files(tmp_path) == []
        return
    with pytest.raises((ConnectionError, ValueError)):
        _fetch(tmp_path, world, failing)
    assert _files(tmp_path) == []


def _manifest(root: Path, snapshot: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(
        (oi.snapshot_dir(root, snapshot) / "manifest.json").read_text()
    )
    return data


def test_the_snapshot_id_is_the_manifest_content_address(tmp_path: Path) -> None:
    world = make_world()
    first = _fetch(tmp_path / "a", world)
    again = _fetch(tmp_path / "b", world)
    assert first == again
    assert first.startswith("oix-") and len(first) == 16
    manifest = _manifest(tmp_path / "a", first)
    body = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert first == "oix-" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
    assert oi.latest_snapshot_id(tmp_path / "a") == first
    payloads = world.payloads()
    sources = {s.name: s for s in oi.all_sources(world.spec)}
    entries = {e["name"]: e for e in manifest["files"]}
    assert set(entries) == {f"raw/{n}.csv" for n in sources} | {
        f"documents/{d.name}.pdf" for i in world.spec.indices for d in i.documents
    }
    for name, source in sources.items():
        entry = entries[f"raw/{name}.csv"]
        payload = payloads[source.url]
        assert entry["sha256"] == hashlib.sha256(payload).hexdigest()
        assert entry["bytes"] == len(payload)
        assert entry["rows"] == len(world.rows[name])
        on_disk = oi.snapshot_dir(tmp_path / "a", first) / f"raw/{name}.csv"
        assert on_disk.read_bytes() == payload
    changed = make_world()
    day = date(2021, 10, 1)
    _replace(changed, "nqclri_fred", day, _value(changed, "nqclri_fred", day) + "1")
    assert _fetch(tmp_path / "c", changed) != first


def test_a_failed_document_is_recorded_and_warned_not_fatal(tmp_path: Path) -> None:
    world = make_world()
    world.documents["methodology_nqtri"] = None
    report = _check(tmp_path, world)
    snapshot = report["snapshot_id"]
    fetch = json.loads((oi.snapshot_dir(tmp_path, snapshot) / "fetch.json").read_text())
    assert [item["name"] for item in fetch["documents_missing"]] == ["methodology_nqtri"]
    assert report["status"] == "WARN"
    assert report["failed"] == [] and report["warned"] == ["documents"]
    assert report["documents"]["missing"] == ["methodology_nqtri"]
    assert sorted(report["documents"]["kept"]) == ["methodology_bxn", "methodology_nqclri"]


def test_the_fetch_prints_and_stores_no_value_outside_the_raw_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    world = make_world()
    snapshot = _fetch(tmp_path, world)
    printed = capsys.readouterr().out
    assert snapshot in printed
    assert "4321" not in printed and "100.0" not in printed
    directory = oi.snapshot_dir(tmp_path, snapshot)
    for name in ("manifest.json", "fetch.json"):
        assert "4321" not in (directory / name).read_text(encoding="utf-8")


# --- 78: check and load ----------------------------------------------------------------------


def test_a_clean_snapshot_passes_and_loads_spliced(tmp_path: Path) -> None:
    world = make_world()
    report = _check(tmp_path, world)
    assert (report["status"], report["failed"], report["warned"]) == ("PASS", [], [])
    for index_id in ("NQTRI", "NQCLRI"):
        assert report["indices"][index_id]["seam"]["dates"] == "PASS"
        assert report["indices"][index_id]["seam"]["residual"] == "PASS"
        assert report["indices"][index_id]["seam"]["status"] == "PASS"
        assert report["indices"][index_id]["base"] == "PASS"
    crosscheck = report["indices"]["BXN"]["crosscheck"]
    assert crosscheck["over_tolerance_days"] == 0 and crosscheck["overlap_days"] > 100
    loaded = oi.load_snapshot(tmp_path, report["snapshot_id"])
    assert loaded.snapshot_id == report["snapshot_id"]
    assert loaded.check["status"] == "PASS"
    assert list(loaded.levels) == ["BXN", "NQTRI", "NQCLRI"]
    bxn = loaded.levels["BXN"]
    assert [d.date() for d in bxn.index] == [d for d, _ in world.rows["bxn_cboe"]]
    assert bxn.iloc[0] == pytest.approx(LEAK_BASE)
    for index_id, (csv_name, fred_name) in SPLIT.items():
        series = loaded.levels[index_id]
        expected = [(d, float(t)) for d, t in world.rows[csv_name]] + [
            (d, float(t)) for d, t in world.rows[fred_name] if t != "."
        ]
        assert [d.date() for d in series.index] == [d for d, _ in expected]
        assert series.to_numpy().tolist() == [v for _, v in expected]
        assert series.index.is_monotonic_increasing and not series.index.has_duplicates


def test_empty_fred_closed_days_and_other_date_forms_give_the_same_check(tmp_path: Path) -> None:
    # TRADING-2567-42: the same world with FRED's closed days left empty and the Cboe and Nasdaq
    # dates in the probe's other forms checks and loads exactly like the world written with ".".
    dotted = make_world()
    expected = _check(tmp_path / "dot", dotted)
    world = make_world()
    for name in FRED_NAMES:
        assert any(t == "." for _, t in world.rows[name])
        world.rows[name] = [(d, "" if t == "." else t) for d, t in world.rows[name]]
    world.styles.update(bxn_cboe="us2", nqtri_nasdaq="mon", nqclri_nasdaq="us2")
    report = _check(tmp_path / "empty", world)
    assert (report["status"], report["failed"], report["warned"]) == ("PASS", [], [])
    assert report["snapshot_id"] != expected["snapshot_id"]
    assert {k: v for k, v in report.items() if k != "snapshot_id"} == {
        k: v for k, v in expected.items() if k != "snapshot_id"
    }
    first = oi.load_snapshot(tmp_path / "dot", expected["snapshot_id"])
    second = oi.load_snapshot(tmp_path / "empty", report["snapshot_id"])
    for index_id in oi.INDEX_IDS:
        pd.testing.assert_series_equal(first.levels[index_id], second.levels[index_id])


def test_windows_1252_nasdaq_files_with_another_column_give_the_same_check(tmp_path: Path) -> None:
    # TRADING-2567-43: the same world with both Nasdaq files written in Windows-1252, with CRLF
    # lines, padded header names and a third column, checks and loads exactly like the UTF-8 one;
    # the snapshot keeps the downloaded bytes.
    plain = make_world()
    expected = _check(tmp_path / "utf8", plain)
    world = make_world()
    payloads = world.payloads()
    for name in ("nqtri_nasdaq", "nqclri_nasdaq"):
        url = world.source(name).url
        lines = payloads[url].decode("ascii").splitlines()
        marked = [lines[0].replace(",", "\u00a0, ") + ",Note \u2013 Nasdaq\u00ae"]
        marked += [f"{line},\u201c{i}\u201d" for i, line in enumerate(lines[1:])]
        payloads[url] = ("\r\n".join(marked) + "\r\n").encode("cp1252")
        with pytest.raises(UnicodeDecodeError):
            payloads[url].decode("utf-8")
    root = tmp_path / "cp1252"
    _write_decision(root)
    snapshot = oi.fetch_snapshot(world.spec, root, get=FakeGet(payloads))
    directory = oi.snapshot_dir(root, snapshot)
    for name in ("nqtri_nasdaq", "nqclri_nasdaq"):
        assert (directory / f"raw/{name}.csv").read_bytes() == payloads[world.source(name).url]
    report = oi.check_snapshot(root, snapshot, world.reference)
    assert (report["status"], report["failed"], report["warned"]) == ("PASS", [], [])
    assert report["snapshot_id"] != expected["snapshot_id"]
    assert {k: v for k, v in report.items() if k != "snapshot_id"} == {
        k: v for k, v in expected.items() if k != "snapshot_id"
    }
    first = oi.load_snapshot(tmp_path / "utf8", expected["snapshot_id"])
    second = oi.load_snapshot(root, snapshot)
    for index_id in oi.INDEX_IDS:
        pd.testing.assert_series_equal(first.levels[index_id], second.levels[index_id])


def test_the_parts_are_cut_at_the_spec_dates_even_when_the_files_overlap(tmp_path: Path) -> None:
    world = make_world()
    fred = world.rows["nqtri_fred"]
    # Nasdaq's file runs on past the seam and FRED's starts before it; neither extra row is used.
    world.rows["nqtri_nasdaq"].append((date(2021, 8, 23), "1.5"))
    world.rows["nqtri_fred"] = [(date(2021, 8, 20), "2.5"), *fred]
    report = _check(tmp_path, world)
    assert report["indices"]["NQTRI"]["seam"]["status"] == "PASS"
    assert "NQTRI:order" not in report["failed"]
    series = oi.load_snapshot(tmp_path, report["snapshot_id"]).levels["NQTRI"]
    assert 1.5 not in series.to_numpy() and 2.5 not in series.to_numpy()


@pytest.mark.parametrize(
    ("case", "dates", "residual"),
    [
        ("fred_starts_late", "FAIL", "FAIL"),
        ("csv_ends_early", "FAIL", "FAIL"),
        ("seam_jump", "PASS", "FAIL"),
        ("small_seam_move", "PASS", "PASS"),
        ("trading_day_between", "FAIL", "FAIL"),
    ],
)
def test_the_seam_checks_the_next_trading_day_and_the_seam_day_return(
    tmp_path: Path, case: str, dates: str, residual: str
) -> None:
    # A spec whose parts leave out a trading day: Nasdaq's file through 2021-08-19, FRED's from
    # 2021-08-23, while the QQQ calendar trades on 2021-08-20.
    world = make_world(through=date(2021, 8, 19)) if case == "trading_day_between" else make_world()
    seam = date(2021, 8, 23)
    if case == "fred_starts_late":
        world.rows["nqclri_fred"] = [(d, t) for d, t in world.rows["nqclri_fred"] if d != seam]
    elif case == "csv_ends_early":
        world.rows["nqclri_nasdaq"] = world.rows["nqclri_nasdaq"][:-1]
    elif case == "seam_jump":
        _scale_from(world, "nqclri_fred", seam, 1.02)
    elif case == "small_seam_move":
        _scale_from(world, "nqclri_fred", seam, 1.01)
    report = _check(tmp_path, world)
    seam_report = report["indices"]["NQCLRI"]["seam"]
    assert (seam_report["dates"], seam_report["residual"]) == (dates, residual)
    assert set(seam_report.values()) <= {"PASS", "FAIL"} | {
        None,
        "2021-08-18",
        "2021-08-19",
        "2021-08-20",
        "2021-08-23",
        "2021-08-24",
    }
    failed = dates == "FAIL" or residual == "FAIL"
    assert ("NQCLRI:seam" in report["failed"]) is failed
    other = "FAIL" if case == "trading_day_between" else "PASS"
    assert report["indices"]["NQTRI"]["seam"]["status"] == other


def _fault(world: World, case: str) -> None:
    rows = world.rows
    if case == "first_date":
        rows["nqtri_nasdaq"] = [(d, t) for d, t in rows["nqtri_nasdaq"] if d > date(2021, 5, 27)]
    elif case == "order_repeat":
        rows["bxn_cboe"].insert(10, rows["bxn_cboe"][10])
    elif case == "order_swap":
        rows["nqtri_fred"][5], rows["nqtri_fred"][6] = rows["nqtri_fred"][6], rows["nqtri_fred"][5]
    elif case == "nonpositive":
        _replace(world, "nqclri_nasdaq", date(2021, 7, 1), "0")
    elif case == "move":
        _replace(world, "bxn_cboe", date(2021, 9, 15), f"{LEAK_BASE * 1.4:.6f}")
    elif case == "gap":
        gap = (date(2021, 7, 12), date(2021, 7, 16))
        rows["bxn_cboe"] = [(d, t) for d, t in rows["bxn_cboe"] if not gap[0] <= d <= gap[1]]
    elif case == "ends_early":
        rows["bxn_cboe"] = [(d, t) for d, t in rows["bxn_cboe"] if d <= date(2021, 10, 27)]


@pytest.mark.parametrize(
    ("case", "label"),
    [
        ("first_date", "NQTRI:first_date"),
        ("order_repeat", "BXN:order"),
        ("order_swap", "NQTRI:order"),
        ("nonpositive", "NQCLRI:nonpositive"),
        ("move", "BXN:move"),
        ("gap", "BXN:gap"),
        ("ends_early", "BXN:ends_early"),
    ],
)
def test_each_data_fault_fails_the_check(tmp_path: Path, case: str, label: str) -> None:
    world = make_world()
    _fault(world, case)
    report = _check(tmp_path, world)
    assert report["status"] == "FAIL"
    assert label in report["failed"]
    with pytest.raises(ValueError):
        oi.load_snapshot(tmp_path, report["snapshot_id"])


def test_the_fault_details_are_dates_and_counts(tmp_path: Path) -> None:
    world = make_world()
    _fault(world, "move")
    _fault(world, "gap")
    summary = _check(tmp_path, world)["indices"]["BXN"]
    assert summary["large_moves"] == 2
    assert summary["large_move_dates"] == ["2021-09-15", "2021-09-16"]
    assert summary["long_gaps"] == 1
    assert summary["long_gap_dates"] == ["2021-07-09..2021-07-19"]


def test_the_known_2001_closure_is_not_a_gap(tmp_path: Path) -> None:
    closed = tuple(date(2001, 9, d) for d in (3, 11, 12, 13, 14)) + (date(2001, 11, 22),)
    world = make_world(
        start=date(2001, 8, 1),
        end=date(2001, 12, 28),
        holidays=closed,
        first=date(2001, 8, 17),
        through=date(2001, 10, 19),
        seam_start=date(2001, 10, 22),
        data_end=date(2002, 1, 4),
        crosscheck_from=date(2001, 9, 4),
    )
    report = _check(tmp_path, world)
    assert report["status"] == "PASS", report["failed"]
    assert report["indices"]["BXN"]["long_gaps"] == 0
    longer = tuple(date(2001, 9, d) for d in (3, 10, 11, 12, 13, 14)) + (date(2001, 11, 22),)
    other = make_world(
        start=date(2001, 8, 1),
        end=date(2001, 12, 28),
        holidays=longer,
        first=date(2001, 8, 17),
        through=date(2001, 10, 19),
        seam_start=date(2001, 10, 22),
        data_end=date(2002, 1, 4),
        crosscheck_from=date(2001, 9, 4),
    )
    assert "BXN:gap" in _check(tmp_path / "other", other)["failed"]


def test_a_changed_or_missing_file_or_a_broken_manifest_fails(tmp_path: Path) -> None:
    world = make_world()
    snapshot = _fetch(tmp_path, world)
    directory = oi.snapshot_dir(tmp_path, snapshot)
    path = directory / "raw" / "nqtri_fred.csv"
    original = path.read_bytes()
    path.write_bytes(original.replace(b"2021-09-01", b"2021-09-02", 1))
    report = oi.check_snapshot(tmp_path, snapshot, world.reference)
    assert report["status"] == "FAIL" and "files" in report["failed"]
    assert report["file_mismatches"] == ["raw/nqtri_fred.csv"]
    path.unlink()
    assert "files" in oi.check_snapshot(tmp_path, snapshot, world.reference)["failed"]
    path.write_bytes(original)
    assert oi.check_snapshot(tmp_path, snapshot, world.reference)["status"] == "PASS"
    manifest = directory / "manifest.json"
    tampered = json.loads(manifest.read_text())
    tampered["files"][0]["bytes"] += 1
    manifest.write_text(json.dumps(tampered))
    report = oi.check_snapshot(tmp_path, snapshot, world.reference)
    assert report["status"] == "FAIL" and report["failed"] == ["manifest"]


def test_an_unparsable_raw_file_fails_the_check_for_its_index(tmp_path: Path) -> None:
    world = make_world()
    snapshot = _fetch(tmp_path, world)
    directory = oi.snapshot_dir(tmp_path, snapshot)
    manifest = _manifest(tmp_path, snapshot)
    garbage = b"Date,NQCLRI\nnot a date,1\n"
    # Rewrite the file and its manifest entry consistently, so only the parse can catch it.
    for entry in manifest["files"]:
        if entry["name"] == "raw/nqclri_nasdaq.csv":
            entry.update(sha256=hashlib.sha256(garbage).hexdigest(), bytes=len(garbage))
    body = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    other = "oix-" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:12]
    target = oi.snapshot_dir(tmp_path, other)
    (target / "raw").mkdir(parents=True)
    for source in oi.all_sources(world.spec):
        name = f"raw/{source.name}.csv"
        payload = garbage if source.name == "nqclri_nasdaq" else (directory / name).read_bytes()
        (target / name).write_bytes(payload)
    (target / "documents").mkdir()
    for item in (directory / "documents").iterdir():
        (target / "documents" / item.name).write_bytes(item.read_bytes())
    (target / "manifest.json").write_text(body + "\n")
    report = oi.check_snapshot(tmp_path, other, world.reference)
    assert report["status"] == "FAIL"
    assert report["failed"] == ["NQCLRI:parse"]
    assert report["unreadable"] == ["nqclri_nasdaq"]


def _warning(world: World, case: str) -> None:
    if case == "closed_days":
        _replace(world, "nqtri_fred", date(2021, 9, 30), ".")
    elif case == "calendar_missing":
        world.rows["nqclri_nasdaq"] = [
            (d, t) for d, t in world.rows["nqclri_nasdaq"] if d != date(2021, 6, 15)
        ]
    elif case == "calendar_extra":
        rows = world.rows["bxn_cboe"]
        at = next(i for i, (d, _) in enumerate(rows) if d > date(2021, 7, 5))
        rows.insert(at, (date(2021, 7, 5), rows[at - 1][1]))
    elif case == "base":
        _scale_from(world, "nqtri_nasdaq", date(2021, 5, 21), 1.001)
        _scale_from(world, "nqtri_fred", date(2021, 8, 23), 1.001)
    elif case == "crosscheck":
        day = date(2021, 9, 1)
        _replace(world, "bxn_fred", day, f"{float(_value(world, 'bxn_fred', day)) * 1.001:.6f}")


@pytest.mark.parametrize(
    ("case", "labels", "key", "listed"),
    [
        # A closed-day marker on a trading day also leaves the index without that day.
        (
            "closed_days",
            ["NQTRI:calendar", "NQTRI:closed_days"],
            "closed_on_trading_dates",
            ["2021-09-30"],
        ),
        ("calendar_missing", ["NQCLRI:calendar"], "missing_calendar_dates", ["2021-06-15"]),
        ("calendar_extra", ["BXN:calendar"], "off_calendar_dates", ["2021-07-05"]),
        ("base", ["NQTRI:base"], "base", "WARN"),
        ("crosscheck", ["BXN:crosscheck"], "crosscheck", {"over_tolerance_days": 1}),
    ],
)
def test_each_warning_warns_without_failing(
    tmp_path: Path, case: str, labels: list[str], key: str, listed: Any
) -> None:
    world = make_world()
    _warning(world, case)
    report = _check(tmp_path, world)
    assert (report["status"], report["failed"], report["warned"]) == ("WARN", [], labels)
    found = report["indices"][labels[0].split(":")[0]][key]
    if isinstance(listed, dict):
        assert {k: found[k] for k in listed} == listed
    else:
        assert found == listed
    assert oi.load_snapshot(tmp_path, report["snapshot_id"]).check["status"] == "WARN"


def test_the_check_writes_no_value(tmp_path: Path) -> None:
    world = make_world()
    _warning(world, "crosscheck")
    _fault(world, "move")
    report = _check(tmp_path, world)
    directory = oi.snapshot_dir(tmp_path, report["snapshot_id"])
    written = json.loads((directory / "check.json").read_text(encoding="utf-8"))
    assert written == report
    leaves = list(_walk(written))
    assert not [v for v in leaves if isinstance(v, float)]
    text = (directory / "check.json").read_text(encoding="utf-8") + (
        directory / "check.md"
    ).read_text(encoding="utf-8")
    assert "4321" not in text and "6050" not in text
    assert report["status"] in text


@pytest.mark.parametrize("case", ["unchecked", "failed", "changed", "other_check"])
def test_load_reads_only_a_checked_unchanged_snapshot(tmp_path: Path, case: str) -> None:
    world = make_world()
    if case == "failed":
        _fault(world, "nonpositive")
    snapshot = _fetch(tmp_path, world)
    directory = oi.snapshot_dir(tmp_path, snapshot)
    if case != "unchecked":
        oi.check_snapshot(tmp_path, snapshot, world.reference)
    if case == "changed":
        path = directory / "raw" / "bxn_cboe.csv"
        path.write_bytes(path.read_bytes() + b"\n")
    if case == "other_check":
        check = json.loads((directory / "check.json").read_text())
        check["snapshot_id"] = "oix-000000000000"
        (directory / "check.json").write_text(json.dumps(check))
    with pytest.raises(ValueError):
        oi.load_snapshot(tmp_path, snapshot)


@pytest.mark.parametrize(
    "snapshot", ["latest", "oix-12", "oix-ABCDEF123456", "../oix-0123456789ab"]
)
def test_a_snapshot_id_must_be_a_content_address(tmp_path: Path, snapshot: str) -> None:
    with pytest.raises(ValueError):
        oi.snapshot_dir(tmp_path, snapshot)


def _fake_qlh(
    monkeypatch: pytest.MonkeyPatch, world: World, directory: Path, calls: list[tuple[Any, ...]]
) -> None:
    def resolve(root: Path, snapshot_id: str | None = None) -> Path:
        calls.append(("resolve", root, snapshot_id))
        return directory

    def load_daily(found: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        calls.append(("load", found))
        return pd.DataFrame({"ndx_tr_return": world.reference}), {"status": "PASS"}

    monkeypatch.setattr(qlh, "resolve_snapshot_dir", resolve)
    monkeypatch.setattr(qlh, "load_daily", load_daily)


def test_the_commands_print_only_summaries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = make_world()
    _write_decision(tmp_path)
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(yaml.safe_dump(world.spec.model_dump(mode="json")), encoding="utf-8")
    get = FakeGet(world.payloads())
    monkeypatch.setattr(oi, "getter_factory", lambda: get)
    calls: list[tuple[Any, ...]] = []
    _fake_qlh(monkeypatch, world, tmp_path / "qlh", calls)
    runner = CliRunner()
    fetched = runner.invoke(oi.app, ["fetch", "--root", str(tmp_path), "--spec", str(spec_path)])
    assert fetched.exit_code == 0, fetched.output
    snapshot = oi.latest_snapshot_id(tmp_path)
    assert snapshot in fetched.output
    assert get.calls and all(url in world.payloads() for url in get.calls)
    checked = runner.invoke(
        oi.app, ["check", "--root", str(tmp_path), "--qlh-root", str(tmp_path / "q")]
    )
    assert checked.exit_code == 0, checked.output
    assert "PASS" in checked.output and snapshot in checked.output
    assert calls == [
        ("resolve", tmp_path / "q", "qlh-20261013-0b0df53cb1a2"),
        ("load", tmp_path / "qlh"),
    ]
    for output in (fetched.output, checked.output):
        assert "4321" not in output
    assert oi.load_snapshot(tmp_path, snapshot).check["status"] == "PASS"


def test_a_failed_check_exits_non_zero_and_a_failed_fetch_explains_without_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = make_world()
    _fault(world, "move")
    snapshot = _fetch(tmp_path, world)
    _fake_qlh(monkeypatch, world, tmp_path, [])
    runner = CliRunner()
    checked = runner.invoke(oi.app, ["check", "--root", str(tmp_path), "--snapshot", snapshot])
    assert checked.exit_code == 1
    assert "FAIL" in checked.output and "BXN:move" in checked.output
    assert "4321" not in checked.output and "6050" not in checked.output
    broken = make_world()
    _replace(broken, "bxn_cboe", date(2021, 6, 1), "98765.4321x")
    monkeypatch.setattr(oi, "getter_factory", lambda: FakeGet(broken.payloads()))
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(yaml.safe_dump(broken.spec.model_dump(mode="json")), encoding="utf-8")
    other = tmp_path / "other"
    _write_decision(other)
    fetched = runner.invoke(oi.app, ["fetch", "--root", str(other), "--spec", str(spec_path)])
    assert fetched.exit_code == 1
    assert "bxn_cboe" in fetched.output and "98765" not in fetched.output
    assert _files(other) == []


def test_the_module_never_reaches_other_data() -> None:
    source = Path(oi.__file__).read_text(encoding="utf-8")
    for word in ("us_market", "ff_factors", "ndx_price_1986_1999", "fmp", "FMP_API_KEY", "openai"):
        assert word not in source
    hosts = {u.split("/")[2] for u in (s.url for s in oi.all_sources(oi.load_spec()))}
    assert hosts == {"cdn.cboe.com", "indexes.nasdaq.com", "fred.stlouisfed.org"}
