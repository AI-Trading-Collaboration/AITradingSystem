"""TRADING-2569-01 acceptance tests: judgment ledger import (judgments_export.v1).

Written by Claude and copied into the repository unchanged. All data here is synthetic; no real
ledger content appears in this file.
"""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT, load_data_sources
from ai_trading_system.research_judgments import importer
from ai_trading_system.research_judgments.__main__ import app
from ai_trading_system.research_judgments.methods import load_methods
from ai_trading_system.research_judgments.schema import Issue, check_export, effective_prob

KNOWN = {"TSM", "NVDA", "SMH"}
METHODS = {"guidance_base_rate", "capex_cycle", "other"}
NOW = datetime(2026, 10, 14, 8, 0, 0, tzinfo=UTC)
EXPORTED = "2026-10-14T07:55:00Z"
SECRET = "SECRET-STATEMENT-4711"


def J(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "a1",
        "ticker": "TSM",
        "statement": "gross margin above 66%",
        "prob": 0.65,
        "deadline": "2026-10-15",
        "kind": "基本面",
        "createdAt": "2026-10-01T06:16:08.523Z",
        "purpose": "research",
        "aiMode": "assisted",
        "methods": ["guidance_base_rate"],
        "status": "open",
        "addenda": [],
    }
    base.update(over)
    return base


MARKET = {
    "benchmark": "SMH",
    "horizon": 20,
    "anchor": "event_prev_close",
    "direction": "outperform",
}


def export(rows: list[dict[str, Any]], exported_at: str = EXPORTED) -> dict[str, Any]:
    return {"format": "judgments_export.v1", "exported_at": exported_at, "judgments": rows}


def check(rows: list[dict[str, Any]], baseline: list[dict[str, Any]] | None = None) -> list[Issue]:
    return check_export(
        export(rows),
        known_tickers=KNOWN,
        method_ids=METHODS,
        baseline=None if baseline is None else {r["id"]: r for r in baseline},
    )


def codes(issues: list[Issue]) -> set[tuple[str, str | None]]:
    return {(i.code, i.field) for i in issues}


# ---------- format and schema ----------


def test_valid_export_has_no_issues_and_keeps_unknown_fields() -> None:
    row = J(qs=["Q1"], weighing={"start": 70, "result": 75}, metaNote="x", fromDraft="d1")
    assert check([row]) == []


@pytest.mark.parametrize(
    "doc",
    [
        {"judgments": []},
        {"format": "judgments_export.v2", "exported_at": EXPORTED, "judgments": []},
        {"format": "judgments_export.v1", "judgments": []},
        {"format": "judgments_export.v1", "exported_at": "yesterday", "judgments": []},
        {"format": "judgments_export.v1", "exported_at": "2026-10-14T07:55:00", "judgments": []},
        {"format": "judgments_export.v1", "exported_at": EXPORTED},
        {"format": "judgments_export.v1", "exported_at": EXPORTED, "judgments": {"a": 1}},
        [],
    ],
)
def test_bad_document_is_a_format_issue(doc: Any) -> None:
    issues = check_export(doc, known_tickers=KNOWN, method_ids=METHODS)
    assert issues and {i.code for i in issues} == {"format"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prob", 0.0),
        ("prob", 1.0),
        ("prob", -0.1),
        ("prob", 1.2),
        ("prob", None),
        ("prob", "high"),
        ("prob", "0.65"),
        ("prob", True),
        ("kind", "other_kind"),
        ("purpose", "fun"),
        ("aiMode", "robot"),
        ("deadline", "2026-13-45"),
        ("deadline", "soon"),
        ("deadline", "2026-10-15T00:00:00Z"),
        ("deadline", 1728000000),
        ("createdAt", "bad"),
        ("createdAt", "2026-10-01T06:16:08"),
        ("createdAt", 1790000000),
        ("statement", ""),
        ("ticker", ""),
        ("status", ""),
        ("methods", "guidance_base_rate"),
        ("methods", ["guidance_base_rate", "guidance_base_rate"]),
        ("aiProb", 1.5),
        ("aiProb", 0),
        ("aiProb", 1.0),
        ("aiProb", "0.4"),
        ("sawSystem", "yes"),
        ("eventTiming", "noon"),
    ],
)
def test_bad_value_is_a_schema_issue_on_that_field(field: str, value: Any) -> None:
    issues = check([J(**{field: value})])
    assert (("schema", field)) in codes(issues)


@pytest.mark.parametrize(
    "field",
    [
        "ticker",
        "statement",
        "prob",
        "deadline",
        "kind",
        "createdAt",
        "purpose",
        "aiMode",
        "methods",
        "status",
    ],
)
def test_missing_required_field_is_a_schema_issue(field: str) -> None:
    row = J()
    del row[field]
    assert ("schema", field) in codes(check([row]))


def test_optional_fields_may_be_null_or_absent() -> None:
    assert check([J(aiProb=None, sawSystem=None, eventId=None, eventTiming=None, addenda=[])]) == []
    row = J()
    del row["addenda"]
    assert check([row]) == []
    assert check([J(aiProb=0.4, sawSystem=False, eventId="e1", eventTiming="amc")]) == []


def test_all_problems_are_reported_not_just_the_first() -> None:
    rows = [J(id="a", prob=2), J(id="b", ticker="XYZ"), J(id="c", methods=["nope"]), J(id="d")]
    issues = check(rows)
    assert {(i.code, i.id) for i in issues} == {
        ("schema", "a"),
        ("unknown_ticker", "b"),
        ("unknown_method", "c"),
    }


def test_duplicate_id_is_reported_once_per_extra_copy() -> None:
    issues = check([J(id="x"), J(id="x"), J(id="y")])
    assert [(i.code, i.id) for i in issues] == [("duplicate_id", "x")]


# ---------- tickers, methods, market ----------


def test_unknown_ticker_is_exact_match() -> None:
    assert ("unknown_ticker", "ticker") in codes(check([J(ticker="XYZ")]))
    assert ("unknown_ticker", "ticker") in codes(check([J(ticker="tsm")]))
    assert check([J(ticker="NVDA")]) == []


def test_unknown_method_names_the_tag() -> None:
    issues = check([J(methods=["guidance_base_rate", "astrology"])])
    assert [(i.code, i.detail) for i in issues] == [("unknown_method", "astrology")]


def test_research_needs_a_method_but_learning_may_have_none() -> None:
    assert ("schema", "methods") in codes(check([J(methods=[])]))
    assert check([J(purpose="learning", methods=[])]) == []


def test_price_and_vol_judgments_need_a_market_block() -> None:
    assert ("market_missing", "market") in codes(check([J(kind="价格")]))
    assert ("market_missing", "market") in codes(check([J(kind="波动", market=None)]))
    assert check([J(kind="价格", market=MARKET)]) == []
    assert check([J(kind="波动", market={**MARKET, "direction": "above"})]) == []
    assert check([J(kind="基本面", market=MARKET)]) == []


@pytest.mark.parametrize(
    "market",
    [
        {**MARKET, "horizon": 0},
        {**MARKET, "horizon": -5},
        {**MARKET, "horizon": 2.5},
        {**MARKET, "horizon": "20"},
        {**MARKET, "anchor": "open"},
        {**MARKET, "direction": "sideways"},
        {**MARKET, "benchmark": ""},
        {k: v for k, v in MARKET.items() if k != "horizon"},
        {**MARKET, "extra": 1},
    ],
)
def test_incomplete_or_invalid_market_block_is_a_schema_issue(market: dict[str, Any]) -> None:
    assert ("schema", "market") in codes(check([J(kind="价格", market=market)]))


def test_direction_must_fit_the_kind() -> None:
    assert ("schema", "market") in codes(
        check([J(kind="价格", market={**MARKET, "direction": "above"})])
    )
    assert ("schema", "market") in codes(
        check([J(kind="波动", market={**MARKET, "direction": "outperform"})])
    )
    assert check([J(kind="价格", market={**MARKET, "direction": "underperform"})]) == []
    assert check([J(kind="波动", market={**MARKET, "direction": "below"})]) == []


# ---------- judgment before event ----------


@pytest.mark.parametrize(
    ("created", "deadline", "ok"),
    [
        ("2026-10-14T23:00:00Z", "2026-10-15", True),
        ("2026-10-15T00:00:00Z", "2026-10-15", False),
        ("2026-10-15T08:00:00Z", "2026-10-15", False),
        ("2026-10-16T01:00:00Z", "2026-10-15", False),
        ("2026-10-15T08:00:00+09:00", "2026-10-15", True),
        ("2026-10-15T00:30:00+09:00", "2026-10-15", True),
        ("2026-10-14T20:00:00-08:00", "2026-10-15", False),
        ("2026-10-14T15:59:59-08:00", "2026-10-15", True),
    ],
)
def test_judgment_must_be_written_on_an_earlier_utc_day_than_the_event(
    created: str, deadline: str, ok: bool
) -> None:
    issues = check([J(createdAt=created, deadline=deadline)])
    assert (issues == []) is ok
    if not ok:
        assert [(i.code, i.field) for i in issues] == [("time_order", "createdAt")]


# ---------- against the previous import ----------


def base_rows() -> list[dict[str, Any]]:
    return [
        J(id="a1", addenda=[{"at": "2026-10-02T00:00:00Z", "rationale": "first"}]),
        J(
            id="p1",
            kind="价格",
            market=MARKET,
            ticker="NVDA",
            aiProb=0.6,
            sawSystem=True,
            methods=["guidance_base_rate", "capex_cycle"],
        ),
    ]


def test_same_export_against_itself_has_no_issues() -> None:
    assert check(base_rows(), base_rows()) == []


@pytest.mark.parametrize(
    ("row", "field", "value"),
    [
        ("a1", "ticker", "NVDA"),
        ("a1", "statement", "gross margin above 67%"),
        ("a1", "prob", 0.7),
        ("a1", "createdAt", "2026-10-02T06:16:08.523Z"),
        ("a1", "deadline", "2026-10-16"),
        ("a1", "methods", ["capex_cycle"]),
        ("a1", "aiMode", "human"),
        ("a1", "purpose", "learning"),
        ("a1", "aiProb", 0.5),
        ("a1", "sawSystem", True),
        ("p1", "market", {**MARKET, "horizon": 10}),
        ("p1", "aiProb", 0.61),
        ("p1", "aiProb", None),
        ("p1", "sawSystem", False),
    ],
)
def test_judgment_fields_cannot_change_between_imports(row: str, field: str, value: Any) -> None:
    new = base_rows()
    target = next(r for r in new if r["id"] == row)
    target[field] = value
    assert ("immutable", field) in codes(check(new, base_rows()))


def test_kind_cannot_change_between_imports() -> None:
    new = base_rows()
    new[0]["kind"] = "价格"
    new[0]["market"] = MARKET
    assert ("immutable", "kind") in codes(check(new, base_rows()))


def test_resolution_notes_and_metadata_may_change() -> None:
    new = base_rows()
    new[0].update(
        status="resolved",
        review="hit",
        errorType="",
        resolvedAt="2026-10-16T00:00:00Z",
        metaNote="x",
    )
    new[0]["weighing"] = {"result": 70}
    new[0]["eventId"] = "e1"
    new[0]["eventTiming"] = "bmo"
    new[0]["addenda"].append({"at": "2026-10-03T00:00:00Z", "rationale": "second"})
    assert check(new, base_rows()) == []


def test_equal_values_in_another_spelling_are_not_changes() -> None:
    new = base_rows()
    new[0]["createdAt"] = "2026-10-01T06:16:08.523+00:00"
    new[0]["methods"] = ["guidance_base_rate"]
    new[1]["methods"] = list(reversed(new[1]["methods"]))
    assert check(new, base_rows()) == []


def test_a_field_absent_before_equals_null_now() -> None:
    old = base_rows()
    old[0].pop("aiProb", None)
    old[0].pop("sawSystem", None)
    new = base_rows()
    new[0]["aiProb"] = None
    new[0]["sawSystem"] = None
    assert check(new, old) == []
    new[0]["aiProb"] = 0.5
    assert ("immutable", "aiProb") in codes(check(new, old))


def test_addenda_are_append_only() -> None:
    edited = base_rows()
    edited[0]["addenda"][0]["rationale"] = "changed"
    assert ("addenda_rewritten", "addenda") in codes(check(edited, base_rows()))
    removed = base_rows()
    removed[0]["addenda"] = []
    assert ("addenda_rewritten", "addenda") in codes(check(removed, base_rows()))
    old = copy.deepcopy(base_rows())
    old[0]["addenda"].append({"at": "2026-10-03T00:00:00Z", "rationale": "second"})
    reordered = copy.deepcopy(old)
    reordered[0]["addenda"].reverse()
    assert ("addenda_rewritten", "addenda") in codes(check(reordered, old))


def test_a_judgment_cannot_disappear_but_new_ones_may_appear() -> None:
    assert [(i.code, i.id) for i in check(base_rows()[:1], base_rows())] == [("disappeared", "p1")]
    assert check([*base_rows(), J(id="n1", ticker="SMH")], base_rows()) == []


# ---------- the importer ----------


def write_export(
    path: Path, rows: list[dict[str, Any]], exported_at: str = EXPORTED, newline: str = "\n"
) -> bytes:
    text = json.dumps(export(rows, exported_at), ensure_ascii=False, indent=1).replace(
        "\n", newline
    )
    data = text.encode("utf-8")
    path.write_bytes(data)
    return data


def run_import(path: Path, data_dir: Path, **kw: Any) -> importer.ImportResult:
    return importer.import_export(
        path, data_dir=data_dir, known_tickers=KNOWN, method_ids=METHODS, now=NOW, **kw
    )


def log_lines(data_dir: Path) -> list[str]:
    return (data_dir / "imports.jsonl").read_bytes().decode("utf-8").split("\n")[:-1]


def tree(data_dir: Path) -> list[str]:
    return (
        sorted(str(p.relative_to(data_dir)) for p in data_dir.rglob("*"))
        if data_dir.exists()
        else []
    )


def test_first_import_copies_the_file_and_starts_the_log(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    raw = write_export(
        src,
        [J(id="a1"), J(id="l1", purpose="learning", methods=["other"]), J(id="b1", ticker="NVDA")],
    )
    data_dir = tmp_path / "data"
    result = run_import(src, data_dir)
    sha = hashlib.sha256(raw).hexdigest()
    assert (result.status, result.seq, result.issues, result.export_sha256) == (
        "imported",
        1,
        [],
        sha,
    )
    assert result.counts == {"rows": 3, "research": 2, "learning": 1}
    name = f"judgments_20261014T075500Z_{sha[:8]}.json"
    assert (data_dir / "exports" / name).read_bytes() == raw
    assert src.read_bytes() == raw
    raw_log = (data_dir / "imports.jsonl").read_bytes()
    assert raw_log.endswith(b"\n") and b"\r" not in raw_log and raw_log.count(b"\n") == 1
    record = json.loads(raw_log)
    assert record == {
        "seq": 1,
        "imported_at": "2026-10-14T08:00:00Z",
        "file": name,
        "export_sha256": sha,
        "exported_at": EXPORTED,
        "rows": 3,
        "rows_research": 2,
        "rows_learning": 1,
        "new_ids": ["a1", "b1", "l1"],
        "changed_ids": [],
        "prev_line_sha256": "0" * 64,
    }
    assert list(record) == sorted(record)


def test_importing_the_same_bytes_again_changes_nothing(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    write_export(src, [J()])
    data_dir = tmp_path / "data"
    run_import(src, data_dir)
    before = tree(data_dir), (data_dir / "imports.jsonl").read_bytes()
    again = run_import(src, data_dir)
    assert (again.status, again.seq) == ("unchanged", 1)
    assert (tree(data_dir), (data_dir / "imports.jsonl").read_bytes()) == before


def test_second_import_chains_to_the_first_and_lists_changes(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J(id="a1"), J(id="b1", ticker="NVDA")])
    run_import(first, data_dir)
    second = tmp_path / "two.json"
    write_export(
        second,
        [
            J(id="a1", status="resolved"),
            J(id="b1", ticker="NVDA", review="hit"),
            J(id="c1", ticker="SMH"),
        ],
        exported_at="2026-10-16T01:00:00Z",
    )
    result = run_import(second, data_dir)
    assert (result.status, result.seq) == ("imported", 2)
    lines = log_lines(data_dir)
    assert len(lines) == 2
    r1, r2 = (json.loads(x) for x in lines)
    assert r2["prev_line_sha256"] == hashlib.sha256(lines[0].encode("utf-8")).hexdigest()
    assert (r2["seq"], r2["new_ids"], r2["changed_ids"], r2["rows"]) == (2, ["c1"], ["a1", "b1"], 3)
    assert len(list((data_dir / "exports").iterdir())) == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "resolved"),
        ("review", "hit"),
        ("errorType", "分析错"),
        ("resolvedAt", "2026-10-16T00:00:00Z"),
    ],
)
def test_each_resolution_field_counts_as_a_change(tmp_path: Path, field: str, value: str) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J(id="a1"), J(id="b1", ticker="NVDA")])
    run_import(first, data_dir)
    second = tmp_path / "two.json"
    write_export(
        second, [J(id="a1"), J(id="b1", ticker="NVDA", **{field: value})], "2026-10-16T01:00:00Z"
    )
    assert run_import(second, data_dir).status == "imported"
    assert json.loads(log_lines(data_dir)[1])["changed_ids"] == ["b1"]


def test_addendum_counts_as_a_change(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J(id="a1"), J(id="b1", ticker="NVDA")])
    run_import(first, data_dir)
    second = tmp_path / "two.json"
    write_export(
        second,
        [J(id="a1"), J(id="b1", ticker="NVDA", addenda=[{"at": "2026-10-05T00:00:00Z"}])],
        "2026-10-16T01:00:00Z",
    )
    run_import(second, data_dir)
    assert json.loads(log_lines(data_dir)[1])["changed_ids"] == ["b1"]


def test_rejected_import_writes_nothing_at_all(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    src = tmp_path / "in.json"
    write_export(src, [J(ticker="XYZ")])
    result = run_import(src, data_dir)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["unknown_ticker"]
    assert not data_dir.exists()
    ok = tmp_path / "ok.json"
    write_export(ok, [J()])
    run_import(ok, data_dir)
    snapshot = {p: p.read_bytes() for p in data_dir.rglob("*") if p.is_file()}
    bad = tmp_path / "bad.json"
    write_export(bad, [J(prob=0.9)], "2026-10-16T01:00:00Z")
    result = run_import(bad, data_dir)
    assert result.status == "rejected" and ("immutable", "prob") in codes(result.issues)
    assert {p: p.read_bytes() for p in data_dir.rglob("*") if p.is_file()} == snapshot


def test_older_export_than_the_last_import_is_stale(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J()])
    run_import(first, data_dir)
    older = tmp_path / "older.json"
    write_export(older, [J(status="resolved")], exported_at="2026-10-14T07:00:00Z")
    result = run_import(older, data_dir)
    assert result.status == "rejected" and ("stale_export", "exported_at") in codes(result.issues)
    same_time = tmp_path / "same.json"
    write_export(same_time, [J(status="resolved")])
    assert ("stale_export", "exported_at") in codes(run_import(same_time, data_dir).issues)


def test_not_json_is_a_format_issue(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    src.write_bytes(b"{not json")
    result = run_import(src, tmp_path / "data")
    assert result.status == "rejected" and [i.code for i in result.issues] == ["format"]
    src.write_bytes(b"\xff\xfe\x00")
    assert [i.code for i in run_import(src, tmp_path / "data").issues] == ["format"]


def test_edited_copy_of_the_last_import_stops_the_next_one(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J()])
    run_import(first, data_dir)
    copy_path = next((data_dir / "exports").iterdir())
    copy_path.write_bytes(copy_path.read_bytes().replace(b"0.65", b"0.99"))
    second = tmp_path / "two.json"
    write_export(second, [J(status="resolved")], "2026-10-16T01:00:00Z")
    result = run_import(second, data_dir)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["baseline_tampered"]
    copy_path.unlink()
    assert [i.code for i in run_import(second, data_dir).issues] == ["baseline_tampered"]


def test_edited_log_line_breaks_the_chain(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J()])
    run_import(first, data_dir)
    second = tmp_path / "two.json"
    write_export(second, [J(status="resolved")], "2026-10-16T01:00:00Z")
    run_import(second, data_dir)
    third = tmp_path / "three.json"
    write_export(third, [J(status="resolved", review="hit")], "2026-10-17T01:00:00Z")
    lines = log_lines(data_dir)
    tampered = lines[0].replace('"rows":1', '"rows":2')
    assert tampered != lines[0]
    (data_dir / "imports.jsonl").write_bytes(
        ("\n".join([tampered, lines[1]]) + "\n").encode("utf-8")
    )
    result = run_import(third, data_dir)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["chain_broken"]


def test_log_line_with_a_wrong_sequence_number_breaks_the_chain(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J()])
    run_import(first, data_dir)
    line = log_lines(data_dir)[0]
    assert '"seq":1' in line
    (data_dir / "imports.jsonl").write_bytes(
        (line.replace('"seq":1', '"seq":2') + "\n").encode("utf-8")
    )
    second = tmp_path / "two.json"
    write_export(second, [J(status="resolved")], "2026-10-16T01:00:00Z")
    result = run_import(second, data_dir)
    assert result.status == "rejected" and [i.code for i in result.issues] == ["chain_broken"]


def test_crlf_export_is_stored_byte_for_byte(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    raw = write_export(src, [J()], newline="\r\n")
    assert b"\r\n" in raw
    result = run_import(src, tmp_path / "data")
    assert result.status == "imported"
    assert next((tmp_path / "data" / "exports").iterdir()).read_bytes() == raw


def test_dry_run_checks_but_writes_nothing(tmp_path: Path) -> None:
    src = tmp_path / "in.json"
    write_export(src, [J()])
    data_dir = tmp_path / "data"
    result = run_import(src, data_dir, dry_run=True)
    assert result.status == "imported" and result.counts["rows"] == 1
    assert not data_dir.exists()
    bad = tmp_path / "bad.json"
    write_export(bad, [J(ticker="XYZ")])
    assert run_import(bad, data_dir, dry_run=True).status == "rejected"


# ---------- registry, source entry, command ----------


def test_method_registry_in_the_repo() -> None:
    registry = load_methods()
    assert [m.id for m in registry.methods] == [
        "guidance_base_rate",
        "hf_leading",
        "op_leverage",
        "capex_cycle",
        "chain_readthrough",
        "expectation_crowding",
        "valuation_implied",
        "vol_pricing",
        "accounting_detail",
        "other",
    ]
    other = registry.methods[-1]
    assert (other.screen_template, other.mechanizable) == ("none", "none")
    crowding = next(m for m in registry.methods if m.id == "expectation_crowding")
    assert (crowding.screen_template, crowding.mechanizable) == ("event_study", "full")
    assert registry.ids() == {m.id for m in registry.methods}


def write_registry(path: Path, methods: list[dict[str, Any]], version: int = 1) -> Path:
    path.write_text(
        yaml.safe_dump({"schema_version": version, "methods": methods}, allow_unicode=True),
        encoding="utf-8",
    )
    return path


def method(id_: str, **over: Any) -> dict[str, Any]:
    return {
        "id": id_,
        "label": id_,
        "mechanizable": "none",
        "screen_template": "none",
        "question": "q",
        **over,
    }


@pytest.mark.parametrize(
    "methods",
    [
        [method("a"), method("a"), method("other")],
        [method("a")],
        [method("Bad Id"), method("other")],
        [method("a", mechanizable="maybe"), method("other")],
        [method("a", screen_template="magic"), method("other")],
        [method("a", extra=1), method("other")],
        [],
    ],
)
def test_bad_registry_is_refused(tmp_path: Path, methods: list[dict[str, Any]]) -> None:
    with pytest.raises(ValueError):
        load_methods(write_registry(tmp_path / "m.yaml", methods))


def test_registry_schema_version_is_checked(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_methods(write_registry(tmp_path / "m.yaml", [method("other")], version=2))
    assert load_methods(write_registry(tmp_path / "ok.yaml", [method("other")])).ids() == {"other"}


def test_methods_file_is_zone_c_by_name() -> None:
    ship = yaml.safe_load(
        (PROJECT_ROOT / "config" / "gov008_ship.yaml").read_text(encoding="utf-8")
    )
    assert "config/research/research_*.yaml" in ship["zones"]["C"]
    assert (PROJECT_ROOT / "config" / "research" / "research_judgment_methods.yaml").is_file()


def test_ledger_is_registered_as_a_private_manual_source() -> None:
    sources = {s.source_id: s for s in load_data_sources().sources}
    source = sources["local_judgment_ledger"]
    assert (source.source_type, source.status, source.cadence, source.requires_credentials) == (
        "manual_input",
        "active",
        "manual",
        False,
    )
    assert source.llm_permission.external_llm_allowed is False
    assert source.llm_permission.redistribution_allowed is False
    assert source.llm_permission.max_content_sent_level == "metadata_only"
    assert "data/external/judgments" in source.cache_paths
    assert {"schema", "duplicate_ids", "addenda_append_only", "hash_chained_import_log"} <= set(
        source.validation_checks
    )


def test_command_prints_ids_and_codes_but_never_statements(tmp_path: Path) -> None:
    runner = CliRunner()
    good = tmp_path / "good.json"
    write_export(
        good, [J(statement=SECRET), J(id="p1", kind="价格", market=MARKET, statement=SECRET)]
    )
    root = tmp_path / "root"
    out = runner.invoke(app, ["check", str(good), "--root", str(root)])
    assert out.exit_code == 0, out.output
    assert out.output.strip() == "ok rows=2 research=2 learning=0"
    assert not (root / "data").exists()
    out = runner.invoke(app, ["import", str(good), "--root", str(root)])
    assert out.exit_code == 0, out.output
    assert out.output.strip() == "imported seq=1 rows=2 research=2 learning=0"
    assert SECRET not in out.output
    assert (root / "data" / "external" / "judgments" / "imports.jsonl").is_file()
    again = runner.invoke(app, ["import", str(good), "--root", str(root)])
    assert again.exit_code == 0 and again.output.strip() == "unchanged seq=1"

    bad = tmp_path / "bad.json"
    write_export(bad, [J(id="z9", ticker="XYZ", statement=SECRET)])
    out = runner.invoke(app, ["import", str(bad), "--root", str(tmp_path / "root2")])
    assert out.exit_code == 1
    assert "issue unknown_ticker id=z9 field=ticker" in out.output
    assert "rejected: nothing was written" in out.output
    assert SECRET not in out.output
    assert not (tmp_path / "root2" / "data").exists()


def test_command_uses_the_repo_ticker_and_method_lists(tmp_path: Path) -> None:
    runner = CliRunner()
    src = tmp_path / "in.json"
    write_export(
        src,
        [
            J(ticker="ASML", methods=["expectation_crowding"]),
            J(id="a2", ticker="SMH", methods=["chain_readthrough"]),
            J(id="a3", ticker="ASX", methods=["capex_cycle"]),
            J(id="a4", ticker="SPY", methods=["other"]),
        ],
    )
    out = runner.invoke(app, ["check", str(src), "--root", str(tmp_path)])
    assert out.exit_code == 0, out.output
    write_export(src, [J(ticker="ZZZZ")])
    assert runner.invoke(app, ["check", str(src), "--root", str(tmp_path)]).exit_code == 1


# ---------- probability revisions ----------


def rev(at: str, prob: float, **extra: Any) -> dict[str, Any]:
    return {"at": at, "prob": prob, **extra}


R1 = rev("2026-10-01T06:33:08.542Z", 0.55)
R2 = rev("2026-10-01T06:35:28.919Z", 0.35, note="x")


def test_valid_revisions_have_no_issues() -> None:
    assert check([J(probRevisions=[R1, R2])]) == []
    assert check([J(probRevisions=[])]) == []
    assert check([J(probRevisions=[rev("2026-10-14T23:59:59Z", 0.5)])]) == []
    assert check([J(probRevisions=[rev("2026-10-01T06:16:08.523Z", 0.5)])]) == []


@pytest.mark.parametrize(
    "revisions",
    [
        "0.5",
        [0.5],
        [rev("2026-10-02T00:00:00Z", 0.0)],
        [rev("2026-10-02T00:00:00Z", 1.0)],
        [rev("2026-10-02T00:00:00Z", 1.2)],
        [rev("2026-10-02T00:00:00Z", "0.4")],
        [rev("2026-10-02T00:00:00Z", True)],
        [rev("2026-10-02T00:00:00", 0.4)],
        [rev("soon", 0.4)],
        [rev(1790000000, 0.4)],
        [{"prob": 0.4}],
        [{"at": "2026-10-02T00:00:00Z"}],
        [rev("2026-10-02T00:00:00Z", 0.4, extra=1)],
        [rev("2026-10-03T00:00:00Z", 0.4), rev("2026-10-02T00:00:00Z", 0.3)],
        [rev("2026-10-03T00:00:00Z", 0.4), rev("2026-10-03T00:00:00Z", 0.3)],
    ],
)
def test_bad_revision_is_a_schema_issue(revisions: Any) -> None:
    assert ("schema", "probRevisions") in codes(check([J(probRevisions=revisions)]))


@pytest.mark.parametrize(
    "at",
    [
        "2026-10-01T06:16:08.522Z",
        "2026-10-15T00:00:00Z",
        "2026-10-15T08:00:00Z",
        "2026-10-14T20:00:00-08:00",
    ],
)
def test_revision_must_fall_between_the_judgment_and_the_event(at: str) -> None:
    issues = check([J(probRevisions=[rev(at, 0.4)])])
    assert [(i.code, i.field) for i in issues] == [("time_order", "probRevisions")]


def test_revision_times_are_compared_in_utc() -> None:
    assert check([J(probRevisions=[rev("2026-10-15T08:00:00+09:00", 0.4)])]) == []


def test_effective_prob_is_the_latest_revision_else_the_original() -> None:
    assert effective_prob(J(prob=0.65)) == 0.65
    assert effective_prob(J(prob=0.65, probRevisions=[])) == 0.65
    assert effective_prob(J(prob=0.65, probRevisions=[R1])) == 0.55
    assert effective_prob(J(prob=0.65, probRevisions=[R1, R2])) == 0.35
    assert (
        effective_prob(J(prob=0.65, probRevisions=[R2, rev("2026-10-02T00:00:00Z", 0.45)])) == 0.45
    )


def test_revisions_are_append_only_against_the_previous_import() -> None:
    old = [J(id="a1", probRevisions=[R1])]
    assert check([J(id="a1", probRevisions=[R1])], old) == []
    assert check([J(id="a1", probRevisions=[R1, R2])], old) == []
    assert check([J(id="a1", probRevisions=[R1])], [J(id="a1")]) == []
    assert check([J(id="a1")], [J(id="a1")]) == []
    for new in (
        [],
        [rev("2026-10-01T06:33:08.542Z", 0.56)],
        [rev("2026-10-01T06:33:08.543Z", 0.55)],
        [R2],
    ):
        assert ("revisions_rewritten", "probRevisions") in codes(
            check([J(id="a1", probRevisions=new)], old)
        )
    two = [J(id="a1", probRevisions=[R1, R2])]
    assert ("revisions_rewritten", "probRevisions") in codes(
        check([J(id="a1", probRevisions=[R1])], two)
    )
    assert (
        check([J(id="a1", probRevisions=[rev("2026-10-01T06:33:08.542+00:00", 0.55), R2])], two)
        == []
    )


def test_the_original_prob_stays_fixed_when_a_revision_exists() -> None:
    old = [J(id="a1", probRevisions=[R1])]
    assert ("immutable", "prob") in codes(check([J(id="a1", prob=0.55, probRevisions=[R1])], old))


def test_new_revision_counts_as_a_change_in_the_log(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first = tmp_path / "one.json"
    write_export(first, [J(id="a1"), J(id="b1", ticker="NVDA")])
    run_import(first, data_dir)
    second = tmp_path / "two.json"
    write_export(
        second, [J(id="a1"), J(id="b1", ticker="NVDA", probRevisions=[R1])], "2026-10-16T01:00:00Z"
    )
    assert run_import(second, data_dir).status == "imported"
    assert json.loads(log_lines(data_dir)[1])["changed_ids"] == ["b1"]
