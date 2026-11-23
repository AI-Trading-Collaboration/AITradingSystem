"""Local-only, content-addressed Nasdaq-100 option index intake (TRADING-2567-41).

Owner decision: owner_decision:TRADING-2567:2026-11-22:option_index_first_look.
Fetch keeps BXN from Cboe, NQTRI/NQCLRI from Nasdaq through 2021-08-20 and FRED
from 2021-08-23, FRED's BXN copy for cross-checking only, and available methodology
files. Check judges the whole series against the checked QQQ calendar and Nasdaq-100
total returns, writing labels, counts, dates and PASS/WARN/FAIL, never levels or returns.
Readers use load_snapshot for checked, unchanged snapshots. No redistribution or external models.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Self

import numpy as np
import pandas as pd
import requests
import typer
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.paths import join_root, state_dir
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SPEC_PATH = PROJECT_ROOT / "config" / "research" / "research_option_indices.yaml"
SNAPSHOT_PARTS = ("data", "raw", "research", "option_indices")
SNAPSHOT_SCHEMA_VERSION = "research_option_indices_snapshot.v1"
DECISION_REQUIREMENTS = (
    Path("docs") / "requirements" / "TRADING-2567_QQQ_Research_Program_1999_Window.md"
)
INDEX_IDS = ("BXN", "NQTRI", "NQCLRI")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research-probe"
PAUSE_SECONDS = 0.5
TIMEOUT_SECONDS = 60
# Fixed data-integrity checks from the owner-approved intake specification, not research filters.
FIRST_DATE_SLACK_DAYS = 5
MAX_DAILY_MOVE = 0.25
MAX_GAP_DAYS = 5
KNOWN_GAPS = ((date(2001, 9, 10), date(2001, 9, 17)),)
SEAM_TOLERANCE = 0.015
CROSSCHECK_TOLERANCE = 1e-4
BASE_LEVEL = 100.0
BASE_TOLERANCE = 1e-6
MAX_LISTED_DATES = 10


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DocumentSpec(_Strict):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    url: str = Field(pattern=r"^https://[^\s]+$")


class SourceSpec(DocumentSpec):
    date_column: str
    value_column: str
    from_date: date | None = None
    through_date: date | None = None
    closed_marker: str | None = None


class IndexSpec(_Strict):
    id: Literal["BXN", "NQTRI", "NQCLRI"]
    name: str
    first_date: date
    base_100_on_first_date: bool
    sources: tuple[SourceSpec, ...] = Field(min_length=1, max_length=2)
    crosscheck: SourceSpec | None = None
    documents: tuple[DocumentSpec, ...] = ()

    @model_validator(mode="after")
    def validate_parts(self) -> Self:
        if len(self.sources) == 1:
            if self.sources[0].from_date is not None or self.sources[0].through_date is not None:
                raise ValueError("single source must be unbounded")
        else:
            first, second = self.sources
            if (
                first.from_date is not None
                or first.through_date is None
                or second.from_date is None
                or second.through_date is not None
                or first.through_date >= second.from_date
            ):
                raise ValueError("two sources need ordered, closed splice boundaries")
        if self.crosscheck is not None and (
            self.crosscheck.from_date is not None or self.crosscheck.through_date is not None
        ):
            raise ValueError("crosscheck must be unbounded")
        return self


class OptionIndexSpec(_Strict):
    schema_version: Literal["research_option_indices.v1"]
    owner_decision: str = Field(
        pattern=r"^owner_decision:[A-Za-z0-9_.-]+:\d{4}-\d{2}-\d{2}:[A-Za-z0-9_.-]+$"
    )
    qlh_snapshot: str = Field(pattern=r"^qlh-[A-Za-z0-9_-]+$")
    redistribution_allowed: Literal[False]
    external_llm_allowed: Literal[False]
    indices: tuple[IndexSpec, ...]

    @model_validator(mode="after")
    def validate_indices(self) -> Self:
        if tuple(i.id for i in self.indices) != INDEX_IDS:
            raise ValueError("indices must be BXN, NQTRI, NQCLRI in that order")
        names = [s.name for s in all_sources(self)] + [
            d.name for i in self.indices for d in i.documents
        ]
        if len(names) != len(set(names)):
            raise ValueError("source and document names must be unique")
        return self


def load_spec(path: Path = DEFAULT_SPEC_PATH) -> OptionIndexSpec:
    return OptionIndexSpec.model_validate(safe_load_yaml_path(path))


def index_sources(index: IndexSpec) -> tuple[SourceSpec, ...]:
    return index.sources + (() if index.crosscheck is None else (index.crosscheck,))


def all_sources(spec: OptionIndexSpec) -> tuple[SourceSpec, ...]:
    return tuple(s for index in spec.indices for s in index_sources(index))


# Fixed English month abbreviations keep CSV dates independent of the system locale.
_ENGLISH_MONTHS = (
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
)


def parse_csv(payload: bytes, source: SourceSpec) -> pd.DataFrame:
    name = source.name
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ValueError(f"{name}: not UTF-8 text") from None
    rows = csv.reader(io.StringIO(text))
    header: list[str] | None = None
    dates: list[date] = []
    values: list[float] = []
    for number, row in enumerate(rows, 1):
        if not row or not any(cell.strip() for cell in row):
            continue
        if header is None:
            header = row
            for column in (source.date_column, source.value_column):
                if column not in header:
                    raise ValueError(f"{name}: the header has no column {column}")
            di, vi = header.index(source.date_column), header.index(source.value_column)
            continue
        if len(row) <= max(di, vi):
            raise ValueError(f"{name}: row {number} is short")
        try:
            token = row[di].strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", token):
                day = date.fromisoformat(token)
            elif re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", token):
                day = datetime.strptime(token, "%m/%d/%Y").date()
            elif re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", token):
                day = datetime.strptime(token, "%m/%d/%y").date()
            elif match := re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", token):
                day = date(
                    int(match[3]), _ENGLISH_MONTHS.index(match[2].lower()) + 1, int(match[1])
                )
            else:
                raise ValueError
        except ValueError:
            raise ValueError(f"{name}: row {number} has an unreadable date") from None
        token = row[vi].strip()
        try:
            if source.closed_marker is not None and token in (source.closed_marker, ""):
                value = float("nan")
            else:
                value = float(token.replace(",", ""))
                if not np.isfinite(value):
                    raise ValueError
        except ValueError:
            raise ValueError(f"{name}: row {number} has an unreadable value") from None
        dates.append(day)
        values.append(value)
    if header is None:
        raise ValueError(f"{name}: empty file")
    if not dates:
        raise ValueError(f"{name}: no rows")
    return pd.DataFrame({"date": pd.to_datetime(dates), "value": values})


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def decision_recorded(spec: OptionIndexSpec, root: Path) -> None:
    try:
        text = join_root(root, DECISION_REQUIREMENTS).read_text(encoding="utf-8")
    except OSError:
        text = ""
    if re.search(re.escape(spec.owner_decision) + r"(?![A-Za-z0-9_])", text) is None:
        raise ValueError("option index intake needs the owner decision in the requirements")


def snapshot_root(root: Path) -> Path:
    return state_dir(root, *SNAPSHOT_PARTS)


def snapshot_dir(root: Path, snapshot_id: str) -> Path:
    if re.fullmatch(r"oix-[0-9a-f]{12}", snapshot_id) is None:
        raise ValueError("invalid option index snapshot id")
    return snapshot_root(root) / snapshot_id


def latest_snapshot_id(root: Path) -> str:
    return (snapshot_root(root) / "LATEST").read_text(encoding="utf-8").strip()


def fetch_snapshot(spec: OptionIndexSpec, root: Path, *, get: Callable[[str], bytes]) -> str:
    decision_recorded(spec, root)
    payloads: dict[str, bytes] = {}
    entries: list[dict[str, Any]] = []
    summaries: list[str] = []
    for source in all_sources(spec):
        payload = get(source.url)
        frame = parse_csv(payload, source)
        name = f"raw/{source.name}.csv"
        payloads[name] = payload
        entries.append(
            {"name": name, "sha256": _digest(payload), "bytes": len(payload), "rows": len(frame)}
        )
        summaries.append(
            f"{source.name}：{len(frame)} 行，"
            f"{_day(frame['date'].iloc[0])}..{_day(frame['date'].iloc[-1])}"
        )
    missing = []
    documents = [d for index in spec.indices for d in index.documents]
    for document in documents:
        try:
            payload = get(document.url)
        except Exception as exc:
            missing.append({"name": document.name, "error": type(exc).__name__})
            continue
        name = f"documents/{document.name}.pdf"
        payloads[name] = payload
        entries.append({"name": name, "sha256": _digest(payload), "bytes": len(payload)})
    manifest = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "spec": spec.model_dump(mode="json"),
        "files": entries,
    }
    body = _canonical(manifest)
    snapshot_id = "oix-" + _digest(body)[:12]
    directory = snapshot_dir(root, snapshot_id)
    for name, payload in payloads.items():
        path = directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    (directory / "manifest.json").write_bytes(body)
    (directory / "fetch.json").write_bytes(
        _canonical(
            {
                "downloaded_at": datetime.now(UTC).isoformat(),
                "user_agent": USER_AGENT,
                "sources": [s.model_dump(mode="json") for s in all_sources(spec)],
                "documents": [d.model_dump(mode="json") for d in documents],
                "documents_missing": missing,
            }
        )
    )
    (snapshot_root(root) / "LATEST").write_text(snapshot_id + "\n", encoding="utf-8")
    typer.echo(f"快照 {snapshot_id}")
    for summary in summaries:
        typer.echo(summary)
    typer.echo(f"方法文件：{len(documents) - len(missing)}/{len(documents)} 份")
    return snapshot_id


def http_getter() -> Callable[[str], bytes]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    def get(url: str) -> bytes:
        time.sleep(PAUSE_SECONDS)
        response = session.get(url, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.content

    return get


getter_factory = http_getter


def _manifest(directory: Path, snapshot_id: str) -> tuple[dict[str, Any], OptionIndexSpec]:
    try:
        raw = json.loads((directory / "manifest.json").read_bytes())
        if (
            set(raw) != {"schema_version", "spec", "files"}
            or raw["schema_version"] != SNAPSHOT_SCHEMA_VERSION
            or "oix-" + _digest(_canonical(raw))[:12] != snapshot_id
        ):
            raise ValueError
        spec = OptionIndexSpec.model_validate(raw["spec"])
        required = {f"raw/{s.name}.csv" for s in all_sources(spec)}
        optional = {f"documents/{d.name}.pdf" for i in spec.indices for d in i.documents}
        names = [e["name"] for e in raw["files"]]
        if len(names) != len(set(names)) or not required <= set(names) <= required | optional:
            raise ValueError
        for entry in raw["files"]:
            expected = {"name", "sha256", "bytes"}
            if entry["name"] in required:
                expected.add("rows")
            if (
                set(entry) != expected
                or re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is None
                or type(entry["bytes"]) is not int
                or entry["bytes"] < 0
                or ("rows" in entry and (type(entry["rows"]) is not int or entry["rows"] < 1))
            ):
                raise ValueError
        return raw, spec
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise ValueError("invalid option index manifest") from None


def _mismatches(directory: Path, manifest: dict[str, Any]) -> list[str]:
    bad = []
    for entry in manifest["files"]:
        try:
            payload = (directory / entry["name"]).read_bytes()
            ok = len(payload) == entry["bytes"] and _digest(payload) == entry["sha256"]
        except OSError:
            ok = False
        if not ok:
            bad.append(entry["name"])
    return sorted(bad)


def _part(frame: pd.DataFrame, source: SourceSpec) -> pd.DataFrame:
    keep = frame["value"].notna()
    if source.from_date is not None:
        keep &= frame["date"] >= pd.Timestamp(source.from_date)
    if source.through_date is not None:
        keep &= frame["date"] <= pd.Timestamp(source.through_date)
    return frame.loc[keep]


def _levels(index: IndexSpec, frames: dict[str, pd.DataFrame]) -> pd.Series:
    frame = pd.concat([_part(frames[s.name], s) for s in index.sources])
    return pd.Series(
        frame["value"].to_numpy(dtype=float), index=pd.DatetimeIndex(frame["date"]), name=index.id
    ).sort_index()


def _day(value: Any) -> str:
    return pd.Timestamp(value).date().isoformat()


def _listed(dates: pd.DatetimeIndex) -> list[str]:
    return [_day(d) for d in dates[:MAX_LISTED_DATES]]


def _index_check(
    index: IndexSpec,
    frames: dict[str, pd.DataFrame],
    reference: pd.Series,
    failed: set[str],
    warned: set[str],
) -> dict[str, Any]:
    row: dict[str, Any] = {"sources": {}}
    for source in index_sources(index):
        frame = frames[source.name]
        days = pd.DatetimeIndex(frame["date"])
        duplicates = int(days.duplicated().sum())
        unordered = not days.is_monotonic_increasing
        row["sources"][source.name] = {
            "rows": len(frame),
            "first_date": _day(days[0]),
            "last_date": _day(days[-1]),
            "duplicates": duplicates,
            "unordered": unordered,
            "closed": int(frame["value"].isna().sum()),
            "kept": len(_part(frame, source)),
        }
        if duplicates or unordered:
            failed.add(f"{index.id}:order")
        if source in index.sources and not len(_part(frame, source)):
            failed.add(f"{index.id}:empty_part")
    levels = _levels(index, frames)
    if levels.empty:
        return row
    days = pd.DatetimeIndex(levels.index)
    calendar = pd.DatetimeIndex(reference.index)
    first, last = days[0], days[-1]
    moves = levels.pct_change(fill_method=None).abs() > MAX_DAILY_MOVE
    gaps = [
        f"{_day(a)}..{_day(b)}"
        for a, b in zip(days[:-1], days[1:], strict=True)
        if (b - a).days > MAX_GAP_DAYS
        and not any(lo <= a.date() <= b.date() <= hi for lo, hi in KNOWN_GAPS)
    ]
    row.update(
        first_date=_day(first),
        last_date=_day(last),
        first_date_late=(first.date() - index.first_date).days > FIRST_DATE_SLACK_DAYS,
        nonpositive=int((levels <= 0).sum()),
        large_moves=int(moves.sum()),
        large_move_dates=_listed(days[moves.to_numpy()]),
        long_gaps=len(gaps),
        long_gap_dates=gaps[:MAX_LISTED_DATES],
        ends_before_reference=bool(last < calendar[-1]),
    )
    for key, label in (
        ("first_date_late", "first_date"),
        ("nonpositive", "nonpositive"),
        ("large_moves", "move"),
        ("long_gaps", "gap"),
        ("ends_before_reference", "ends_early"),
    ):
        if row[key]:
            failed.add(f"{index.id}:{label}")
    row["seam"] = None
    if len(index.sources) == 2:
        left, right = index.sources
        assert left.through_date is not None and right.from_date is not None
        a, b = _part(frames[left.name], left), _part(frames[right.name], right)
        csv_last = None if a.empty else pd.Timestamp(a["date"].iloc[-1])
        fred_first = None if b.empty else pd.Timestamp(b["date"].iloc[0])
        following = calendar[calendar > csv_last] if csv_last is not None else calendar[:0]
        next_day = following[0] if len(following) else None
        dates_ok = bool(
            csv_last == pd.Timestamp(left.through_date)
            and fred_first == pd.Timestamp(right.from_date)
            and fred_first == next_day
        )
        residual_ok = False
        if dates_ok and csv_last is not None and fred_first is not None:
            segment = reference.loc[(calendar > csv_last) & (calendar <= fred_first)]
            prior = float(a["value"].iloc[-1])
            if prior > 0:
                own = float(b["value"].iloc[0]) / prior - 1.0
                market = float(np.prod(1.0 + segment.to_numpy(dtype=float))) - 1.0
                residual_ok = bool(segment.notna().all() and abs(own - market) <= SEAM_TOLERANCE)
        row["seam"] = {
            "csv_last": None if csv_last is None else _day(csv_last),
            "fred_first": None if fred_first is None else _day(fred_first),
            "next_trading_day": None if next_day is None else _day(next_day),
            "dates": "PASS" if dates_ok else "FAIL",
            "residual": "PASS" if residual_ok else "FAIL",
            "status": "PASS" if dates_ok and residual_ok else "FAIL",
        }
        if not dates_ok or not residual_ok:
            failed.add(f"{index.id}:seam")
    closed = pd.DatetimeIndex([])
    for source in index.sources:
        frame = frames[source.name]
        mask = frame["value"].isna()
        if source.from_date is not None:
            mask &= frame["date"] >= pd.Timestamp(source.from_date)
        if source.through_date is not None:
            mask &= frame["date"] <= pd.Timestamp(source.through_date)
        closed = closed.union(pd.DatetimeIndex(frame.loc[mask, "date"]))
    closed = closed.intersection(calendar)
    off = days[days <= calendar[-1]].difference(calendar)
    missing = calendar[(calendar >= first) & (calendar <= last)].difference(days)
    row.update(
        closed_on_trading_days=len(closed),
        closed_on_trading_dates=_listed(closed),
        off_calendar=len(off),
        off_calendar_dates=_listed(off),
        missing_calendar=len(missing),
        missing_calendar_dates=_listed(missing),
    )
    if len(closed):
        warned.add(f"{index.id}:closed_days")
    if len(off) or len(missing):
        warned.add(f"{index.id}:calendar")
    row["base"] = None
    if index.base_100_on_first_date:
        base_ok = first.date() == index.first_date and abs(levels.iloc[0] - BASE_LEVEL) <= (
            BASE_TOLERANCE
        )
        row["base"] = "PASS" if base_ok else "WARN"
        if not base_ok:
            warned.add(f"{index.id}:base")
    row["crosscheck"] = None
    if index.crosscheck is not None:
        copy = _part(frames[index.crosscheck.name], index.crosscheck)
        other = pd.Series(copy["value"].to_numpy(), index=pd.DatetimeIndex(copy["date"]))
        # Duplicates already fail order; avoid ambiguous alignment while finishing diagnostics.
        primary = levels.loc[~levels.index.duplicated()]
        other = other.loc[~other.index.duplicated()]
        common = primary.index.intersection(other.index)
        over = int(
            ((other.loc[common] / primary.loc[common] - 1).abs() > CROSSCHECK_TOLERANCE).sum()
        )
        row["crosscheck"] = {"overlap_days": len(common), "over_tolerance_days": over}
        if not len(common) or over:
            warned.add(f"{index.id}:crosscheck")
    return row


def check_snapshot(root: Path, snapshot_id: str, reference: pd.Series) -> dict[str, Any]:
    directory = snapshot_dir(root, snapshot_id)
    calendar = pd.DatetimeIndex(reference.index)
    if calendar.empty or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError("invalid reference calendar")
    report: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "reference": {
            "first_date": _day(calendar[0]),
            "last_date": _day(calendar[-1]),
            "days": len(calendar),
        },
        "file_mismatches": [],
        "unreadable": [],
        "documents": {"kept": [], "missing": []},
        "indices": {},
        "qlh_snapshot": None,
    }
    failed: set[str] = set()
    warned: set[str] = set()
    try:
        manifest, spec = _manifest(directory, snapshot_id)
    except ValueError:
        failed.add("manifest")
    else:
        report["qlh_snapshot"] = spec.qlh_snapshot
        report["file_mismatches"] = _mismatches(directory, manifest)
        if report["file_mismatches"]:
            failed.add("files")
        names = {e["name"] for e in manifest["files"]}
        for index in spec.indices:
            for doc in index.documents:
                key = "kept" if f"documents/{doc.name}.pdf" in names else "missing"
                report["documents"][key].append(doc.name)
        if report["documents"]["missing"]:
            warned.add("documents")
        for index in spec.indices:
            frames = {}
            for source in index_sources(index):
                try:
                    frames[source.name] = parse_csv(
                        (directory / f"raw/{source.name}.csv").read_bytes(), source
                    )
                except (OSError, ValueError):
                    report["unreadable"].append(source.name)
            if len(frames) != len(index_sources(index)):
                failed.add(f"{index.id}:parse")
                report["indices"][index.id] = {}
            else:
                report["indices"][index.id] = _index_check(index, frames, reference, failed, warned)
    report.update(
        status="FAIL" if failed else "WARN" if warned else "PASS",
        failed=sorted(failed),
        warned=sorted(warned),
    )
    (directory / "check.json").write_bytes(_canonical(report))
    (directory / "check.md").write_text(
        "# 期权指数快照检查\n\n```json\n" + _canonical(report).decode() + "\n```\n",
        encoding="utf-8",
    )
    return report


@dataclass(frozen=True)
class OptionIndexSnapshot:
    snapshot_id: str
    spec: OptionIndexSpec
    levels: dict[str, pd.Series]
    check: dict[str, Any]


def load_snapshot(root: Path, snapshot_id: str) -> OptionIndexSnapshot:
    directory = snapshot_dir(root, snapshot_id)
    manifest, spec = _manifest(directory, snapshot_id)
    try:
        check = json.loads((directory / "check.json").read_bytes())
        if (
            check["status"] not in {"PASS", "WARN"}
            or check["snapshot_id"] != snapshot_id
            or _mismatches(directory, manifest)
        ):
            raise ValueError
        frames = {
            s.name: parse_csv((directory / f"raw/{s.name}.csv").read_bytes(), s)
            for s in all_sources(spec)
        }
        levels: dict[str, pd.Series] = {i.id: _levels(i, frames) for i in spec.indices}
        for series in levels.values():
            if (
                series.empty
                or series.index.has_duplicates
                or not series.index.is_monotonic_increasing
            ):
                raise ValueError
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError("option index snapshot must be checked and unchanged") from None
    return OptionIndexSnapshot(snapshot_id, spec, levels, check)


app = typer.Typer(help="本机期权指数快照：只下载和检查，不外传。")


@app.command("fetch")
def fetch_command(
    root: Annotated[Path, typer.Option()] = PROJECT_ROOT,
    spec: Annotated[Path, typer.Option()] = DEFAULT_SPEC_PATH,
) -> None:
    try:
        fetch_snapshot(load_spec(join_root(root, spec)), root, get=getter_factory())
    except Exception as exc:
        message = sanitize_diagnostic_text(f"{type(exc).__name__}: {exc}")
        typer.echo(f"下载失败：{message}")
        raise typer.Exit(code=1) from None


@app.command("check")
def check_command(
    snapshot: Annotated[str | None, typer.Option()] = None,
    root: Annotated[Path, typer.Option()] = PROJECT_ROOT,
    qlh_root: Annotated[Path, typer.Option()] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    try:
        snapshot_id = snapshot or latest_snapshot_id(root)
        _, spec = _manifest(snapshot_dir(root, snapshot_id), snapshot_id)
        daily, _ = qlh.load_daily(qlh.resolve_snapshot_dir(qlh_root, spec.qlh_snapshot))
        report = check_snapshot(root, snapshot_id, daily["ndx_tr_return"])
    except Exception as exc:
        message = sanitize_diagnostic_text(f"{type(exc).__name__}: {exc}")
        typer.echo(f"检查失败：{message}")
        raise typer.Exit(code=1) from None
    typer.echo(f"快照 {snapshot_id}：{report['status']}")
    if report["status"] != "PASS":
        typer.echo(f"FAIL：{report['failed']}；WARN：{report['warned']}")
    if report["status"] == "FAIL":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
