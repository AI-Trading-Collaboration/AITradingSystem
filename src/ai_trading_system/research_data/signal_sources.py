"""Local-only, content-addressed signal intake (TRADING-2565 S4a).

Never print rows or send snapshots to external models. Research consumers must use load_snapshot,
which verifies the content address, each CSV checksum and the local quality check.

Monthly sources are FRED's monthly series dated at month start in its own convention and
Robert Shiller's monthly stock-market data, one column per source, dated at month start.
Manela's news implied volatility (TRADING-2565-91) comes from his .xlsx workbook, read with
the standard library and dated at month start.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from numbers import Real
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import requests
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import FredRateProvider, ProviderDownloadError, RateRequest
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.paths import state_dir
from ai_trading_system.research_bench.screen_settings import (
    LoadedScreenSettings,
    load_screen_settings,
)
from ai_trading_system.research_bench.sources import SHILLER_COLUMNS, SourceSpec

# S4a's fixed data-quality contract: a gap requires MORE than five missing weekdays. Weekday
# counting (not an exchange calendar) is deliberate and never determines signal eligibility.
MAX_MISSING_BUSINESS_DAYS = 5
KEN_FRENCH_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/{dataset}_CSV.zip"
# Use fetch --shiller-url when the site lists a new workbook address.
SHILLER_URL = (
    "https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/downloads/"
    "1449043e-a1ed-4e2e-8e9c-7b9a2a064493/ie_data.xls?ver=1791297298731"
)
SHILLER_SHEET = "Data"
# Column headers in the row beginning with "Date".
SHILLER_HEADERS = {"P": "P", "D": "D", "E": "E", "CPI": "CPI", "GS10": "Rate GS10"}
# Manela's NVIX workbook (TRADING-2565-91); use fetch --manela-url for a new data-page file.
MANELA_URL = "https://asaf.manela.org/papers/mm/nvix/nvix_and_categories_timeseries_mar2016.xlsx"
MANELA_SHEET = "news_implied_volatility"
# Row 1 headers: the date column and each dataset's column.
MANELA_DATE_HEADER = "Date"
MANELA_HEADERS = {"nvix": "NVIX"}
MANELA_MISSING_TEXT = frozenset({"", "nan", "na", "#n/a"})
# Bound XML expansion in memory before parsing any workbook member (not a data-quality threshold).
MANELA_MAX_MEMBER_BYTES = 20 * 1024 * 1024
# Download history, not an evaluation window: all approved FRED sources begin after 1900.
FRED_HISTORY_START = date(1900, 1, 1)


@dataclass(frozen=True)
class SignalSnapshot:
    snapshot_id: str
    frames: dict[str, pd.DataFrame]
    specs: tuple[SourceSpec, ...]


def parse_ken_french_csv(text: str) -> pd.DataFrame:
    """Read only the first daily block (the value-weighted block for industry portfolios)."""

    lines = text.splitlines()
    header_position = next(
        (i for i, line in enumerate(lines) if line.lstrip().startswith(",")), None
    )
    if header_position is None:
        raise ValueError("Ken French CSV has no header")
    columns = [name.strip() for name in next(csv.reader([lines[header_position]]))[1:]]
    if not columns or any(not name for name in columns) or len(set(columns)) != len(columns):
        raise ValueError("Ken French CSV has invalid columns")
    rows: list[list[str]] = []
    for line in lines[header_position + 1 :]:
        if not re.match(r"^\d{8},", line):
            break
        row = next(csv.reader([line]))
        if len(row) != len(columns) + 1:
            raise ValueError("Ken French CSV has an invalid row width")
        rows.append(row)
    try:
        dates = pd.to_datetime([row[0] for row in rows], format="%Y%m%d")
        frame = pd.DataFrame([row[1:] for row in rows], columns=columns, index=dates).astype(float)
    except (ValueError, TypeError):
        raise ValueError("Ken French CSV has invalid dates or numbers") from None
    frame.index.name = "date"
    return frame.replace([-99.99, -999], np.nan) / 100


def _shiller_month(cell: object) -> pd.Timestamp | None:
    """Read Shiller's y.mm month notation: y.1 (including numeric y.10) is October."""

    if isinstance(cell, bool):
        return None
    if isinstance(cell, Real):
        try:
            number = float(cell)
        except (ValueError, OverflowError):
            return None
        if not np.isfinite(number):
            return None
        text = f"{number:.2f}"
    elif isinstance(cell, str):
        text = cell.strip()
    else:
        return None
    if re.fullmatch(r"\d{4}\.\d{1,2}", text) is None:
        return None
    year, fraction = text.split(".")
    month = int(fraction.ljust(2, "0"))
    if not 1 <= month <= 12:
        return None
    try:
        return pd.Timestamp(year=int(year), month=month, day=1)
    except ValueError:
        return None


def parse_shiller_table(raw: pd.DataFrame) -> pd.DataFrame:
    """Read the first monthly block of a headerless Shiller Data sheet by exact headers."""

    header_position = next(
        (i for i in range(len(raw)) if len(raw.columns) and str(raw.iat[i, 0]).strip() == "Date"),
        None,
    )
    if header_position is None:
        raise ValueError("Shiller sheet has no Date header")
    headers = raw.iloc[header_position].tolist()
    columns = sorted(SHILLER_COLUMNS)
    positions: list[int] = []
    for column in columns:
        matches = [
            i
            for i, cell in enumerate(headers)
            if isinstance(cell, str) and cell == SHILLER_HEADERS[column]
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Shiller sheet needs exactly one {SHILLER_HEADERS[column]}: {headers}"
            )
        positions.append(matches[0])
    dates: list[pd.Timestamp] = []
    rows: list[list[Any]] = []
    for i in range(header_position + 1, len(raw)):
        month = _shiller_month(raw.iat[i, 0])
        if month is None:
            if dates:
                break
            continue
        dates.append(month)
        rows.append(raw.iloc[i, positions].tolist())
    index = pd.DatetimeIndex(dates, name="date")
    if not len(index) or not index.is_monotonic_increasing or not index.is_unique:
        raise ValueError("Shiller sheet needs increasing, unique monthly rows")
    frame = pd.DataFrame(rows, index=index, columns=columns)
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)
    return frame


def _shiller_fetch(url: str = SHILLER_URL) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    try:
        raw = pd.read_excel(io.BytesIO(response.content), sheet_name=SHILLER_SHEET, header=None)
    except ImportError as exc:
        raise ValueError(
            f"reading Shiller's workbook needs xlrd (.xls) or openpyxl (.xlsx): {exc}"
        ) from None
    return parse_shiller_table(raw)


def parse_manela_workbook(data: bytes) -> pd.DataFrame:
    """Read Manela's NVIX column, storing numeric yyyymmdd month ends as monthly dates."""

    main = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    relation_id = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    package = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise ValueError("Manela workbook is not a readable zip") from None

    def read_xml(name: str) -> ET.Element:
        try:
            info = archive.getinfo(name)
        except KeyError:
            raise ValueError(f"Manela workbook is missing member {name}") from None
        if info.file_size > MANELA_MAX_MEMBER_BYTES:
            raise ValueError(f"Manela workbook member exceeds size limit: {name}")
        try:
            payload = archive.read(info)
        except (zipfile.BadZipFile, RuntimeError, NotImplementedError):
            raise ValueError("Manela workbook member is not readable") from None
        if b"<!DOCTYPE" in payload:
            raise ValueError(f"Manela workbook member contains a document type: {name}")
        try:
            return ET.fromstring(payload)
        except ET.ParseError:
            raise ValueError(f"Manela workbook member has invalid XML: {name}") from None

    with archive:
        workbook = read_xml("xl/workbook.xml")
        relations = read_xml("xl/_rels/workbook.xml.rels")
        sheets = [
            sheet for sheet in workbook.iter(f"{main}sheet") if sheet.get("name") == MANELA_SHEET
        ]
        if len(sheets) != 1:
            raise ValueError("Manela workbook needs exactly one data sheet")
        rid = sheets[0].get(relation_id)
        targets = [
            item.get("Target")
            for item in relations.iter(f"{package}Relationship")
            if rid is not None and item.get("Id") == rid
        ]
        if len(targets) != 1 or not targets[0]:
            raise ValueError("Manela workbook has no unique data sheet relationship")
        target = targets[0]
        member = target.lstrip("/") if target.startswith("/") else f"xl/{target}"
        if not member.startswith("xl/") or ".." in member.split("/"):
            raise ValueError("Manela workbook data sheet path is outside xl/")
        strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared = read_xml("xl/sharedStrings.xml")
            strings = [
                "".join(node.text or "" for node in item.iter(f"{main}t"))
                for item in shared.findall(f"{main}si")
            ]
        sheet = read_xml(member)

    def cell_text(cell: ET.Element) -> str | None:
        kind = cell.get("t")
        if kind == "s":
            value = cell.find(f"{main}v")
            try:
                if value is None or value.text is None:
                    raise ValueError
                position = int(value.text)
            except ValueError:
                raise ValueError("Manela workbook has an invalid shared string index") from None
            if not 0 <= position < len(strings):
                raise ValueError("Manela workbook shared string index is out of range")
            return strings[position]
        if kind == "inlineStr":
            return "".join(node.text or "" for node in cell.iter(f"{main}t"))
        if kind == "str":
            return cell.findtext(f"{main}v", default="")
        return None

    def column(cell: ET.Element) -> str:
        match = re.match(r"[A-Z]+", cell.get("r", ""))
        if match is None:
            raise ValueError("Manela workbook cell has no column reference")
        return match.group()

    sheet_rows = sheet.findall(f"{main}sheetData/{main}row")
    if not sheet_rows or sheet_rows[0].get("r") != "1":
        raise ValueError("Manela workbook headers must be on row 1")
    headers: dict[str, list[str]] = {}
    for cell in sheet_rows[0].findall(f"{main}c"):
        name = column(cell)
        text = cell_text(cell)
        if text is not None:
            headers.setdefault(text.strip().lower(), []).append(name)
    positions: list[str] = []
    for header in (MANELA_DATE_HEADER, *MANELA_HEADERS.values()):
        matches = headers.get(header.lower(), [])
        if len(matches) != 1:
            raise ValueError(f"Manela workbook needs exactly one {header} header")
        positions.append(matches[0])

    dates: list[pd.Timestamp] = []
    rows: list[list[float]] = []
    for row in sheet_rows[1:]:
        cells = {column(cell): cell for cell in row.findall(f"{main}c")}
        if not any(position in cells for position in positions):
            continue
        date_cell = cells.get(positions[0])
        if date_cell is None or date_cell.get("t") not in (None, "n"):
            raise ValueError("Manela workbook date must be numeric")
        date_text = date_cell.findtext(f"{main}v")
        try:
            if date_text is None:
                raise ValueError
            number = float(date_text)
            if not number.is_integer():
                raise ValueError
            day = pd.Timestamp(datetime.strptime(f"{int(number):08d}", "%Y%m%d"))
            if not day.is_month_end:
                raise ValueError
        except (ValueError, OverflowError):
            raise ValueError("Manela workbook date must be a yyyymmdd month end") from None
        dates.append(day.replace(day=1))
        values: list[float] = []
        for position in positions[1:]:
            data_cell = cells.get(position)
            if data_cell is None or data_cell.get("t") == "e":
                values.append(np.nan)
                continue
            text = cell_text(data_cell)
            if text is not None:
                if text.strip().lower() not in MANELA_MISSING_TEXT:
                    raise ValueError("Manela workbook data column contains unexpected text")
                values.append(np.nan)
                continue
            raw = data_cell.findtext(f"{main}v")
            try:
                values.append(float(raw) if raw is not None and raw.strip() else np.nan)
            except ValueError:
                raise ValueError("Manela workbook data column contains an invalid number") from None
        rows.append(values)
    if not dates:
        raise ValueError("Manela workbook has no data rows")
    index = pd.DatetimeIndex(dates, name="date")
    months = index.to_period("M").astype("int64").to_numpy()
    if np.any(np.diff(months) != 1):
        raise ValueError("Manela workbook months must be consecutive and increasing")
    return pd.DataFrame(rows, index=index, columns=list(MANELA_HEADERS), dtype=float)


def _manela_fetch(url: str = MANELA_URL) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return parse_manela_workbook(response.content)


def extract_csv(data: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            files = [
                info
                for info in archive.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".csv")
            ]
            if len(files) != 1:
                raise ValueError("download must contain exactly one CSV")
            return archive.read(files[0]).decode("utf-8")
    except (zipfile.BadZipFile, UnicodeDecodeError, RuntimeError, NotImplementedError):
        raise ValueError("download is not a readable UTF-8 CSV zip") from None


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _snapshot_root(root: Path) -> Path:
    return state_dir(root, "data", "raw", "research", "signal_sources")


def _snapshot_dir(root: Path, snapshot_id: str) -> Path:
    if not re.fullmatch(r"sig-[0-9a-f]{12}", snapshot_id):
        raise ValueError("invalid signal snapshot id")
    return state_dir(root, "data", "raw", "research", "signal_sources", snapshot_id)


def _date_range(dates: pd.DatetimeIndex) -> dict[str, str | None]:
    valid = dates.dropna()
    return {
        "first_date": valid.min().date().isoformat() if len(valid) else None,
        "last_date": valid.max().date().isoformat() if len(valid) else None,
    }


def fetch_snapshot(
    settings: LoadedScreenSettings,
    root: Path,
    *,
    fred_fetch: Callable[[str], pd.Series],
    ken_french_fetch: Callable[[str], bytes],
    shiller_fetch: Callable[[], pd.DataFrame] | None = None,
    shiller_url: str = SHILLER_URL,
    manela_fetch: Callable[[], pd.DataFrame] | None = None,
    manela_url: str = MANELA_URL,
) -> str:
    """Preserve returned row order, duplicates and missing values; only check judges quality.

    Download Shiller's workbook once per snapshot, with each Shiller source taking one column.
    Download Manela's NVIX workbook once per snapshot in the same way.
    """

    sources: list[dict[str, Any]] = []
    payloads: dict[str, bytes] = {}
    shiller_table: pd.DataFrame | None = None
    manela_table: pd.DataFrame | None = None
    for spec in settings.settings.sources:
        if spec.provider == "fred":
            frame = fred_fetch(spec.dataset).to_frame(name="value")
        elif spec.provider == "shiller":
            if shiller_table is None:
                if shiller_fetch is None:
                    raise ValueError("Shiller sources need a Shiller fetch")
                shiller_table = shiller_fetch()
            if spec.dataset not in shiller_table.columns:
                raise ValueError(f"Shiller table is missing column {spec.dataset}")
            frame = shiller_table[spec.dataset].to_frame(name="value")
        elif spec.provider == "manela":
            if manela_table is None:
                if manela_fetch is None:
                    raise ValueError("Manela sources need a Manela fetch")
                manela_table = manela_fetch()
            if spec.dataset not in manela_table.columns:
                raise ValueError(f"Manela table is missing column {spec.dataset}")
            frame = manela_table[spec.dataset].to_frame(name="value")
        else:
            frame = parse_ken_french_csv(extract_csv(ken_french_fetch(spec.dataset)))
        dates = pd.DatetimeIndex(frame.index)
        frame.index = dates
        payload = frame.to_csv(
            index_label="date", date_format="%Y-%m-%d", lineterminator="\n"
        ).encode("utf-8")
        filename = f"{spec.source_id}.csv"
        payloads[filename] = payload
        sources.append(
            {
                "spec": spec.model_dump(mode="json"),
                "filename": filename,
                "sha256": _digest(payload),
                "row_count": len(frame),
                **_date_range(dates),
            }
        )
    manifest = {"schema_version": "research_signal_snapshot.v1", "sources": sources}
    manifest_payload = _canonical_json(manifest)
    snapshot_id = "sig-" + _digest(manifest_payload)[:12]
    directory = _snapshot_dir(root, snapshot_id)
    directory.mkdir(parents=True, exist_ok=True)
    for filename, payload in payloads.items():
        (directory / filename).write_bytes(payload)
    (directory / "manifest.json").write_bytes(manifest_payload + b"\n")
    # Fetch time is operational metadata, deliberately excluded from the content address.
    (directory / "fetch.json").write_bytes(
        _canonical_json(
            {
                "downloaded_at": datetime.now(tz=UTC).isoformat(),
                "sources": [
                    {
                        "source_id": spec.source_id,
                        "provider": spec.provider,
                        "source_class": "public-convenience",
                        "endpoint": FredRateProvider.base_url
                        if spec.provider == "fred"
                        else shiller_url
                        if spec.provider == "shiller"
                        else manela_url
                        if spec.provider == "manela"
                        else KEN_FRENCH_URL.format(dataset=spec.dataset),
                        "request_parameters": {"dataset": spec.dataset},
                    }
                    for spec in settings.settings.sources
                ],
            }
        )
        + b"\n"
    )
    (_snapshot_root(root) / "LATEST").write_text(snapshot_id + "\n", encoding="utf-8")
    typer.echo(f"快照：{snapshot_id}")
    for source in sources:
        typer.echo(
            f"  {source['spec']['source_id']}：{source['row_count']} 行，"
            f"{source['first_date']}..{source['last_date']}"
        )
    return snapshot_id


def _read_manifest(directory: Path, snapshot_id: str) -> dict[str, Any]:
    try:
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if manifest["schema_version"] != "research_signal_snapshot.v1":
            raise ValueError("unsupported manifest schema")
        if "sig-" + _digest(_canonical_json(manifest))[:12] != snapshot_id:
            raise ValueError("manifest does not match snapshot id")
        sources = manifest["sources"]
        if not sources:
            raise ValueError("manifest has no sources")
        ids = []
        for source in sources:
            spec = SourceSpec.model_validate(source["spec"])
            ids.append(spec.source_id)
            if source["filename"] != f"{spec.source_id}.csv":
                raise ValueError("invalid snapshot filename")
        if len(set(ids)) != len(ids):
            raise ValueError("manifest has duplicate sources")
        return dict(manifest)
    except (OSError, KeyError, TypeError, ValueError):
        raise ValueError("signal snapshot manifest is missing or invalid") from None


def _read_frame(payload: bytes) -> pd.DataFrame:
    try:
        frame = pd.read_csv(io.BytesIO(payload), dtype={"date": str}, float_precision="round_trip")
        if "date" not in frame or len(frame.columns) < 2:
            raise ValueError("missing CSV columns")
        dates = pd.to_datetime(frame.pop("date"), format="%Y-%m-%d", errors="coerce")
        frame.index = pd.DatetimeIndex(dates, name="date")
        return frame
    except (ValueError, UnicodeDecodeError, pd.errors.ParserError):
        raise ValueError("signal snapshot CSV has an invalid schema") from None


def check_snapshot(root: Path, snapshot_id: str) -> dict[str, Any]:
    """Check without repairs; reports contain only identifiers, counts, dates and ratios."""

    directory = _snapshot_dir(root, snapshot_id)
    summaries: dict[str, dict[str, Any]] = {}
    failed = False
    warned = False
    try:
        manifest = _read_manifest(directory, snapshot_id)
    except ValueError:
        manifest = {"sources": []}
        failed = True
    for source in manifest["sources"]:
        source_id = source["spec"]["source_id"]
        record: dict[str, Any] = {"file_mismatch_count": 0, "invalid_file_count": 0}
        summaries[source_id] = record
        try:
            payload = (directory / source["filename"]).read_bytes()
            record["file_mismatch_count"] = int(_digest(payload) != source["sha256"])
            frame = _read_frame(payload)
        except (OSError, ValueError):
            record["invalid_file_count"] = 1
            failed = True
            continue
        dates = pd.DatetimeIndex(frame.index)
        numeric = frame.apply(pd.to_numeric, errors="coerce")
        ratios = {
            str(column): float(numeric[column].notna().mean()) if len(frame) else 0.0
            for column in frame.columns
        }
        invalid_numeric = int((frame.notna() & numeric.isna()).to_numpy().sum())
        gaps = 0
        monthly = source["spec"]["frequency"] == "monthly"
        if monthly:
            record["off_month_start_count"] = int(np.count_nonzero(dates.day != 1))
        if not dates.hasnans:
            if monthly:
                months = dates.to_period("M").astype("int64").to_numpy()
                gaps = int(np.count_nonzero(np.diff(months) > 1))
            else:
                gaps = sum(
                    bool(
                        np.busday_count(previous.date(), following.date()) - 1
                        > MAX_MISSING_BUSINESS_DAYS
                    )
                    for previous, following in zip(dates[:-1], dates[1:], strict=True)
                )
        record.update(
            row_count=len(frame),
            **_date_range(dates),
            invalid_date_count=int(np.count_nonzero(dates.isna())),
            duplicate_date_count=int(dates.duplicated().sum()),
            unordered_count=int(not dates.is_monotonic_increasing),
            nonempty_ratio=ratios,
            invalid_numeric_count=invalid_numeric,
            gap_count=int(gaps),
            metadata_mismatch_count=int(
                len(frame) != source["row_count"]
                or _date_range(dates)
                != {"first_date": source["first_date"], "last_date": source["last_date"]}
            ),
        )
        failed |= bool(
            record["file_mismatch_count"]
            or record["metadata_mismatch_count"]
            or dates.hasnans
            or dates.has_duplicates
            or not dates.is_monotonic_increasing
            or not len(frame)
            or not ratios
            or any(ratio == 0 for ratio in ratios.values())
            or invalid_numeric
            or record.get("off_month_start_count", 0)
        )
        warned |= gaps > 0
    report = {
        "snapshot_id": snapshot_id,
        "status": "FAIL" if failed else "WARN" if warned else "PASS",
        "sources": summaries,
    }
    (directory / "check.json").write_bytes(_canonical_json(report) + b"\n")
    lines = [f"# 信号快照检查：{snapshot_id}", "", f"状态：{report['status']}", ""]
    for source_id, record in summaries.items():
        lines.append(f"## {source_id}")
        for key, value in record.items():
            lines.append(f"- {key}：{json.dumps(value, ensure_ascii=False, sort_keys=True)}")
    (directory / "check.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def load_snapshot(root: Path, snapshot_id: str) -> SignalSnapshot:
    directory = _snapshot_dir(root, snapshot_id)
    manifest = _read_manifest(directory, snapshot_id)
    try:
        check = json.loads((directory / "check.json").read_bytes())
        if check.get("status") not in {"PASS", "WARN"} or check.get("snapshot_id") != snapshot_id:
            raise ValueError("snapshot check did not pass")
    except (OSError, ValueError, AttributeError):
        raise ValueError("signal snapshot is unchecked or failed") from None
    frames: dict[str, pd.DataFrame] = {}
    specs = []
    for source in manifest["sources"]:
        spec = SourceSpec.model_validate(source["spec"])
        try:
            payload = (directory / source["filename"]).read_bytes()
        except OSError:
            raise ValueError("signal snapshot CSV is missing") from None
        if _digest(payload) != source["sha256"]:
            raise ValueError("signal snapshot CSV checksum mismatch")
        frames[spec.source_id] = _read_frame(payload)
        specs.append(spec)
    return SignalSnapshot(snapshot_id, frames, tuple(specs))


def _fred_fetch(dataset: str) -> pd.Series:
    request = RateRequest(series_ids=[dataset], start=FRED_HISTORY_START, end=date.today())
    frame = FredRateProvider().download_rates(request)
    rows = frame.loc[frame["series"] == dataset]
    return pd.Series(rows["value"].to_numpy(), index=pd.to_datetime(rows["date"]))


def _ken_french_fetch(dataset: str) -> bytes:
    response = requests.get(KEN_FRENCH_URL.format(dataset=dataset), timeout=60)
    response.raise_for_status()
    return bytes(response.content)


def _safe_error_text(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, ProviderDownloadError) and exc.diagnostic.exception_type:
        text += f" ({exc.diagnostic.exception_type}: {exc.diagnostic.exception_message})"
    return sanitize_diagnostic_text(text)


app = typer.Typer(help="信号筛选数据入口：只在本机，只输出汇总。", no_args_is_help=True)
RootOption = Annotated[Path, typer.Option(help="代码根目录；状态路径遵循 AITS_STATE_ROOT。")]


@app.command("fetch")
def fetch_command(
    root: RootOption = PROJECT_ROOT,
    shiller_url: Annotated[str, typer.Option(help="Shiller 月度数据的下载地址。")] = SHILLER_URL,
    manela_url: Annotated[str, typer.Option(help="Manela 的 NVIX 工作簿地址。")] = MANELA_URL,
) -> None:
    """下载全部批准来源为本机快照；不打印数据行。"""

    try:
        fetch_snapshot(
            load_screen_settings(),
            root,
            fred_fetch=_fred_fetch,
            ken_french_fetch=_ken_french_fetch,
            shiller_fetch=lambda: _shiller_fetch(shiller_url),
            shiller_url=shiller_url,
            manela_fetch=lambda: _manela_fetch(manela_url),
            manela_url=manela_url,
        )
    except Exception as exc:
        typer.echo(f"下载失败：{_safe_error_text(exc)}")
        raise typer.Exit(code=1) from None


@app.command("check")
def check_command(
    snapshot: Annotated[str, typer.Option(help="快照 ID，默认 LATEST。")] = "",
    root: RootOption = PROJECT_ROOT,
) -> None:
    """检查已有快照，写只含汇总的 check.json 和 check.md。"""

    try:
        snapshot_id = snapshot or (_snapshot_root(root) / "LATEST").read_text("utf-8").strip()
        report = check_snapshot(root, snapshot_id)
    except Exception as exc:
        typer.echo(f"检查失败：{_safe_error_text(exc)}")
        raise typer.Exit(code=1) from None
    typer.echo(f"快照：{snapshot_id}；检查：{report['status']}")
    if report["status"] == "FAIL":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
