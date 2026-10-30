"""Local-only, content-addressed earnings event intake (TRADING-2565-98).

The period 6 earnings-shock print-only table reads three inputs: seventeen issuers' 8-K lists,
eight annual QQQ trust holdings documents as raw bytes, and adjusted closes for FMP tickers.
Everything stops at QQQ's development window: filings and prices after data_end are discarded
in memory and never written. Never print data rows or send snapshots to external models.
Readers use load_snapshot. Parsing holdings rows belongs to the printing module, not this intake.
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import os
import re
import time
from collections import Counter
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
from ai_trading_system.data.market_data import PriceDataProvider
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.paths import state_dir
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SPEC_PATH = PROJECT_ROOT / "config" / "research" / "research_earnings.yaml"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/"
# The QQQ trust files the annual holdings documents.
HOLDINGS_CIK = 1067839
SEC_USER_AGENT_ENV = "SEC_USER_AGENT"
# SEC fair access permits ten requests/second; one per quarter-second stays well below that.
SEC_PAUSE_SECONDS = 0.25
SEC_TIMEOUT_SECONDS = 60
# QQQ development ends here; 2011-2021 is validation and must not enter these snapshots.
QQQ_DEVELOPMENT_END = date(2010, 12, 31)
FILING_FORMS = frozenset({"8-K", "8-K/A"})
FILING_COLUMNS = ("cik", "accessionNumber", "form", "filingDate", "acceptanceDateTime", "items")
# Results use Item 2.02 from 2004-08-23; previously the same disclosure used Item 12.
RESULTS_ITEMS = frozenset({"2.02", "12"})
HOLDINGS_MARKERS = ("schedule of investments", "total investments")
SNAPSHOT_SCHEMA_VERSION = "research_earnings_snapshot.v1"
# Same contract as signal_sources: a gap requires MORE than five missing weekdays.
MAX_MISSING_BUSINESS_DAYS = 5
EASTERN = "America/New_York"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IssuerSpec(_Strict):
    id: str = Field(pattern=r"^[A-Z]{2,5}$")
    ciks: tuple[int, ...] = Field(min_length=1)
    entity_pattern: str = Field(min_length=1)
    fmp_ticker: str | None = Field(default=None, pattern=r"^[A-Z]{1,5}$")
    foreign: Literal[False] = False

    @model_validator(mode="after")
    def validate_issuer(self) -> Self:
        if any(cik <= 0 for cik in self.ciks) or len(set(self.ciks)) != len(self.ciks):
            raise ValueError("issuer CIKs must be positive and unique")
        try:
            re.compile(self.entity_pattern)
        except re.error:
            raise ValueError("invalid entity pattern") from None
        return self


class ForeignIssuer(_Strict):
    id: str = Field(pattern=r"^[A-Z]{2,5}$")
    foreign: Literal[True]


class HoldingsDocument(_Strict):
    fiscal_year: int
    filing_date: date
    accession: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    document: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*\.(txt|htm)$")
    form: str = Field(min_length=1)


class EarningsSpec(_Strict):
    schema_version: Literal["research_earnings.v1"]
    filing_start: date
    data_end: date
    price_start: date
    issuers: tuple[IssuerSpec, ...] = Field(min_length=1)
    foreign: tuple[ForeignIssuer, ...] = ()
    holdings_documents: tuple[HoldingsDocument, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_window_and_identifiers(self) -> Self:
        if not self.filing_start <= self.price_start <= self.data_end <= QQQ_DEVELOPMENT_END:
            raise ValueError("earnings window must stay inside QQQ development")
        identifiers = [issuer.id for issuer in self.issuers] + [
            issuer.id for issuer in self.foreign
        ]
        ciks = [cik for issuer in self.issuers for cik in issuer.ciks]
        tickers = [issuer.fmp_ticker for issuer in self.issuers if issuer.fmp_ticker]
        years = [document.fiscal_year for document in self.holdings_documents]
        for values in (identifiers, ciks, tickers, years):
            if len(set(values)) != len(values):
                raise ValueError("earnings identifiers must be unique")
        for document in self.holdings_documents:
            if not date(document.fiscal_year, 9, 30) < document.filing_date <= self.data_end:
                raise ValueError("holdings filing must follow fiscal year end inside the window")
        return self


def load_spec(path: Path = DEFAULT_SPEC_PATH) -> EarningsSpec:
    return EarningsSpec.model_validate(safe_load_yaml_path(path))


@dataclass(frozen=True)
class EarningsSnapshot:
    snapshot_id: str
    spec: EarningsSpec
    entities: tuple[dict[str, Any], ...]
    filings: pd.DataFrame
    documents: dict[int, tuple[str, bytes]]
    prices: pd.DataFrame


def is_results_filing(form: str, items: str) -> bool:
    return form == "8-K" and bool(
        RESULTS_ITEMS.intersection(item.strip() for item in items.split(","))
    )


def parse_acceptance(value: str) -> pd.Timestamp:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value):
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ"
        try:
            return pd.Timestamp(datetime.strptime(value, fmt).replace(tzinfo=UTC))
        except ValueError:
            pass
    raise ValueError("acceptance time is not an ISO UTC time") from None


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _snapshot_root(root: Path) -> Path:
    return state_dir(root, "data", "raw", "research", "earnings_events")


def _snapshot_dir(root: Path, snapshot_id: str) -> Path:
    if re.fullmatch(r"ern-[0-9a-f]{12}", snapshot_id) is None:
        raise ValueError("invalid earnings snapshot id")
    return _snapshot_root(root) / snapshot_id


def _holdings_name(document: HoldingsDocument) -> str:
    return f"holdings/{document.fiscal_year}{Path(document.document).suffix.lower()}"


def _filing_rows(page: dict[str, Any], cik: int, spec: EarningsSpec) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in range(len(page.get("accessionNumber", []))):
        row: dict[str, Any] = {"cik": cik}
        for column in FILING_COLUMNS[1:]:
            values = page.get(column, [])
            row[column] = values[position] if position < len(values) else ""
        if (
            row["form"] in FILING_FORMS
            and spec.filing_start.isoformat() <= row["filingDate"] <= spec.data_end.isoformat()
        ):
            rows.append(row)
    return rows


def fetch_snapshot(
    spec: EarningsSpec,
    root: Path,
    *,
    sec_get: Callable[[str], bytes],
    price_provider: PriceDataProvider,
) -> str:
    """Prepare all requests and payloads in memory before creating any snapshot files."""

    entities: list[dict[str, Any]] = []
    filing_rows: list[dict[str, Any]] = []
    for issuer in spec.issuers:
        for cik in issuer.ciks:
            data = json.loads(sec_get(f"{SUBMISSIONS_URL}CIK{cik:010d}.json"))
            names = [data["name"], *(item["name"] for item in data.get("formerNames", []))]
            if not any(re.search(issuer.entity_pattern, name, re.IGNORECASE) for name in names):
                raise ValueError(f"CIK {cik} does not name issuer {issuer.id}")
            entities.append({"cik": cik, "issuer": issuer.id, "name": data["name"]})
            filing_rows.extend(_filing_rows(data["filings"]["recent"], cik, spec))
            for page in data["filings"].get("files", []):
                if (
                    page["filingFrom"] <= spec.data_end.isoformat()
                    and page["filingTo"] >= spec.filing_start.isoformat()
                ):
                    filing_rows.extend(
                        _filing_rows(json.loads(sec_get(SUBMISSIONS_URL + page["name"])), cik, spec)
                    )
    payloads: dict[str, bytes] = {}
    for document in spec.holdings_documents:
        url = (
            f"{ARCHIVES_URL}{HOLDINGS_CIK}/{document.accession.replace('-', '')}/"
            f"{document.document}"
        )
        payloads[_holdings_name(document)] = sec_get(url)
    price_frames: list[pd.DataFrame] = []
    for issuer in spec.issuers:
        if issuer.fmp_ticker is None:
            continue
        requests_ = qlh.etf_reference_requests(issuer.fmp_ticker, spec.price_start, spec.data_end)
        rows = qlh._download_reference(price_provider, requests_)
        rows = rows.loc[
            rows["date"].between(spec.price_start.isoformat(), spec.data_end.isoformat()),
            ["date", "adj_close"],
        ].copy()
        rows.insert(0, "ticker", issuer.fmp_ticker)
        price_frames.append(rows)
    filings = pd.DataFrame(filing_rows, columns=list(FILING_COLUMNS)).sort_values(
        ["cik", "acceptanceDateTime", "accessionNumber"], kind="stable", ignore_index=True
    )
    prices = (
        pd.concat(price_frames, ignore_index=True)
        if price_frames
        else pd.DataFrame(columns=["ticker", "date", "adj_close"])
    ).sort_values(["ticker", "date"], kind="stable", ignore_index=True)
    payloads = {
        "filings.csv": filings.to_csv(index=False, lineterminator="\n").encode("utf-8"),
        "prices.csv": prices.to_csv(index=False, lineterminator="\n").encode("utf-8"),
        **payloads,
    }
    files: list[dict[str, Any]] = []
    for name, payload in payloads.items():
        entry: dict[str, Any] = {"name": name, "sha256": _digest(payload), "bytes": len(payload)}
        if name in {"filings.csv", "prices.csv"}:
            entry["rows"] = len(filings) if name == "filings.csv" else len(prices)
        files.append(entry)
    manifest = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "spec": spec.model_dump(mode="json"),
        "entities": entities,
        "files": files,
    }
    body = _canonical_json(manifest)
    snapshot_id = "ern-" + _digest(body)[:12]
    fetch_body = _canonical_json(
        {
            "downloaded_at": datetime.now(tz=UTC).isoformat(),
            "sources": [
                {"name": "sec_submissions", "source_class": "primary", "endpoint": SUBMISSIONS_URL},
                {
                    "name": "qqq_trust_holdings",
                    "source_class": "primary",
                    "endpoint": f"{ARCHIVES_URL}{HOLDINGS_CIK}/",
                },
                {"name": "fmp_adjusted_close", "source_class": "paid-vendor", "endpoint": "FMP"},
            ],
        }
    )
    directory = _snapshot_dir(root, snapshot_id)
    (directory / "holdings").mkdir(parents=True, exist_ok=True)
    for name, payload in payloads.items():
        (directory / name).write_bytes(payload)
    (directory / "manifest.json").write_bytes(body + b"\n")
    (directory / "fetch.json").write_bytes(fetch_body + b"\n")
    (_snapshot_root(root) / "LATEST").write_text(snapshot_id + "\n", encoding="utf-8")
    typer.echo(f"快照：{snapshot_id}")
    for issuer in spec.issuers:
        count = int(filings["cik"].isin(issuer.ciks).sum())
        typer.echo(f"  {issuer.id}：{len(issuer.ciks)} 个 CIK，8-K 和 8-K/A 共 {count} 行")
    typer.echo(f"持仓文件：{len(spec.holdings_documents)} 份；股价：{len(prices)} 行")
    return snapshot_id


def _read_manifest(directory: Path, snapshot_id: str) -> dict[str, Any]:
    try:
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if manifest["schema_version"] != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError
        if "ern-" + _digest(_canonical_json(manifest))[:12] != snapshot_id:
            raise ValueError
        spec = EarningsSpec.model_validate(manifest["spec"])
        allowed = {"filings.csv", "prices.csv"} | {
            _holdings_name(document) for document in spec.holdings_documents
        }
        names = [entry["name"] for entry in manifest["files"]]
        if (
            len(set(names)) != len(names)
            or not {"filings.csv", "prices.csv"}.issubset(names)
            or not set(names).issubset(allowed)
        ):
            raise ValueError
        for entry in manifest["files"]:
            if (
                not isinstance(entry["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is None
                or not isinstance(entry["bytes"], int)
                or entry["bytes"] < 0
            ):
                raise ValueError
            if entry["name"] in {"filings.csv", "prices.csv"}:
                if not isinstance(entry["rows"], int) or entry["rows"] < 0:
                    raise ValueError
        expected_entities = {cik: issuer.id for issuer in spec.issuers for cik in issuer.ciks}
        entities = manifest["entities"]
        if (
            len(entities) != len(expected_entities)
            or {entity["cik"]: entity["issuer"] for entity in entities} != expected_entities
            or any(set(entity) != {"cik", "issuer", "name"} for entity in entities)
        ):
            raise ValueError
        return dict(manifest)
    except (OSError, KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("earnings snapshot manifest is missing or invalid") from None


def _read_filings(payload: bytes) -> pd.DataFrame:
    try:
        frame = pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False)
        if tuple(frame.columns) != FILING_COLUMNS:
            raise ValueError
        frame["cik"] = frame["cik"].map(int)
        return frame
    except (ValueError, TypeError, OverflowError):
        raise ValueError("earnings filings CSV has an invalid schema") from None


def _read_prices(payload: bytes) -> pd.DataFrame:
    try:
        frame = pd.read_csv(
            io.BytesIO(payload), dtype={"ticker": str, "date": str}, float_precision="round_trip"
        )
        if tuple(frame.columns) != ("ticker", "date", "adj_close"):
            raise ValueError
        if not frame["date"].str.fullmatch(r"\d{4}-\d{2}-\d{2}").all():
            raise ValueError
        frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d", errors="raise")
        if frame["date"].isna().any():
            raise ValueError
        frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="raise").astype(float)
        return frame
    except (ValueError, TypeError, OverflowError):
        raise ValueError("earnings prices CSV has an invalid schema") from None


def _filings_summary(
    frame: pd.DataFrame, spec: EarningsSpec, entities: list[dict[str, Any]], expected_rows: int
) -> dict[str, Any]:
    by_cik = {entity["cik"]: entity["issuer"] for entity in entities}
    hours: Counter[int] = Counter()
    years: dict[str, dict[str, dict[str, int]]] = {}
    results = 0
    missing_time = 0
    for row in frame.to_dict(orient="records"):
        form, items = str(row["form"]), str(row["items"])
        item_set = {item.strip() for item in items.split(",")}
        if is_results_filing(form, items):
            results += 1
            try:
                hours[
                    parse_acceptance(str(row["acceptanceDateTime"])).tz_convert(EASTERN).hour
                ] += 1
            except ValueError:
                missing_time += 1
        issuer = by_cik.get(row["cik"])
        # Only ISO dates can contribute a year; never echo malformed source text in a report.
        try:
            filing_date = date.fromisoformat(str(row["filingDate"]))
        except ValueError:
            continue
        if issuer is not None:
            counts = years.setdefault(issuer, {}).setdefault(
                str(filing_date.year), {"8-K": 0, "8-K/A": 0, "2.02": 0, "12": 0}
            )
            if form in FILING_FORMS:
                counts[form] += 1
            if form == "8-K":
                for item in RESULTS_ITEMS:
                    counts[item] += int(item in item_set)
    return {
        "row_count": len(frame),
        "row_count_mismatch": int(len(frame) != expected_rows),
        "outside_dates": int(
            (
                ~frame["filingDate"].between(
                    spec.filing_start.isoformat(), spec.data_end.isoformat()
                )
            ).sum()
        ),
        "duplicate_accessions": int(frame["accessionNumber"].duplicated().sum()),
        "other_forms": int((~frame["form"].isin(FILING_FORMS)).sum()),
        "unknown_ciks": int((~frame["cik"].isin(by_cik)).sum()),
        "results_filings": results,
        "results_without_time": missing_time,
        "results_eastern_hours": {str(hour): hours[hour] for hour in sorted(hours)},
        "per_issuer_year": years,
    }


def _prices_summary(frame: pd.DataFrame, spec: EarningsSpec, expected_rows: int) -> dict[str, Any]:
    tickers: dict[str, dict[str, Any]] = {}
    configured = [issuer.fmp_ticker for issuer in spec.issuers if issuer.fmp_ticker]
    for ticker in configured:
        dates = pd.DatetimeIndex(frame.loc[frame["ticker"] == ticker, "date"]).sort_values()
        gaps = sum(
            bool(np.busday_count(a.date(), b.date()) - 1 > MAX_MISSING_BUSINESS_DAYS)
            for a, b in zip(dates[:-1], dates[1:], strict=True)
        )
        tickers[ticker] = {
            "rows": len(dates),
            "first_date": dates.min().date().isoformat() if len(dates) else None,
            "last_date": dates.max().date().isoformat() if len(dates) else None,
            "gaps": gaps,
        }
    return {
        "row_count_mismatch": int(len(frame) != expected_rows),
        "nonpositive": int((~(frame["adj_close"] > 0)).sum()),
        "duplicates": int(frame.duplicated(["ticker", "date"]).sum()),
        "after_data_end": int((frame["date"] > pd.Timestamp(spec.data_end)).sum()),
        "before_price_start": int((frame["date"] < pd.Timestamp(spec.price_start)).sum()),
        "unknown_tickers": int(frame.loc[~frame["ticker"].isin(configured), "ticker"].nunique()),
        "tickers": tickers,
    }


def check_snapshot(root: Path, snapshot_id: str) -> dict[str, Any]:
    """Judge without repairs and write only safe labels, counts, dates, hours and byte sizes."""

    directory = _snapshot_dir(root, snapshot_id)
    failed: set[str] = set()
    warned: set[str] = set()
    report: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "file_mismatches": [],
        "filings": {},
        "holdings": {},
        "prices": {},
    }
    try:
        manifest = _read_manifest(directory, snapshot_id)
    except ValueError:
        failed.add("manifest")
    else:
        spec = EarningsSpec.model_validate(manifest["spec"])
        payloads: dict[str, bytes] = {}
        entries = {entry["name"]: entry for entry in manifest["files"]}
        for name, entry in entries.items():
            try:
                payload = (directory / name).read_bytes()
                payloads[name] = payload
                if _digest(payload) == entry["sha256"] and len(payload) == entry["bytes"]:
                    continue
            except OSError:
                pass
            report["file_mismatches"].append(name)
        report["file_mismatches"].sort()
        if report["file_mismatches"]:
            failed.add("files")
        try:
            filings = _read_filings(payloads["filings.csv"])
        except (ValueError, KeyError):
            failed.add("filings_schema")
        else:
            summary = _filings_summary(
                filings, spec, manifest["entities"], entries["filings.csv"]["rows"]
            )
            report["filings"] = summary
            if any(
                summary[key]
                for key in (
                    "row_count_mismatch",
                    "outside_dates",
                    "duplicate_accessions",
                    "other_forms",
                    "unknown_ciks",
                    "results_without_time",
                )
            ):
                failed.add("filings")
        for document in spec.holdings_documents:
            payload = payloads.get(_holdings_name(document))
            if payload is None:
                report["holdings"][str(document.fiscal_year)] = {"present": False}
                failed.add("holdings")
                continue
            text = re.sub(r"<[^>]+>", " ", payload.decode("utf-8", errors="replace"))
            text = " ".join(html.unescape(text).replace("\xa0", " ").split()).lower()
            markers = (*HOLDINGS_MARKERS, f"september 30, {document.fiscal_year}")
            missing = [marker for marker in markers if marker not in text]
            report["holdings"][str(document.fiscal_year)] = {
                "present": True,
                "bytes": len(payload),
                "missing_markers": missing,
            }
            if missing:
                failed.add("holdings")
        try:
            prices = _read_prices(payloads["prices.csv"])
        except (ValueError, KeyError):
            failed.add("prices_schema")
        else:
            summary = _prices_summary(prices, spec, entries["prices.csv"]["rows"])
            report["prices"] = summary
            if any(
                summary[key]
                for key in (
                    "row_count_mismatch",
                    "nonpositive",
                    "duplicates",
                    "after_data_end",
                    "before_price_start",
                    "unknown_tickers",
                )
            ) or any(item["rows"] == 0 for item in summary["tickers"].values()):
                failed.add("prices")
            if any(item["gaps"] for item in summary["tickers"].values()):
                warned.add("price_gaps")
    report.update(
        status="FAIL" if failed else "WARN" if warned else "PASS",
        failed=sorted(failed),
        warned=sorted(warned),
    )
    # An unreadable manifest may live in a missing directory; still record the failed check.
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "check.json").write_bytes(_canonical_json(report) + b"\n")
    lines = [f"# 财报事件快照检查：{snapshot_id}", "", f"状态：{report['status']}", ""]
    for key, value in report.items():
        lines.append(f"- {key}：{json.dumps(value, ensure_ascii=False, sort_keys=True)}")
    (directory / "check.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def load_snapshot(root: Path, snapshot_id: str) -> EarningsSnapshot:
    directory = _snapshot_dir(root, snapshot_id)
    manifest = _read_manifest(directory, snapshot_id)
    try:
        check = json.loads((directory / "check.json").read_bytes())
        if check.get("status") not in {"PASS", "WARN"} or check.get("snapshot_id") != snapshot_id:
            raise ValueError
    except (OSError, ValueError, AttributeError, TypeError):
        raise ValueError("earnings snapshot is unchecked or failed") from None
    payloads: dict[str, bytes] = {}
    for entry in manifest["files"]:
        try:
            payload = (directory / entry["name"]).read_bytes()
        except OSError:
            raise ValueError("earnings snapshot file is missing") from None
        if _digest(payload) != entry["sha256"] or len(payload) != entry["bytes"]:
            raise ValueError("earnings snapshot file checksum mismatch")
        payloads[entry["name"]] = payload
    spec = EarningsSpec.model_validate(manifest["spec"])
    try:
        documents = {
            document.fiscal_year: (document.document, payloads[_holdings_name(document)])
            for document in spec.holdings_documents
        }
    except KeyError:
        raise ValueError("earnings snapshot holdings document is missing") from None
    return EarningsSnapshot(
        snapshot_id=snapshot_id,
        spec=spec,
        entities=tuple(dict(entity) for entity in manifest["entities"]),
        filings=_read_filings(payloads["filings.csv"]),
        documents=documents,
        prices=_read_prices(payloads["prices.csv"]),
    )


def sec_getter(user_agent: str) -> Callable[[str], bytes]:
    def get(url: str) -> bytes:
        time.sleep(SEC_PAUSE_SECONDS)
        response = requests.get(
            url,
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=SEC_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return bytes(response.content)

    return get


price_provider_factory: Callable[[], PriceDataProvider] = qlh.fmp_price_provider
app = typer.Typer(help="财报事件研究数据入口：只在本机，只输出汇总。", no_args_is_help=True)
RootOption = Annotated[Path, typer.Option(help="代码根目录；状态路径遵循 AITS_STATE_ROOT。")]


@app.command("fetch")
def fetch_command(root: RootOption = PROJECT_ROOT) -> None:
    """下载批准来源；只输出汇总。"""

    agent = os.environ.get(SEC_USER_AGENT_ENV, "").strip()
    if not agent:
        typer.echo("下载失败：SEC_USER_AGENT 没有设置")
        raise typer.Exit(code=1)
    try:
        fetch_snapshot(
            load_spec(), root, sec_get=sec_getter(agent), price_provider=price_provider_factory()
        )
    except Exception as exc:
        text = sanitize_diagnostic_text(f"{type(exc).__name__}: {exc}", extra_secrets=(agent,))
        typer.echo(f"下载失败：{text}")
        raise typer.Exit(code=1) from None


@app.command("check")
def check_command(
    snapshot: Annotated[str, typer.Option(help="快照 ID，默认 LATEST。")] = "",
    root: RootOption = PROJECT_ROOT,
) -> None:
    """检查已有快照，只写本机汇总。"""

    try:
        snapshot_id = snapshot or (_snapshot_root(root) / "LATEST").read_text("utf-8").strip()
        report = check_snapshot(root, snapshot_id)
    except Exception as exc:
        typer.echo(f"检查失败：{sanitize_diagnostic_text(f'{type(exc).__name__}: {exc}')}")
        raise typer.Exit(code=1) from None
    typer.echo(f"快照：{snapshot_id}；检查：{report['status']}")
    if report["status"] == "FAIL":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
