from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from ai_trading_system.config import SecCompaniesConfig
from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SEC_PIT_CURRENT_HISTORY_GRADE,
    read_sec_pit_raw_manifest,
    sec_pit_submissions_path,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

SEC_FILING_TIMELINE_COLUMNS = (
    "ticker",
    "cik",
    "company_name",
    "accession_number",
    "accession_number_compact",
    "form",
    "filing_date",
    "report_date",
    "acceptance_datetime_utc",
    "available_time_utc",
    "available_date",
    "available_for_signal_date",
    "primary_document",
    "primary_doc_description",
    "is_xbrl",
    "is_inline_xbrl",
    "source_url",
    "source_endpoint",
    "raw_payload_path",
    "raw_payload_sha256",
    "source_row_number",
    "pit_data_grade",
    "confidence_level",
    "confidence_reason",
)

_NEW_YORK = ZoneInfo("America/New_York")


def build_filing_timeline(
    *,
    sec_companies: SecCompaniesConfig,
    raw_dir: Path,
    start: date,
    end: date,
) -> pd.DataFrame:
    if start > end:
        raise ValueError("start must be on or before end")
    raw_manifest = read_sec_pit_raw_manifest(raw_dir)
    records: list[dict[str, object]] = []
    for company in sec_companies.companies:
        if not company.active:
            continue
        path = sec_pit_submissions_path(raw_dir, company.ticker, company.cik)
        if not path.exists():
            continue
        raw = _load_json(path)
        raw_meta = _raw_manifest_meta(raw_manifest, path)
        for row_index, filing in enumerate(_recent_filing_records(raw), start=1):
            filing_date = _date_or_none(filing.get("filingDate"))
            if filing_date is None or filing_date > end:
                continue
            accession = str(filing.get("accessionNumber") or "")
            if not accession:
                continue
            acceptance = _parse_acceptance_datetime(filing.get("acceptanceDateTime"))
            available_time, grade, confidence, reason = _availability_fields(
                acceptance,
                filing_date,
            )
            available_date = available_time.date() if available_time is not None else None
            available_for_signal_date = (
                next_us_trading_day(available_date) if available_date is not None else None
            )
            compact = accession.replace("-", "")
            source_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(company.cik)}/{compact}/{filing.get('primaryDocument') or ''}"
            )
            records.append(
                {
                    "ticker": company.ticker,
                    "cik": company.cik,
                    "company_name": company.company_name,
                    "accession_number": accession,
                    "accession_number_compact": compact,
                    "form": str(filing.get("form") or "").upper(),
                    "filing_date": filing_date.isoformat(),
                    "report_date": _date_text(filing.get("reportDate")),
                    "acceptance_datetime_utc": (
                        acceptance.isoformat() if acceptance is not None else ""
                    ),
                    "available_time_utc": (
                        available_time.isoformat() if available_time is not None else ""
                    ),
                    "available_date": available_date.isoformat() if available_date else "",
                    "available_for_signal_date": (
                        available_for_signal_date.isoformat()
                        if available_for_signal_date is not None
                        else ""
                    ),
                    "primary_document": str(filing.get("primaryDocument") or ""),
                    "primary_doc_description": str(filing.get("primaryDocDescription") or ""),
                    "is_xbrl": _bool_text(filing.get("isXBRL")),
                    "is_inline_xbrl": _bool_text(filing.get("isInlineXBRL")),
                    "source_url": source_url,
                    "source_endpoint": raw_meta.get("source_endpoint", ""),
                    "raw_payload_path": str(path),
                    "raw_payload_sha256": raw_meta.get("checksum_sha256", ""),
                    "source_row_number": row_index,
                    "pit_data_grade": grade,
                    "confidence_level": confidence,
                    "confidence_reason": reason,
                }
            )
    frame = pd.DataFrame(records, columns=list(SEC_FILING_TIMELINE_COLUMNS))
    if not frame.empty:
        frame = frame.sort_values(
            ["ticker", "available_for_signal_date", "accession_number"]
        ).reset_index(drop=True)
    return frame


def build_filing_timeline_csv(
    *,
    sec_companies: SecCompaniesConfig,
    raw_dir: Path,
    start: date,
    end: date,
    output_path: Path,
) -> Path:
    frame = build_filing_timeline(
        sec_companies=sec_companies,
        raw_dir=raw_dir,
        start=start,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def next_us_trading_day(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _availability_fields(
    acceptance: datetime | None,
    filing_date: date,
) -> tuple[datetime | None, str, str, str]:
    if acceptance is not None:
        return (
            acceptance,
            SEC_PIT_BACKTEST_DATA_GRADE,
            "high",
            "matched SEC acceptanceDateTime; daily signal availability delayed "
            "to next US trading day",
        )
    fallback = datetime.combine(
        filing_date,
        time(23, 59, 59),
        tzinfo=_NEW_YORK,
    ).astimezone(UTC)
    return (
        fallback,
        SEC_PIT_CURRENT_HISTORY_GRADE,
        "low",
        "missing acceptanceDateTime; used filing_date 23:59:59 America/New_York fallback",
    )


def _recent_filing_records(data: dict[str, Any]) -> tuple[dict[str, object], ...]:
    filings = data.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict):
        return ()
    lengths = [len(value) for value in recent.values() if isinstance(value, list)]
    if not lengths:
        return ()
    records: list[dict[str, object]] = []
    for index in range(max(lengths)):
        record: dict[str, object] = {}
        for key, values in recent.items():
            if isinstance(values, list) and index < len(values):
                record[key] = values[index]
        records.append(record)
    return tuple(records)


def _raw_manifest_meta(raw_manifest: pd.DataFrame, path: Path) -> dict[str, str]:
    if raw_manifest.empty or "output_path" not in raw_manifest.columns:
        return {}
    matches = raw_manifest.loc[raw_manifest["output_path"].astype(str) == str(path)]
    if matches.empty:
        return {}
    record = matches.iloc[-1].to_dict()
    return {str(key): "" if value is None else str(value) for key, value in record.items()}


def _parse_acceptance_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _date_text(value: object) -> str:
    parsed = _date_or_none(value)
    return parsed.isoformat() if parsed is not None else ""


def _date_or_none(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _bool_text(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value in {0, "0", "false", "False", ""}:
        return "false"
    if value in {1, "1", "true", "True"}:
        return "true"
    return "false"


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"SEC submissions JSON was not an object: {path}")
    return data
