from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from ai_trading_system.config import SecCompaniesConfig
from ai_trading_system.fundamentals.sec_filing_timeline import next_us_trading_day
from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SEC_PIT_CURRENT_HISTORY_GRADE,
    read_sec_pit_raw_manifest,
    sec_pit_companyfacts_path,
)

SEC_XBRL_FACTS_LONG_COLUMNS = (
    "ticker",
    "cik",
    "taxonomy",
    "concept",
    "unit",
    "value",
    "start_date",
    "end_date",
    "fy",
    "fp",
    "form",
    "filed_date",
    "accession_number",
    "frame",
    "period_type",
    "is_duration",
    "is_instant",
    "raw_fact_index",
    "filing_acceptance_datetime_utc",
    "available_time_utc",
    "available_for_signal_date",
    "source_endpoint",
    "raw_payload_path",
    "raw_payload_sha256",
    "join_status",
    "pit_data_grade",
    "confidence_level",
    "confidence_reason",
)

_NEW_YORK = ZoneInfo("America/New_York")


def build_xbrl_facts_long(
    *,
    sec_companies: SecCompaniesConfig,
    raw_dir: Path,
    filing_timeline_path: Path,
    end: date,
) -> pd.DataFrame:
    timeline = _read_timeline(filing_timeline_path)
    timeline_by_key = {
        (str(row["ticker"]).upper(), str(row["accession_number"])): row
        for row in timeline.to_dict(orient="records")
    }
    raw_manifest = read_sec_pit_raw_manifest(raw_dir)
    records: list[dict[str, object]] = []
    for company in sec_companies.companies:
        if not company.active:
            continue
        path = sec_pit_companyfacts_path(raw_dir, company.ticker, company.cik)
        if not path.exists():
            continue
        payload = _load_json(path)
        raw_meta = _raw_manifest_meta(raw_manifest, path)
        fact_index = 0
        for taxonomy, concept, unit, fact in _iter_companyfacts(payload):
            fact_index += 1
            accession = str(fact.get("accn") or "")
            timeline_row = timeline_by_key.get((company.ticker, accession))
            availability = _fact_availability(company.ticker, fact, timeline_row)
            available_for_signal_date = availability["available_for_signal_date"]
            if available_for_signal_date:
                parsed_available = date.fromisoformat(str(available_for_signal_date))
                if parsed_available > end:
                    continue
            records.append(
                {
                    "ticker": company.ticker,
                    "cik": company.cik,
                    "taxonomy": taxonomy,
                    "concept": concept,
                    "unit": unit,
                    "value": fact.get("val", ""),
                    "start_date": _date_text(fact.get("start")),
                    "end_date": _date_text(fact.get("end")),
                    "fy": fact.get("fy", ""),
                    "fp": str(fact.get("fp") or ""),
                    "form": str(fact.get("form") or "").upper(),
                    "filed_date": _date_text(fact.get("filed")),
                    "accession_number": accession,
                    "frame": str(fact.get("frame") or ""),
                    "period_type": classify_xbrl_period_type(fact),
                    "is_duration": str(_is_duration(fact)).lower(),
                    "is_instant": str(not _is_duration(fact)).lower(),
                    "raw_fact_index": fact_index,
                    "filing_acceptance_datetime_utc": availability[
                        "filing_acceptance_datetime_utc"
                    ],
                    "available_time_utc": availability["available_time_utc"],
                    "available_for_signal_date": available_for_signal_date,
                    "source_endpoint": raw_meta.get("source_endpoint", ""),
                    "raw_payload_path": str(path),
                    "raw_payload_sha256": raw_meta.get("checksum_sha256", ""),
                    "join_status": availability["join_status"],
                    "pit_data_grade": availability["pit_data_grade"],
                    "confidence_level": availability["confidence_level"],
                    "confidence_reason": availability["confidence_reason"],
                }
            )
    frame = pd.DataFrame(records, columns=list(SEC_XBRL_FACTS_LONG_COLUMNS))
    if not frame.empty:
        frame = frame.sort_values(
            ["ticker", "concept", "unit", "end_date", "available_time_utc"]
        ).reset_index(drop=True)
    return frame


def build_xbrl_facts_long_csv(
    *,
    sec_companies: SecCompaniesConfig,
    raw_dir: Path,
    filing_timeline_path: Path,
    end: date,
    output_path: Path,
) -> Path:
    frame = build_xbrl_facts_long(
        sec_companies=sec_companies,
        raw_dir=raw_dir,
        filing_timeline_path=filing_timeline_path,
        end=end,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def classify_xbrl_period_type(fact: dict[str, Any]) -> str:
    start = _date_or_none(fact.get("start"))
    end = _date_or_none(fact.get("end"))
    fiscal_period = str(fact.get("fp") or "").upper()
    duration_days = None if start is None or end is None else (end - start).days + 1
    if start is None or duration_days == 1:
        return "instant"
    if fiscal_period == "FY" or (duration_days is not None and duration_days >= 300):
        return "annual"
    if fiscal_period in {"Q1", "Q2", "Q3", "Q4"}:
        if duration_days is not None and 60 <= duration_days <= 120:
            return "quarterly"
        return "ytd"
    if duration_days is not None and 60 <= duration_days <= 120:
        return "quarterly"
    if duration_days is not None and duration_days > 120:
        return "ytd"
    return "unknown"


def _fact_availability(
    ticker: str,
    fact: dict[str, Any],
    timeline_row: dict[str, object] | None,
) -> dict[str, str]:
    if timeline_row is not None:
        grade = str(timeline_row.get("pit_data_grade") or SEC_PIT_BACKTEST_DATA_GRADE)
        confidence = str(timeline_row.get("confidence_level") or "medium")
        acceptance = str(timeline_row.get("acceptance_datetime_utc") or "")
        available_time = str(timeline_row.get("available_time_utc") or "")
        available_for_signal = str(timeline_row.get("available_for_signal_date") or "")
        return {
            "filing_acceptance_datetime_utc": acceptance,
            "available_time_utc": available_time,
            "available_for_signal_date": available_for_signal,
            "join_status": "matched_accession",
            "pit_data_grade": grade,
            "confidence_level": confidence,
            "confidence_reason": (
                "matched companyfacts accn to SEC submissions accession"
                if grade == SEC_PIT_BACKTEST_DATA_GRADE
                else "matched accession but timeline used fallback availability"
            ),
        }

    filed_date = _date_or_none(fact.get("filed"))
    if filed_date is not None:
        fallback = datetime.combine(
            filed_date,
            time(23, 59, 59),
            tzinfo=_NEW_YORK,
        ).astimezone(UTC)
        return {
            "filing_acceptance_datetime_utc": "",
            "available_time_utc": fallback.isoformat(),
            "available_for_signal_date": next_us_trading_day(fallback.date()).isoformat(),
            "join_status": "filed_date_fallback",
            "pit_data_grade": SEC_PIT_CURRENT_HISTORY_GRADE,
            "confidence_level": "low",
            "confidence_reason": (
                f"{ticker} fact accession did not join timeline; used filed_date fallback"
            ),
        }
    return {
        "filing_acceptance_datetime_utc": "",
        "available_time_utc": "",
        "available_for_signal_date": "",
        "join_status": "unmatched",
        "pit_data_grade": SEC_PIT_CURRENT_HISTORY_GRADE,
        "confidence_level": "low",
        "confidence_reason": f"{ticker} fact has no joined filing and no filed date",
    }


def _iter_companyfacts(
    payload: dict[str, Any],
) -> tuple[tuple[str, str, str, dict[str, Any]], ...]:
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        return ()
    rows: list[tuple[str, str, str, dict[str, Any]]] = []
    for taxonomy, taxonomy_value in facts.items():
        if not isinstance(taxonomy_value, dict):
            continue
        for concept, concept_value in taxonomy_value.items():
            if not isinstance(concept_value, dict):
                continue
            units = concept_value.get("units")
            if not isinstance(units, dict):
                continue
            for unit, unit_facts in units.items():
                if not isinstance(unit_facts, list):
                    continue
                for fact in unit_facts:
                    if isinstance(fact, dict):
                        rows.append((str(taxonomy), str(concept), str(unit), fact))
    return tuple(rows)


def _read_timeline(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _raw_manifest_meta(raw_manifest: pd.DataFrame, path: Path) -> dict[str, str]:
    if raw_manifest.empty or "output_path" not in raw_manifest.columns:
        return {}
    matches = raw_manifest.loc[raw_manifest["output_path"].astype(str) == str(path)]
    if matches.empty:
        return {}
    record = matches.iloc[-1].to_dict()
    return {str(key): "" if value is None else str(value) for key, value in record.items()}


def _is_duration(fact: dict[str, Any]) -> bool:
    return (
        _date_or_none(fact.get("start")) is not None and _date_or_none(fact.get("end")) is not None
    )


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


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"SEC companyfacts JSON was not an object: {path}")
    return data
