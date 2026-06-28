from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests


@dataclass(frozen=True)
class FredConnector:
    """Small FRED CSV/API connector used by the free PIT source ingestion layer."""

    timeout_seconds: int = 30
    base_csv_url: str = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    base_api_url: str = "https://api.stlouisfed.org/fred"
    api_key: str | None = None

    def series_csv_url(self, series_id: str) -> str:
        return f"{self.base_csv_url}?id={series_id}"

    def read_cached_series(
        self,
        path: Path,
        *,
        series_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        frame = pd.read_csv(path)
        missing = {"date", "series", "value"} - set(frame.columns)
        if missing:
            raise ValueError(f"FRED cache missing required columns: {sorted(missing)}")
        if series_ids is not None:
            frame = frame.loc[frame["series"].isin(series_ids)].copy()
        return _standardize_series_frame(frame)

    def fetch_series_observations(self, series_id: str) -> pd.DataFrame:
        response = requests.get(self.series_csv_url(series_id), timeout=self.timeout_seconds)
        response.raise_for_status()
        raw = pd.read_csv(StringIO(response.text))
        if raw.empty:
            return _empty_series_frame()
        date_column = raw.columns[0]
        value_column = raw.columns[-1]
        frame = raw.rename(columns={date_column: "date", value_column: "value"})
        frame["series"] = series_id
        return _standardize_series_frame(frame[["date", "series", "value"]])

    def series_metadata_url(self, series_id: str) -> str:
        return (
            f"{self.base_api_url}/series?series_id={series_id}&file_type=json"
            f"{self._api_key_query()}"
        )

    def vintage_dates_url(self, series_id: str) -> str:
        return (
            f"{self.base_api_url}/series/vintagedates?series_id={series_id}&file_type=json"
            f"{self._api_key_query()}"
        )

    def _api_key_query(self) -> str:
        return f"&api_key={self.api_key}" if self.api_key else ""


@dataclass(frozen=True)
class AlfredVintageConnector:
    """ALFRED vintage-aware access contract for revision-sensitive macro series."""

    api_key: str | None
    timeout_seconds: int = 30
    base_api_url: str = "https://api.stlouisfed.org/fred"

    def require_api_key(self) -> str:
        if not self.api_key:
            raise ValueError("ALFRED vintage reads require a FRED API key.")
        return self.api_key

    def observations_url(self, series_id: str, *, as_of_date: str) -> str:
        api_key = self.require_api_key()
        return (
            f"{self.base_api_url}/series/observations?series_id={series_id}"
            f"&vintage_dates={as_of_date}&file_type=json&api_key={api_key}"
        )

    def vintage_dates_url(self, series_id: str) -> str:
        api_key = self.require_api_key()
        return (
            f"{self.base_api_url}/series/vintagedates?series_id={series_id}"
            f"&file_type=json&api_key={api_key}"
        )


@dataclass(frozen=True)
class CboeVixConnector:
    """Free VIX index connector and local price-cache adapter."""

    source_id: str = "cboe_vix_historical"

    def from_price_cache(self, path: Path, *, ticker: str = "^VIX") -> pd.DataFrame:
        frame = pd.read_csv(path)
        missing = {"date", "ticker", "adj_close", "close"} - set(frame.columns)
        if missing:
            raise ValueError(f"price cache missing required VIX columns: {sorted(missing)}")
        vix = frame.loc[frame["ticker"] == ticker].copy()
        if vix.empty:
            return _empty_vix_frame()
        vix["date"] = pd.to_datetime(vix["date"], errors="coerce").dt.date.astype("string")
        vix["vix_level"] = pd.to_numeric(vix["adj_close"], errors="coerce")
        vix["close"] = pd.to_numeric(vix["close"], errors="coerce")
        vix = vix.loc[vix["date"].notna() & vix["vix_level"].notna()].copy()
        vix["source_id"] = self.source_id
        vix["provider"] = "Cboe VIX via local price cache"
        vix["known_at"] = vix["date"]
        vix["available_at"] = vix["date"]
        vix["PIT_status"] = "PIT_APPROVED"
        vix["revision_risk"] = "none"
        return vix[
            [
                "date",
                "vix_level",
                "close",
                "source_id",
                "provider",
                "known_at",
                "available_at",
                "PIT_status",
                "revision_risk",
            ]
        ].sort_values("date")


@dataclass(frozen=True)
class OfficialMacroCalendarConnector:
    """Normalizer for official macro calendar rows.

    The class intentionally separates calendar dates from PIT availability. When
    `known_at` or `source_published_at` is not supplied, rows remain usable only
    as diagnostic calendar risk.
    """

    source_id: str = "official_macro_calendar"

    def normalize_events(self, events: list[dict[str, Any]] | pd.DataFrame) -> pd.DataFrame:
        frame = pd.DataFrame(events)
        if frame.empty:
            return _empty_calendar_frame()
        if "event_date" not in frame.columns:
            raise ValueError("macro calendar events must include event_date")
        if "event_type" not in frame.columns:
            raise ValueError("macro calendar events must include event_type")
        normalized = frame.copy()
        normalized["event_date"] = pd.to_datetime(
            normalized["event_date"], errors="coerce"
        ).dt.date.astype("string")
        normalized["event_name"] = normalized.get("event_name", normalized["event_type"]).fillna(
            normalized["event_type"]
        )
        for column in (
            "scheduled_release_time",
            "source_published_at",
            "known_at",
            "available_at",
            "timezone",
            "source_id",
            "provider",
        ):
            if column not in normalized.columns:
                normalized[column] = ""
        normalized["timezone"] = normalized["timezone"].replace("", "America/New_York")
        normalized["source_id"] = normalized["source_id"].replace("", self.source_id)
        normalized["provider"] = normalized["provider"].replace("", "official_macro_calendar")
        missing_known_at = normalized["known_at"].isna() | (
            normalized["known_at"].astype(str) == ""
        )
        normalized.loc[missing_known_at, "known_at"] = normalized.loc[
            missing_known_at, "event_date"
        ]
        missing_available_at = normalized["available_at"].isna() | (
            normalized["available_at"].astype(str) == ""
        )
        normalized.loc[missing_available_at, "available_at"] = normalized.loc[
            missing_available_at, "known_at"
        ]
        missing_published_at = normalized["source_published_at"].isna() | (
            normalized["source_published_at"].astype(str) == ""
        )
        normalized["PIT_status"] = "PIT_APPROVED"
        normalized.loc[missing_published_at, "PIT_status"] = "PIT_WARNING"
        normalized["allowed_usage"] = "macro_event_calendar_free_v1"
        normalized.loc[missing_published_at, "allowed_usage"] = "diagnostic_only"
        normalized["blocked_usage"] = "promotion,paper_shadow,production,broker"
        normalized["revision_policy"] = normalized.get(
            "revision_policy", "calendar_only_release_values_excluded"
        )
        return normalized[
            [
                "event_date",
                "event_type",
                "event_name",
                "scheduled_release_time",
                "source_published_at",
                "known_at",
                "available_at",
                "timezone",
                "source_id",
                "provider",
                "PIT_status",
                "revision_policy",
                "allowed_usage",
                "blocked_usage",
            ]
        ].sort_values(["event_date", "event_type", "event_name"])


def _standardize_series_frame(frame: pd.DataFrame) -> pd.DataFrame:
    clean = frame.copy()
    clean["date"] = pd.to_datetime(clean["date"], errors="coerce").dt.date.astype("string")
    clean["value"] = pd.to_numeric(clean["value"], errors="coerce")
    clean = clean.loc[clean["date"].notna() & clean["series"].notna()].copy()
    clean["source_id"] = "fred_market_series"
    clean["provider"] = "Federal Reserve Economic Data"
    clean["known_at"] = clean["date"]
    clean["available_at"] = clean["date"]
    clean["PIT_status"] = "PIT_APPROVED"
    clean["revision_risk"] = "low"
    return clean[
        [
            "date",
            "series",
            "value",
            "source_id",
            "provider",
            "known_at",
            "available_at",
            "PIT_status",
            "revision_risk",
        ]
    ].sort_values(["series", "date"])


def _empty_series_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "series",
            "value",
            "source_id",
            "provider",
            "known_at",
            "available_at",
            "PIT_status",
            "revision_risk",
        ]
    )


def _empty_vix_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "vix_level",
            "close",
            "source_id",
            "provider",
            "known_at",
            "available_at",
            "PIT_status",
            "revision_risk",
        ]
    )


def _empty_calendar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "event_date",
            "event_type",
            "event_name",
            "scheduled_release_time",
            "source_published_at",
            "known_at",
            "available_at",
            "timezone",
            "source_id",
            "provider",
            "PIT_status",
            "revision_policy",
            "allowed_usage",
            "blocked_usage",
        ]
    )
