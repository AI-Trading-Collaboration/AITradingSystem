"""Long-history research data for QQQ-type questions (TRADING-2567 S1).

FRED series (Nasdaq-100 total return and price, 3-month T-bill, VIX/VXN/VXO) are downloaded into
a content-addressed snapshot under ``data/raw/research/qqq_long_history/<snapshot_id>/``. Research
code reads the derived daily frame through ``load_daily``, which refuses a snapshot whose checksum
does not match its manifest or whose check is missing, stale or FAIL. This check is the data-quality
gate for these snapshots; they are not part of the daily price cache or ``aits validate-data``.

The Nasdaq series may not be redistributed: snapshots stay on this machine, are never committed,
and the commands below print summaries only, never rows. Run them from the main checkout, where
research code reads the snapshot by default; ``--prices-path`` points the overlap check at the
freshest local ETF prices (read only).

    python -m ai_trading_system.research_data.qqq_long_history fetch [--end YYYY-MM-DD]
    python -m ai_trading_system.research_data.qqq_long_history check [--snapshot ID]
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import pandas as pd
import typer
from pydantic import BaseModel, ConfigDict, Field

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import FredRateProvider, RateDataProvider, RateRequest
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SPEC_PATH = PROJECT_ROOT / "config" / "research" / "qqq_long_history.yaml"
DEFAULT_SNAPSHOT_ROOT = PROJECT_ROOT / "data" / "raw" / "research" / "qqq_long_history"
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
SNAPSHOT_SCHEMA_VERSION = "qqq_long_history_snapshot.v1"
CHECK_SCHEMA_VERSION = "qqq_long_history_check.v3"
LATEST_POINTER = "LATEST"
SERIES_FILE = "series.csv"
MANIFEST_FILE = "manifest.json"
CHECK_JSON_FILE = "check.json"
CHECK_MD_FILE = "check.md"
# Convention for annualizing daily statistics; not a tunable threshold.
TRADING_DAYS_PER_YEAR = 252
# Day-count bases: T-bill discount rates accrue actual/360, fund expense ratios actual/365.
CASH_DAY_COUNT = 360
EXPENSE_DAY_COUNT = 365
# Reports list at most this many dates per issue so a summary never turns into a data dump.
MAX_LISTED_DATES = 10

Severity = Literal["FAIL", "WARN"]
SeriesRole = Literal[
    "index_total_return",
    "index_price",
    "cash_rate",
    "volatility_vix",
    "volatility_vxn",
    "volatility_vxo",
]
VOLATILITY_COLUMNS: dict[str, str] = {
    "volatility_vix": "vix",
    "volatility_vxn": "vxn",
    "volatility_vxo": "vxo",
}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SeriesSpec(_Strict):
    series_id: str = Field(min_length=1)
    role: SeriesRole
    kind: Literal["index_level", "rate_percent", "volatility_index"]
    expected_first_date: date
    discontinued: bool = False


class SyntheticFundSpec(_Strict):
    fund_id: str = Field(pattern=r"^[a-z0-9_]+$")
    leverage: float = Field(gt=1)
    expense_ratio_annual: float = Field(ge=0)
    calibration_ticker: str = Field(min_length=1)


class DerivationSpec(_Strict):
    stale_level_min_other_move: float = Field(gt=0)
    qqq_expense_ratio_annual: float = Field(ge=0)
    synthetic_funds: tuple[SyntheticFundSpec, ...] = Field(min_length=1)
    financing_spread_annual: float = Field(ge=0)


class CheckSpec(_Strict):
    first_date_tolerance_days: int = Field(ge=0)
    suspicious_index_daily_return_abs: float = Field(gt=0)
    max_calendar_gap_days: int = Field(ge=1)
    known_long_gaps: tuple[str, ...] = ()
    rate_percent_range: tuple[float, float]
    volatility_index_range: tuple[float, float]
    min_daily_dividend_component: float
    dividend_window_trading_days: int = Field(ge=2)
    min_window_dividend_component: float = Field(lt=0)
    stale_last_observation_days: int = Field(ge=0)
    overlap_tickers: dict[str, str]
    max_abs_annual_tracking_difference: dict[str, float]


class SourceSpec(_Strict):
    source_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    endpoint: str = Field(min_length=1)
    redistribution_allowed: bool
    external_llm_allowed: bool


class LongHistorySpec(_Strict):
    schema_version: Literal["qqq_long_history_spec.v1"]
    spec_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    owner_decision: str = Field(min_length=1)
    requirement: str = Field(min_length=1)
    policy_metadata: dict[str, str]
    request_start: date
    source: SourceSpec
    series: tuple[SeriesSpec, ...] = Field(min_length=1)
    derivation: DerivationSpec
    checks: CheckSpec

    def series_for(self, role: str) -> SeriesSpec:
        for item in self.series:
            if item.role == role:
                return item
        raise KeyError(f"spec has no series with role {role}")


@dataclass(frozen=True)
class LoadedSpec:
    spec: LongHistorySpec
    path: Path
    sha256: str


class SnapshotIntegrityError(ValueError):
    """The snapshot files do not match their manifest."""


class SnapshotNotCheckedError(ValueError):
    """The snapshot has no passing check for the current spec."""


def load_spec(path: Path = DEFAULT_SPEC_PATH) -> LoadedSpec:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"spec root must be a mapping: {path}")
    spec = LongHistorySpec.model_validate(raw)
    roles = [item.role for item in spec.series]
    if len(set(roles)) != len(roles):
        raise ValueError("each series role may appear only once")
    for required in ("index_total_return", "index_price", "cash_rate"):
        spec.series_for(required)
    fund_ids = {fund.fund_id for fund in spec.derivation.synthetic_funds}
    for column in spec.checks.overlap_tickers.values():
        if column != "qqq_proxy" and column not in fund_ids:
            raise ValueError(f"overlap column {column} is not qqq_proxy or a synthetic fund")
    return LoadedSpec(spec=spec, path=path, sha256=_sha256_bytes(path.read_bytes()))


# --- snapshot ---------------------------------------------------------------------------------


def fetch_snapshot(
    loaded: LoadedSpec,
    *,
    end: date,
    provider: RateDataProvider,
    root: Path = DEFAULT_SNAPSHOT_ROOT,
    downloaded_at: datetime | None = None,
) -> Path:
    """Download the spec's series and write (or reuse) the content-addressed snapshot."""

    spec = loaded.spec
    series_ids = [item.series_id for item in spec.series]
    request = RateRequest(series_ids=series_ids, start=spec.request_start, end=end)
    raw = provider.download_rates(request)
    frame = _normalize_series_frame(raw)
    missing = sorted(set(series_ids) - set(frame["series"]))
    if missing:
        raise ValueError(f"provider returned no rows for: {', '.join(missing)}")
    payload = _series_csv_bytes(frame)
    digest = _sha256_bytes(payload)
    snapshot_id = f"qlh-{end:%Y%m%d}-{digest[:12]}"
    snapshot_dir = root / snapshot_id
    series_path = snapshot_dir / SERIES_FILE
    if series_path.exists() and _sha256_bytes(series_path.read_bytes()) == digest:
        _write_text(root / LATEST_POINTER, snapshot_id + "\n")
        return snapshot_dir
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    series_path.write_bytes(payload)
    manifest = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "spec_id": spec.spec_id,
        "spec_sha256": loaded.sha256,
        "owner_decision": spec.owner_decision,
        "source_id": spec.source.source_id,
        "provider": spec.source.provider,
        "endpoint": spec.source.endpoint,
        "request_parameters": {
            "series_ids": series_ids,
            "start": spec.request_start.isoformat(),
            "end": end.isoformat(),
        },
        "downloaded_at": (downloaded_at or datetime.now(tz=UTC)).isoformat(),
        "series_csv_sha256": digest,
        "row_count": int(len(frame)),
        "series": _series_summary(frame),
        "data_policy": {
            "redistribution_allowed": spec.source.redistribution_allowed,
            "external_llm_allowed": spec.source.external_llm_allowed,
            "local_only": True,
        },
    }
    _write_text(snapshot_dir / MANIFEST_FILE, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    _write_text(root / LATEST_POINTER, snapshot_id + "\n")
    return snapshot_dir


def resolve_snapshot_dir(
    root: Path = DEFAULT_SNAPSHOT_ROOT, snapshot_id: str | None = None
) -> Path:
    if snapshot_id is None:
        pointer = root / LATEST_POINTER
        if not pointer.exists():
            raise FileNotFoundError(f"no snapshot yet under {root}; run fetch first")
        snapshot_id = pointer.read_text(encoding="utf-8").strip()
    snapshot_dir = root / snapshot_id
    if not (snapshot_dir / MANIFEST_FILE).exists():
        raise FileNotFoundError(f"snapshot {snapshot_id} has no manifest under {root}")
    return snapshot_dir


def read_snapshot(snapshot_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    """Return the manifest and the long series frame after verifying the checksum."""

    manifest = json.loads((snapshot_dir / MANIFEST_FILE).read_text(encoding="utf-8"))
    payload = (snapshot_dir / SERIES_FILE).read_bytes()
    if _sha256_bytes(payload) != manifest.get("series_csv_sha256"):
        raise SnapshotIntegrityError(
            f"{snapshot_dir.name}: {SERIES_FILE} does not match the manifest checksum"
        )
    frame = pd.read_csv(snapshot_dir / SERIES_FILE, dtype={"date": str, "series": str})
    return manifest, frame


# --- derivation -------------------------------------------------------------------------------


def derive_daily(frame: pd.DataFrame, spec: LongHistorySpec) -> pd.DataFrame:
    """Daily research frame on the Nasdaq-100 price-index calendar.

    From the first total-return date on, a price-index date without a total-return level is dropped
    from the calendar (FRED carries the price index over some exchange holidays), so returns stay
    defined on every remaining date. A level repeated from the previous date while the other index
    moved is stale and is rebuilt from the other index's move (see ``repair_stale_levels``); the
    ``*_repaired`` columns mark those dates. Returns on day t cover the previous index date to t.
    Cash and financing accrue at the T-bill rate dated on or before the previous index date.
    ``cash_rate_annual`` on day t is the rate dated t, which FRED publishes the next business day: a
    signal that reads it lags it by one day.
    """

    wide = _wide_series(frame)
    price_id = spec.series_for("index_price").series_id
    tr_id = spec.series_for("index_total_return").series_id
    cash_id = spec.series_for("cash_rate").series_id

    price = wide[price_id].dropna()
    dropped = price_only_dates(price.index, wide[tr_id].dropna().index)
    price = price.drop(dropped)
    calendar = pd.DatetimeIndex(price.index, name="date")
    out = pd.DataFrame(index=calendar)
    out["calendar_days"] = calendar.to_series().diff().dt.days.to_numpy()
    repaired = repair_stale_levels(
        price.astype(float).tolist(),
        wide[tr_id].reindex(calendar).astype(float).tolist(),
        spec.derivation.stale_level_min_other_move,
    )
    out["ndx_price"] = repaired.price
    out["ndx_tr"] = repaired.total_return
    out["price_repaired"] = repaired.price_repaired
    out["tr_repaired"] = repaired.total_return_repaired
    out["ndx_price_return"] = out["ndx_price"] / out["ndx_price"].shift(1) - 1
    out["ndx_tr_return"] = out["ndx_tr"] / out["ndx_tr"].shift(1) - 1

    rate = wide[cash_id].dropna() / 100
    rate_on_calendar = (
        rate.reindex(calendar.union(pd.DatetimeIndex(rate.index)))
        .sort_index()
        .ffill()
        .reindex(calendar)
    )
    out["cash_rate_annual"] = rate_on_calendar.to_numpy()
    prior_rate = out["cash_rate_annual"].shift(1)
    days = out["calendar_days"]
    out["cash_return"] = prior_rate * days / CASH_DAY_COUNT

    derivation = spec.derivation
    out["qqq_proxy_return"] = (
        out["ndx_tr_return"] - derivation.qqq_expense_ratio_annual * days / EXPENSE_DAY_COUNT
    )
    for fund in derivation.synthetic_funds:
        financing = (fund.leverage - 1) * (prior_rate + derivation.financing_spread_annual)
        out[f"{fund.fund_id}_return"] = (
            fund.leverage * out["ndx_tr_return"]
            - financing * days / CASH_DAY_COUNT
            - fund.expense_ratio_annual * days / EXPENSE_DAY_COUNT
        )
    for item in spec.series:
        column = VOLATILITY_COLUMNS.get(item.role)
        if column is None:
            continue
        if item.series_id in wide.columns:
            out[column] = wide[item.series_id].reindex(calendar).to_numpy()
        else:
            out[column] = math.nan
    return out


@dataclass(frozen=True)
class RepairedLevels:
    price: list[float]
    total_return: list[float]
    price_repaired: list[bool]
    total_return_repaired: list[bool]


def repair_stale_levels(
    price: Sequence[float], total_return: Sequence[float], min_other_move: float
) -> RepairedLevels:
    """Rebuild index levels that were carried over from the previous date.

    A total-return level equal to the previous one while the price index moved more than
    ``min_other_move`` cannot be right (reinvested dividends only add to the price move), and a
    price level equal to the previous one while the total-return index moved that much is the same
    error the other way. Such a level is replaced by the previous (repaired) level times the other
    index's move on that date, so the day's return comes from the other index and the following
    date, whose level is real again, gets the remainder. Levels are compared as given: a repeated
    value on both indexes is a closed session and stays as it is.
    """

    price_out = [float(value) for value in price]
    tr_out = [float(value) for value in total_return]
    price_flags = [False] * len(price_out)
    tr_flags = [False] * len(tr_out)
    for i in range(1, len(price_out)):
        p_prev, p_now = float(price[i - 1]), float(price[i])
        t_prev, t_now = float(total_return[i - 1]), float(total_return[i])
        if math.isnan(t_prev) or math.isnan(t_now):
            continue
        price_move = p_now / p_prev - 1
        tr_move = t_now / t_prev - 1
        if t_now == t_prev and abs(price_move) > min_other_move:
            tr_out[i] = tr_out[i - 1] * p_now / p_prev
            tr_flags[i] = True
        elif p_now == p_prev and abs(tr_move) > min_other_move:
            price_out[i] = price_out[i - 1] * t_now / t_prev
            price_flags[i] = True
    return RepairedLevels(price_out, tr_out, price_flags, tr_flags)


def price_only_dates(price_dates: pd.Index, total_return_dates: pd.Index) -> pd.DatetimeIndex:
    """Price-index dates on or after the first total-return date that have no total-return level."""

    price_index = pd.DatetimeIndex(price_dates)
    if len(total_return_dates) == 0:
        return pd.DatetimeIndex([])
    total_return_index = pd.DatetimeIndex(total_return_dates)
    after = price_index[price_index >= total_return_index.min()]
    return pd.DatetimeIndex(after[~after.isin(total_return_index)])


def _wide_series(frame: pd.DataFrame) -> pd.DataFrame:
    wide = frame.pivot(index="date", columns="series", values="value")
    wide.index = pd.to_datetime(wide.index)
    return wide.sort_index()


# --- check ------------------------------------------------------------------------------------


@dataclass
class _Issues:
    items: list[dict[str, Any]]

    def add(self, severity: Severity, code: str, subject: str, message: str, **extra: Any) -> None:
        self.items.append(
            {"severity": severity, "code": code, "subject": subject, "message": message, **extra}
        )


def check_snapshot(
    snapshot_dir: Path,
    loaded: LoadedSpec,
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    checked_at: datetime | None = None,
) -> dict[str, Any]:
    """Run the snapshot checks, write check.json and check.md, and return the report."""

    spec = loaded.spec
    checks = spec.checks
    manifest, frame = read_snapshot(snapshot_dir)
    issues = _Issues([])
    request_end = date.fromisoformat(manifest["request_parameters"]["end"])

    series_rows: dict[str, dict[str, Any]] = {}
    for item in spec.series:
        rows = frame[frame["series"] == item.series_id].sort_values("date")
        record: dict[str, Any] = {
            "role": item.role,
            "row_count": int(len(rows)),
            "expected_first_date": item.expected_first_date.isoformat(),
        }
        series_rows[item.series_id] = record
        if rows.empty:
            issues.add("FAIL", "series_missing", item.series_id, "no observations in the snapshot")
            continue
        first = date.fromisoformat(rows["date"].iloc[0])
        last = date.fromisoformat(rows["date"].iloc[-1])
        record.update(first_date=first.isoformat(), last_date=last.isoformat())
        late_by = (first - item.expected_first_date).days
        if late_by > checks.first_date_tolerance_days:
            issues.add(
                "FAIL",
                "first_date_late",
                item.series_id,
                f"first observation {first} is {late_by} days after the expected "
                f"{item.expected_first_date}",
            )
        if rows["date"].duplicated().any():
            issues.add("FAIL", "duplicate_dates", item.series_id, "repeated dates in the series")
        values = pd.to_numeric(rows["value"], errors="coerce")
        if values.isna().any():
            issues.add("FAIL", "non_numeric_values", item.series_id, "non-numeric values present")
        if not item.discontinued and (request_end - last).days > checks.stale_last_observation_days:
            issues.add(
                "WARN",
                "series_stale",
                item.series_id,
                f"last observation {last} is more than {checks.stale_last_observation_days} days "
                f"before the request end {request_end}",
            )
        _check_ranges(item, rows["date"], values, checks, issues)

    if any(issue["severity"] == "FAIL" for issue in issues.items):
        report = _check_report(loaded, manifest, series_rows, issues, {}, {}, checked_at)
        _write_check(snapshot_dir, report)
        return report

    daily = derive_daily(frame, spec)
    calendar_summary = _check_calendar(frame, daily, spec, issues)
    overlap = _check_overlap(daily, prices_path, checks, issues)
    report = _check_report(
        loaded, manifest, series_rows, issues, calendar_summary, overlap, checked_at
    )
    _write_check(snapshot_dir, report)
    return report


def _check_ranges(
    item: SeriesSpec,
    dates: pd.Series,
    values: pd.Series,
    checks: CheckSpec,
    issues: _Issues,
) -> None:
    if item.kind == "index_level":
        if (values <= 0).any():
            issues.add("FAIL", "nonpositive_index_level", item.series_id, "index level <= 0")
            return
        returns = values / values.shift(1) - 1
        flagged = dates[returns.abs() > checks.suspicious_index_daily_return_abs]
        if not flagged.empty:
            issues.add(
                "WARN",
                "suspicious_index_return",
                item.series_id,
                f"{len(flagged)} daily moves above {checks.suspicious_index_daily_return_abs:.0%}",
                dates=flagged.head(MAX_LISTED_DATES).tolist(),
            )
        return
    low, high = (
        checks.rate_percent_range if item.kind == "rate_percent" else checks.volatility_index_range
    )
    flagged = dates[(values < low) | (values > high)]
    if not flagged.empty:
        code = "rate_out_of_range" if item.kind == "rate_percent" else "volatility_out_of_range"
        issues.add(
            "WARN",
            code,
            item.series_id,
            f"{len(flagged)} values outside [{low}, {high}]",
            dates=flagged.head(MAX_LISTED_DATES).tolist(),
        )


def _check_calendar(
    frame: pd.DataFrame, daily: pd.DataFrame, spec: LongHistorySpec, issues: _Issues
) -> dict[str, Any]:
    checks = spec.checks
    known = {gap.strip() for gap in checks.known_long_gaps}
    dates = daily.index
    gaps = daily["calendar_days"]
    long_gaps = [
        f"{_iso(dates[i - 1])}..{_iso(dates[i])}"
        for i in range(1, len(dates))
        if gaps.iloc[i] > checks.max_calendar_gap_days
    ]
    unexplained = [gap for gap in long_gaps if gap not in known]
    if unexplained:
        issues.add(
            "WARN",
            "long_calendar_gap",
            "calendar",
            f"{len(unexplained)} gaps longer than {checks.max_calendar_gap_days} calendar days",
            dates=unexplained[:MAX_LISTED_DATES],
        )

    wide = _wide_series(frame)
    tr_id = spec.series_for("index_total_return").series_id
    price = wide[spec.series_for("index_price").series_id].dropna()
    dropped = price_only_dates(price.index, wide[tr_id].dropna().index)
    # An unchanged level is the price index carried over a closed session; a moved level means a
    # trading day without a total-return level, whose move the next date then absorbs.
    previous = price.shift(1).reindex(dropped)
    moved = dropped[price.reindex(dropped).to_numpy() != previous.to_numpy()]
    if len(moved):
        issues.add(
            "WARN",
            "total_return_missing_on_trading_dates",
            tr_id,
            f"{len(moved)} price-index dates with a moved level have no total-return level; "
            "dropped from the calendar",
            dates=[_iso(item) for item in moved[:MAX_LISTED_DATES]],
        )

    tr_only = pd.DatetimeIndex(wide[tr_id].dropna().index).difference(pd.DatetimeIndex(price.index))
    calendar_index = pd.DatetimeIndex(daily.index)
    tr_repaired = calendar_index[daily["tr_repaired"].to_numpy(dtype=bool)]
    price_repaired = calendar_index[daily["price_repaired"].to_numpy(dtype=bool)]
    if len(tr_repaired) or len(price_repaired):
        issues.add(
            "WARN",
            "stale_levels_repaired",
            "calendar",
            f"{len(tr_repaired)} total-return and {len(price_repaired)} price-index levels "
            "repeated the previous date while the other index moved; rebuilt from the other "
            "index's move",
            dates=[_iso(item) for item in tr_repaired.union(price_repaired)[:MAX_LISTED_DATES]],
        )

    tr_start = daily["ndx_tr"].first_valid_index()
    dividend = (daily["ndx_tr_return"] - daily["ndx_price_return"]).dropna()
    negative = dividend < checks.min_daily_dividend_component
    windows, window_dates = _dividend_windows(daily, checks, issues)
    tr_returns = daily["ndx_tr_return"]
    years = pd.DatetimeIndex(dividend.index).year
    by_year = [
        {
            "year": year,
            "trading_days": int((years == year).sum()),
            "negative_component_days": int(negative[years == year].sum()),
            "total_return_repaired_days": int((tr_repaired.year == year).sum()),
            "price_repaired_days": int((price_repaired.year == year).sum()),
            "implied_dividend_yield_annual": _round(
                dividend[years == year].mean() * TRADING_DAYS_PER_YEAR
            ),
            "dividend_component_daily_std": _round(dividend[years == year].std()),
            **_noise_share_fields(tr_returns, dividend[years == year]),
        }
        for year in sorted(set(years.tolist()))
    ]
    in_windows = dividend[dividend.index.isin(window_dates)]
    noise_attribution = (
        {
            "trading_days": int(len(in_windows)),
            "dividend_component_daily_std": _round(in_windows.std()),
            **_noise_share_fields(tr_returns, in_windows),
        }
        if len(in_windows)
        else {}
    )

    cash = spec.series_for("cash_rate")
    rate_dates = daily["cash_rate_annual"].notna()
    return {
        "calendar_dates": int(len(daily)),
        "first_date": _iso(dates[0]) if len(dates) else None,
        "last_date": _iso(dates[-1]) if len(dates) else None,
        "long_gaps": long_gaps[:MAX_LISTED_DATES],
        "total_return_first_date": None if tr_start is None else _iso(tr_start),
        "price_only_dates_dropped": int(len(dropped)),
        "price_only_dates_unchanged": int(len(dropped) - len(moved)),
        "price_only_dates": [_iso(item) for item in dropped[:MAX_LISTED_DATES]],
        "total_return_only_dates_ignored": int(len(tr_only)),
        "total_return_only_dates": [_iso(item) for item in tr_only[:MAX_LISTED_DATES]],
        "total_return_repaired_days": int(len(tr_repaired)),
        "price_repaired_days": int(len(price_repaired)),
        "implied_dividend_yield_annual": _round(dividend.mean() * TRADING_DAYS_PER_YEAR),
        "negative_component_days": int(negative.sum()),
        "dividend_component_lag1_autocorrelation": (
            _round(dividend.autocorr(lag=1)) if len(dividend) > 2 else None
        ),
        "min_daily_dividend_component": _round(dividend.min()) if len(dividend) else None,
        "dividend_windows": windows,
        "noise_attribution": noise_attribution,
        "dividend_by_year": by_year,
        "cash_rate_series": cash.series_id,
        "cash_rate_dates_covered": int(rate_dates.sum()),
    }


def _dividend_windows(
    daily: pd.DataFrame, checks: CheckSpec, issues: _Issues
) -> tuple[dict[str, Any], pd.DatetimeIndex]:
    """Change of the total-return / price-index ratio over rolling windows of trading days.

    Reinvested dividends only ever raise the ratio. Level noise on single days in either index
    moves a window by at most the size of that noise and reverses in the next window; a
    misalignment or drift that research would inherit accumulates over the window instead.
    Also returns the dates whose returns fall inside a window below the limit.
    """

    ratio = (daily["ndx_tr"] / daily["ndx_price"]).dropna()
    window = checks.dividend_window_trading_days
    change = (ratio / ratio.shift(window) - 1).dropna()
    if change.empty:
        return {}, pd.DatetimeIndex([])
    below = (change < checks.min_window_dividend_component).to_numpy()
    covered = [False] * len(ratio)
    for position in range(len(below)):
        if below[position]:
            covered[position + 1 : position + window + 1] = [True] * window
    window_dates = pd.DatetimeIndex(ratio.index[covered])
    starts = pd.DatetimeIndex(ratio.index[: len(ratio) - window])
    ends = pd.DatetimeIndex(change.index)
    # Consecutive offending windows merge into one span: first window's start .. last window's end.
    spans: list[str] = []
    first = 0
    for position, flag in enumerate(below):
        if not flag:
            first = position + 1
        elif position + 1 == len(below) or not below[position + 1]:
            spans.append(f"{_iso(starts[first])}..{_iso(ends[position])}")
    if spans:
        issues.add(
            "WARN",
            "dividend_window_negative",
            "calendar",
            f"{int(below.sum())} windows of {window} trading days where total return trails price "
            f"return by more than {-checks.min_window_dividend_component:.2%}",
            dates=spans[:MAX_LISTED_DATES],
        )
    summary = {
        "trading_days": window,
        "min_change": _round(float(change.min())),
        "min_change_end_date": _iso(change.idxmin()),
        "windows_below_limit": int(below.sum()),
        "spans_below_limit": int(len(spans)),
    }
    return summary, window_dates


def total_return_noise_share(
    total_return_returns: pd.Series, dividend_component: pd.Series
) -> tuple[float, float] | None:
    """How much of the day-to-day gap between the two indexes sits in the total-return index.

    With d = total-return return minus price-index return on the same dates, the slope of the
    total-return return on d is 1 when the gap is level noise in the total-return index (its return
    is the common move plus d) and 0 when the noise is in the price index (the total-return return
    is the common move alone). Equal noise in both, or one index recorded at a different time of
    day, lands near 0.5. Returns the slope and its OLS standard error, or None with fewer than
    three dates or no variation in d.
    """

    joined = pd.concat([total_return_returns, dividend_component], axis=1, join="inner").dropna()
    if len(joined) < 3:
        return None
    y = joined.iloc[:, 0].to_numpy(dtype=float)
    x = joined.iloc[:, 1].to_numpy(dtype=float)
    if float(x.max() - x.min()) == 0:
        return None
    x_centered = x - x.mean()
    sxx = float((x_centered**2).sum())
    slope = float((x_centered * (y - y.mean())).sum()) / sxx
    residual = y - y.mean() - slope * x_centered
    standard_error = math.sqrt(float((residual**2).sum()) / (len(y) - 2) / sxx)
    return slope, standard_error


def _noise_share_fields(
    total_return_returns: pd.Series, dividend_component: pd.Series
) -> dict[str, float | None]:
    share = total_return_noise_share(total_return_returns, dividend_component)
    return {
        "total_return_noise_share": None if share is None else _round(share[0]),
        "total_return_noise_share_se": None if share is None else _round(share[1]),
    }


def _check_overlap(
    daily: pd.DataFrame,
    prices_path: Path,
    checks: CheckSpec,
    issues: _Issues,
) -> dict[str, Any]:
    if not prices_path.exists():
        issues.add("WARN", "overlap_unavailable", "prices", f"{prices_path} not found")
        return {}
    prices = pd.read_csv(prices_path, usecols=["date", "ticker", "adj_close"])
    prices["date"] = pd.to_datetime(prices["date"])
    result: dict[str, Any] = {
        "prices_path": str(prices_path),
        "prices_sha256": _sha256_file(prices_path),
    }
    for ticker, column in checks.overlap_tickers.items():
        actual = (
            prices[prices["ticker"] == ticker]
            .drop_duplicates("date", keep="last")
            .set_index("date")["adj_close"]
            .sort_index()
        )
        stats = tracking_stats(actual, daily[f"{column}_return"])
        if stats is None:
            issues.add("WARN", "overlap_unavailable", ticker, "fewer than 2 overlapping dates")
            continue
        tolerance = checks.max_abs_annual_tracking_difference.get(column)
        stats["synthetic_column"] = column
        stats["tolerance_abs_annual_tracking_difference"] = tolerance
        if tolerance is not None and abs(stats["annual_tracking_difference"]) > tolerance:
            issues.add(
                "WARN",
                "tracking_outside_tolerance",
                ticker,
                f"annual tracking difference {stats['annual_tracking_difference']:.4f} vs "
                f"tolerance {tolerance}",
            )
        result[ticker] = stats
    return result


def tracking_stats(actual_levels: pd.Series, synthetic_returns: pd.Series) -> dict[str, Any] | None:
    """Actual minus synthetic daily returns on the dates both series have.

    The synthetic return series is turned into a level series first so both sides measure the same
    span between consecutive common dates, even where one calendar has a gap.
    """

    synthetic = synthetic_returns.dropna()
    if synthetic.empty:
        return None
    synthetic_levels = (1 + synthetic).cumprod()
    joined = pd.concat(
        [actual_levels.rename("actual"), synthetic_levels.rename("synthetic")], axis=1, join="inner"
    ).dropna()
    if len(joined) < 2:
        return None
    returns = joined / joined.shift(1) - 1
    returns = returns.dropna()
    diff = returns["actual"] - returns["synthetic"]
    return {
        "overlap_days": int(len(diff)),
        "first_date": _iso(joined.index[0]),
        "last_date": _iso(joined.index[-1]),
        "annual_tracking_difference": _round(diff.mean() * TRADING_DAYS_PER_YEAR),
        "annual_tracking_error": _round(diff.std(ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR)),
        "max_abs_daily_difference": _round(diff.abs().max()),
        "correlation": _round(returns["actual"].corr(returns["synthetic"])),
    }


def _check_report(
    loaded: LoadedSpec,
    manifest: Mapping[str, Any],
    series_rows: Mapping[str, Any],
    issues: _Issues,
    calendar_summary: Mapping[str, Any],
    overlap: Mapping[str, Any],
    checked_at: datetime | None,
) -> dict[str, Any]:
    severities = {issue["severity"] for issue in issues.items}
    status = "FAIL" if "FAIL" in severities else "WARN" if "WARN" in severities else "PASS"
    return {
        "schema_version": CHECK_SCHEMA_VERSION,
        "status": status,
        "snapshot_id": manifest["snapshot_id"],
        "series_csv_sha256": manifest["series_csv_sha256"],
        "spec_id": loaded.spec.spec_id,
        "spec_sha256": loaded.sha256,
        "checked_at": (checked_at or datetime.now(tz=UTC)).isoformat(),
        "request_parameters": manifest["request_parameters"],
        "series": dict(series_rows),
        "calendar": dict(calendar_summary),
        "overlap_tracking": dict(overlap),
        "issues": issues.items,
        "production_effect": "none",
    }


def _write_check(snapshot_dir: Path, report: Mapping[str, Any]) -> None:
    _write_text(snapshot_dir / CHECK_JSON_FILE, json.dumps(report, indent=2, sort_keys=True) + "\n")
    _write_text(snapshot_dir / CHECK_MD_FILE, render_check_markdown(report))


def render_check_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# QQQ 长历史研究快照检查：{report['snapshot_id']}",
        "",
        f"- 状态：**{report['status']}**",
        f"- 检查时间：{report['checked_at']}",
        f"- 规格：`{report['spec_id']}`（sha256 `{report['spec_sha256'][:12]}`）",
        f"- 序列文件 sha256：`{report['series_csv_sha256'][:12]}`",
        "- 数据只在本机，不入库、不再分发、不发给外部 LLM；本报告只含汇总。",
        "",
        "## 序列",
        "",
        "| 序列 | 角色 | 行数 | 首日 | 应有首日 | 末日 |",
        "|---|---|---:|---|---|---|",
    ]
    for series_id, row in report["series"].items():
        lines.append(
            f"| {series_id} | {row['role']} | {row['row_count']} | {row.get('first_date', '-')} | "
            f"{row['expected_first_date']} | {row.get('last_date', '-')} |"
        )
    calendar = report.get("calendar") or {}
    if calendar:
        lines += [
            "",
            "## 日历和派生",
            "",
            f"- 交易日：{calendar['calendar_dates']}"
            f"（{calendar['first_date']} 到 {calendar['last_date']}）",
            f"- 总收益指数首日：{calendar['total_return_first_date']}",
            f"- 从日历去掉的价格指数单独日期：{calendar['price_only_dates_dropped']} 个，"
            f"其中价格未变 {calendar['price_only_dates_unchanged']} 个"
            f"（{', '.join(calendar['price_only_dates']) or '无'}）",
            "- 只有总收益、价格指数没有的日期（不在日历里）："
            f"{calendar['total_return_only_dates_ignored']} 个"
            f"（{', '.join(calendar['total_return_only_dates']) or '无'}）",
            "- 沿用前值、按另一指数修补的水平："
            f"总收益 {calendar['total_return_repaired_days']} 天，"
            f"价格 {calendar['price_repaired_days']} 天",
            f"- 隐含股息率（总收益减价格收益，年化）：{calendar['implied_dividend_yield_annual']}",
            f"- 总收益落后价格收益超过阈值的天数：{calendar['negative_component_days']}；"
            f"日股息分量的一阶自相关：{calendar['dividend_component_lag1_autocorrelation']}",
            f"- 超长间隔：{', '.join(calendar['long_gaps']) or '无'}",
        ]
        windows = calendar.get("dividend_windows") or {}
        if windows:
            lines.append(
                f"- 单日股息分量最低：{calendar['min_daily_dividend_component']}；"
                f"{windows['trading_days']} 个交易日滚动窗口里总收益相对价格收益最差 "
                f"{windows['min_change']}（截至 {windows['min_change_end_date']}），"
                f"低于下限的窗口 {windows['windows_below_limit']} 个"
            )
        attribution = calendar.get("noise_attribution") or {}
        if attribution:
            lines.append(
                f"- 低于下限的窗口里的 {attribution['trading_days']} 个交易日：日股息分量标准差 "
                f"{attribution['dividend_component_daily_std']}，噪声在总收益指数的比例 "
                f"{_share_text(attribution)}（约 1 在总收益指数，约 0 在价格指数，"
                "约 0.5 是两个都有或收盘时间不一致）"
            )
        by_year = calendar.get("dividend_by_year") or []
        if by_year:
            lines += [
                "",
                "### 逐年股息分量",
                "",
                "| 年 | 交易日 | 落后天数 | 隐含股息率 | 总收益修补 | 价格修补 | 日分量标准差 | "
                "噪声在总收益的比例 |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
            lines += [
                f"| {row['year']} | {row['trading_days']} | {row['negative_component_days']} | "
                f"{row['implied_dividend_yield_annual']} | {row['total_return_repaired_days']} | "
                f"{row['price_repaired_days']} | {row['dividend_component_daily_std']} | "
                f"{_share_text(row)} |"
                for row in by_year
            ]
    tracking = report.get("overlap_tracking") or {}
    overlap = {key: value for key, value in tracking.items() if isinstance(value, dict)}
    if overlap:
        lines += [
            "",
            "## 与本机 ETF 复权价的重叠段跟踪（实际减合成）",
            "",
            "| ETF | 合成列 | 天数 | 区间 | 年化跟踪差 | 年化跟踪误差 | 最大单日差 | 相关 |",
            "|---|---|---:|---|---:|---:|---:|---:|",
        ]
        for ticker, stats in overlap.items():
            lines.append(
                f"| {ticker} | {stats['synthetic_column']} | {stats['overlap_days']} | "
                f"{stats['first_date']}..{stats['last_date']} | "
                f"{stats['annual_tracking_difference']} | "
                f"{stats['annual_tracking_error']} | {stats['max_abs_daily_difference']} | "
                f"{stats['correlation']} |"
            )
    lines += ["", "## 问题", ""]
    if not report["issues"]:
        lines.append("无。")
    for issue in report["issues"]:
        dates = f"（{', '.join(issue['dates'])}）" if issue.get("dates") else ""
        lines.append(
            f"- {issue['severity']} `{issue['code']}` {issue['subject']}：{issue['message']}{dates}"
        )
    return "\n".join(lines) + "\n"


def _share_text(row: Mapping[str, Any]) -> str:
    share = row.get("total_return_noise_share")
    if share is None:
        return "-"
    return f"{share:.2f} ± {row['total_return_noise_share_se']:.2f}"


# --- gate for research code -------------------------------------------------------------------


def load_daily(
    snapshot_dir: Path,
    loaded: LoadedSpec | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Return the derived daily frame and the check report, refusing unchecked snapshots."""

    loaded = loaded or load_spec()
    manifest, frame = read_snapshot(snapshot_dir)
    check_path = snapshot_dir / CHECK_JSON_FILE
    if not check_path.exists():
        raise SnapshotNotCheckedError(f"{snapshot_dir.name}: no check report; run check first")
    report = json.loads(check_path.read_text(encoding="utf-8"))
    if report.get("series_csv_sha256") != manifest["series_csv_sha256"]:
        raise SnapshotNotCheckedError(f"{snapshot_dir.name}: check report is for other data")
    if report.get("spec_sha256") != loaded.sha256:
        raise SnapshotNotCheckedError(
            f"{snapshot_dir.name}: spec changed since the last check; run check again"
        )
    if report.get("status") not in {"PASS", "WARN"}:
        raise SnapshotNotCheckedError(
            f"{snapshot_dir.name}: check status is {report.get('status')}"
        )
    return derive_daily(frame, loaded.spec), report


# --- helpers ----------------------------------------------------------------------------------


def _normalize_series_frame(raw: pd.DataFrame) -> pd.DataFrame:
    frame = raw.loc[:, ["date", "series", "value"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["series"] = frame["series"].astype(str)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna(subset=["value"])
    frame = frame.drop_duplicates(["series", "date"], keep="last")
    return frame.sort_values(["series", "date"]).reset_index(drop=True)


def _series_csv_bytes(frame: pd.DataFrame) -> bytes:
    # Fixed line endings and float formatting keep the checksum identical across machines.
    text = frame.to_csv(index=False, lineterminator="\n", float_format="%.10g")
    return text.encode("utf-8")


def _series_summary(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for series_id, rows in frame.groupby("series", sort=True):
        summary[str(series_id)] = {
            "row_count": int(len(rows)),
            "first_date": str(rows["date"].min()),
            "last_date": str(rows["date"].max()),
        }
    return summary


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))


def _iso(value: Any) -> str:
    return str(pd.Timestamp(value).date().isoformat())


def _round(value: float | None) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    return round(float(value), 6)


# --- command line -----------------------------------------------------------------------------

ProviderFactory = Callable[[], RateDataProvider]
provider_factory: ProviderFactory = FredRateProvider

app = typer.Typer(
    help="QQQ 长历史研究数据（TRADING-2567 S1）：只在本机，输出只有汇总。", no_args_is_help=True
)
RootOption = Annotated[Path, typer.Option(help="快照根目录。")]
SpecOption = Annotated[Path, typer.Option(help="规格文件。")]
PricesOption = Annotated[Path, typer.Option(help="本机 ETF 价格缓存，用于重叠段跟踪。")]


@app.command("fetch")
def fetch_command(
    end: Annotated[str, typer.Option(help="请求截止日 YYYY-MM-DD，默认今天（UTC）。")] = "",
    root: RootOption = DEFAULT_SNAPSHOT_ROOT,
    spec_path: SpecOption = DEFAULT_SPEC_PATH,
    prices_path: PricesOption = DEFAULT_PRICES_PATH,
) -> None:
    """下载 FRED 序列成快照，然后检查。"""

    loaded = load_spec(spec_path)
    end_date = date.fromisoformat(end) if end else datetime.now(tz=UTC).date()
    snapshot_dir = fetch_snapshot(loaded, end=end_date, provider=provider_factory(), root=root)
    typer.echo(f"snapshot: {snapshot_dir.name}")
    _finish_check(snapshot_dir, loaded, prices_path)


@app.command("check")
def check_command(
    snapshot: Annotated[str, typer.Option(help="快照 ID，默认 LATEST。")] = "",
    root: RootOption = DEFAULT_SNAPSHOT_ROOT,
    spec_path: SpecOption = DEFAULT_SPEC_PATH,
    prices_path: PricesOption = DEFAULT_PRICES_PATH,
) -> None:
    """检查已有快照，写 check.json 和 check.md。"""

    loaded = load_spec(spec_path)
    snapshot_dir = resolve_snapshot_dir(root, snapshot or None)
    _finish_check(snapshot_dir, loaded, prices_path)


def _finish_check(snapshot_dir: Path, loaded: LoadedSpec, prices_path: Path) -> None:
    report = check_snapshot(snapshot_dir, loaded, prices_path=prices_path)
    for series_id, row in report["series"].items():
        typer.echo(
            f"  {series_id}: {row['row_count']} rows, "
            f"{row.get('first_date', '-')}..{row.get('last_date', '-')}"
        )
    for issue in report["issues"]:
        typer.echo(f"  {issue['severity']} {issue['code']} {issue['subject']}: {issue['message']}")
    typer.echo(f"check: {report['status']} ({snapshot_dir / CHECK_MD_FILE})")
    if report["status"] == "FAIL":
        raise typer.Exit(code=1)


__all__: Sequence[str] = (
    "DEFAULT_SNAPSHOT_ROOT",
    "DEFAULT_SPEC_PATH",
    "LoadedSpec",
    "LongHistorySpec",
    "SnapshotIntegrityError",
    "SnapshotNotCheckedError",
    "check_snapshot",
    "derive_daily",
    "fetch_snapshot",
    "load_daily",
    "load_spec",
    "read_snapshot",
    "resolve_snapshot_dir",
    "tracking_stats",
)


if __name__ == "__main__":
    app()
