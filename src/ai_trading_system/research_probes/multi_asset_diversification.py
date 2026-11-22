"""QQQ multi-asset low-correlation diversification development probe (TRADING-2575).

Path 2 research diagnostic: test whether combining QQQ with low-correlation /
inflation-resilient assets (Gold, 10Y Treasury, trend-filtered shields, and a
simple 4-asset cross-asset trend sleeve) raises 1x excess Sharpe enough to beat
100% buy-and-hold QQQ after matching drawdown via Scheme A margin financing
(including a 5% fixed high-rate stress test).

Data architecture (owner instruction 2026-11-23):
- Primary (2-B): real FMP dividend-adjusted ETFs (GLD from 2004-11-18, IEF/TLT
  from 2002-07-30), plus QQQ/DTB3 from checked qlh snapshot.
- Backfill (2-A): pre-ETF history back to 1998-01-01 from FMP GCUSD (gold spot)
  and official FRED series (DGS10/DGS20 par-bond synthesis, DCOILWTICO, DTWEXM).
- Mandatory fail-closed overlap consistency gate on GLD vs GCUSD, IEF vs DGS10,
  and TLT vs DGS20 before splicing or strategy evaluation.
- Hard cutoff at 2010-12-31 in fetch, check, and run (zero 2011+ data requested
  or read). Print-only development diagnostic: production_effect=none;
  broker_action=none.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any, Literal, NamedTuple

import numpy as np
import pandas as pd
import typer
from dotenv import load_dotenv

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    FredRateProvider,
    PriceDataProvider,
    RateDataProvider,
    RateRequest,
)
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_snapshot
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_bench.simulate import schedule_mask
from ai_trading_system.research_bench.sources import DEFAULT_MAX_STALE_DAYS, known_at
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import asym_vxn_leverage as avl
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt

TASK_ID = "TRADING-2575_QQQ_MULTI_ASSET_DIVERSIFICATION"
REQUIREMENT = "docs/requirements/TRADING-2575_QQQ_Multi_Asset_Diversification_Dev_Probe.md"
DATA_TRIAL = lt.DATA_TRIAL
TRADING_DAYS = 252

# Hard development cutoff: fetch, check, and run never request or read past 2010-12-31.
FETCH_START = date(1998, 1, 1)
DEV_CUTOFF = gp.CUTOFF  # 2010-12-31
FULL_DEV_START = date(1999, 3, 31)
PURE_2B_START = date(2004, 11, 30)

# Temporary development-diagnostic baselines documented in REQUIREMENT; exit upon owner review.
TREND_LOOKBACK = 252
OVERLAP_MIN_DAYS = 1000
OVERLAP_MIN_CORR = 0.85
OVERLAP_MAX_ANNUAL_DIFF = 0.03
LEVERAGE_GRID: tuple[float, ...] = tuple(round(1.0 + 0.05 * i, 2) for i in range(41))
KILL_MIN_EXCESS_OVER_HOLD = 0.010
KILL_MAX_GAP_VS_F0001 = 0.005

FMP_ETFS: tuple[tuple[str, date], ...] = (
    ("GLD", date(2004, 11, 18)),
    ("IEF", date(2002, 7, 30)),
    ("TLT", date(2002, 7, 30)),
)
FMP_PROXIES: tuple[tuple[str, date], ...] = (("GCUSD", date(1998, 1, 2)),)
FRED_SERIES: tuple[str, ...] = ("DGS10", "DGS20", "DCOILWTICO", "DTWEXM")

SERIES_CSV = "series.csv"
MANIFEST_JSON = "manifest.json"
CHECK_JSON = "multi_asset_check.json"
RESULT_JSON = "multi_asset_diversification.json"
RESULT_MD = "multi_asset_diversification.md"

StructureMode = Literal[
    "hold",
    "f0001",
    "qqq70_gold30",
    "qqq60_ief20_gold20",
    "qqq60_trend_shield40",
    "qqq70_cta_lo30",
    "qqq70_cta_ls30",
]

RISKY_ASSETS: tuple[str, ...] = ("qqq", "gold", "treasury_10y", "oil", "usd")


@dataclass(frozen=True)
class PortfolioSpec:
    strategy_id: str
    label: str
    mode: StructureMode
    is_candidate: bool = True
    requires_2a_extras: bool = False


def default_specs() -> tuple[PortfolioSpec, ...]:
    return (
        PortfolioSpec("hold", "买入持有 QQQ (1.0x 基准)", "hold", is_candidate=False),
        PortfolioSpec("f0001_1.0", "F0001 原版 (1.0x 现货趋势)", "f0001", is_candidate=False),
        PortfolioSpec("qqq70_gold30", "A. 黄金单盾 (70% QQQ + 30% 黄金)", "qqq70_gold30"),
        PortfolioSpec(
            "qqq60_ief20_gold20",
            "B. 股债金三盾 (60% QQQ + 20% 10Y债 + 20% 黄金)",
            "qqq60_ief20_gold20",
        ),
        PortfolioSpec(
            "qqq60_trend_shield40",
            "C. 双盾趋势过滤 (60% QQQ + 20% 黄金/20% 10Y债 252日正趋势)",
            "qqq60_trend_shield40",
        ),
        PortfolioSpec(
            "qqq70_cta_lo30",
            "D1. 跨资产多头趋势 (70% QQQ + 30% 债/金/油/汇 252日做多或现金)",
            "qqq70_cta_lo30",
            requires_2a_extras=True,
        ),
        PortfolioSpec(
            "qqq70_cta_ls30",
            "D2. 跨资产多空趋势对照 (70% QQQ + 30% 债/金/油/汇 252日多空)",
            "qqq70_cta_ls30",
            requires_2a_extras=True,
        ),
    )


class SimResult(NamedTuple):
    returns: pd.Series
    gross_exposure: pd.Series
    net_borrowed: pd.Series
    turnover: pd.Series
    trade_cost: pd.Series
    financing_cost: pd.Series
    start: pd.Timestamp
    bust_date: str | None


app = typer.Typer(help="TRADING-2575：QQQ 多资产低相关分散提夏普开发窗口诊断。")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def par_bond_returns(
    yields_percent: pd.Series, calendar: pd.DatetimeIndex, *, years_maturity: int
) -> pd.Series:
    """Realized constant-maturity par-bond daily returns from percent yields."""

    if years_maturity < 1:
        raise ValueError("years_maturity must be >= 1")
    obs = yields_percent.dropna().astype(float)
    if (obs < 0.1).any() or (obs > 25.0).any():
        raise ValueError("bond yields must be percent in [0.1, 25.0]")
    periods = 2 * years_maturity
    y = known_at(obs / 100.0, calendar, 0, DEFAULT_MAX_STALE_DAYS)
    coupon = y.shift(1).to_numpy(dtype=float)
    rate = y.to_numpy(dtype=float)
    days = np.full(len(calendar), np.nan)
    days[1:] = (calendar[1:] - calendar[:-1]).days
    discount = (1.0 + rate / 2.0) ** (-periods)
    price = (coupon / rate) * (1.0 - discount) + discount
    ret = price - 1.0 + coupon * days / 365.0
    return pd.Series(ret, index=calendar, name=f"treasury_{years_maturity}y")


def level_to_returns(
    levels: pd.Series,
    calendar: pd.DatetimeIndex,
    *,
    max_stale_days: int = DEFAULT_MAX_STALE_DAYS,
) -> pd.Series:
    """Align price/index level onto calendar (carrying holidays <= max_stale_days) -> returns."""

    obs = levels.dropna().astype(float)
    if (obs <= 0.0).any():
        raise ValueError(f"series {levels.name} contains nonpositive levels")
    aligned = known_at(obs, calendar, 0, max_stale_days)
    return aligned.pct_change(fill_method=None)


def fetch_multi_asset_snapshot(
    snapshot_dir: Path,
    *,
    rate_provider: RateDataProvider,
    price_provider: PriceDataProvider,
    end: date = DEV_CUTOFF,
    downloaded_at: datetime | None = None,
) -> Path:
    """Download 2-B ETFs and 2-A proxies strictly capped at 2010-12-31 with SHA-256 manifest."""

    if end > DEV_CUTOFF:
        raise ValueError(f"refuses to fetch multi-asset data after {DEV_CUTOFF}: {end}")

    frames: list[pd.DataFrame] = []
    etf_meta: dict[str, Any] = {}
    for ticker, first_expected in (*FMP_ETFS, *FMP_PROXIES):
        reqs = qlh.etf_reference_requests(ticker, first_expected, end)
        rows = qlh._download_reference(price_provider, reqs)
        rows = rows.loc[pd.to_datetime(rows["date"]).dt.date <= end].copy()
        if rows.empty:
            raise ValueError(f"FMP returned no rows for {ticker} up to {end}")
        part = rows.rename(columns={"adj_close": "value"}).assign(series=ticker)
        frames.append(part[["date", "series", "value"]])
        etf_meta[ticker] = {
            "provider": "Financial Modeling Prep",
            "field": "adj_close",
            "first_date": str(pd.to_datetime(part["date"]).min().date()),
            "last_date": str(pd.to_datetime(part["date"]).max().date()),
            "rows": int(len(part)),
        }

    fred_raw = rate_provider.download_rates(
        RateRequest(series_ids=list(FRED_SERIES), start=FETCH_START, end=end)
    )
    fred_raw = fred_raw.dropna(subset=["value"]).copy()
    fred_raw = fred_raw.loc[pd.to_datetime(fred_raw["date"]).dt.date <= end].copy()
    missing_fred = sorted(set(FRED_SERIES) - set(fred_raw["series"]))
    if missing_fred:
        raise ValueError(f"FRED returned no rows for: {', '.join(missing_fred)}")
    frames.append(fred_raw[["date", "series", "value"]])

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined["value"] = combined["value"].astype(float)
    combined = (
        combined.drop_duplicates(subset=["date", "series"], keep="last")
        .sort_values(["series", "date"])
        .reset_index(drop=True)
    )
    payload = combined.to_csv(index=False, lineterminator="\n").encode("utf-8")
    digest = _sha256_bytes(payload)

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir.joinpath(SERIES_CSV).write_bytes(payload)

    series_summary: dict[str, Any] = {}
    for sid, grp in combined.groupby("series"):
        series_summary[str(sid)] = {
            "rows": int(len(grp)),
            "first_date": str(grp["date"].min()),
            "last_date": str(grp["date"].max()),
        }
    manifest = {
        "schema_version": "path2_multi_asset_snapshot.v1",
        "task_id": TASK_ID,
        "fetch_start": FETCH_START.isoformat(),
        "fetch_end": end.isoformat(),
        "cutoff_enforced": DEV_CUTOFF.isoformat(),
        "downloaded_at": (downloaded_at or datetime.now(tz=UTC)).isoformat(),
        "series_csv_sha256": digest,
        "row_count": int(len(combined)),
        "providers": {
            "fmp": {
                "tickers": [t for t, _ in (*FMP_ETFS, *FMP_PROXIES)],
                "endpoint": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted",
            },
            "fred": {
                "series_ids": list(FRED_SERIES),
                "endpoint": "https://fred.stlouisfed.org/graph/fredgraph.csv",
            },
        },
        "series": series_summary,
        "data_policy": {"local_only": True, "redistribution_allowed": False},
    }
    snapshot_dir.joinpath(MANIFEST_JSON).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return snapshot_dir


def load_multi_asset_snapshot(snapshot_dir: Path) -> tuple[dict[str, pd.Series], dict[str, Any]]:
    """Load and checksum-verify the local multi-asset snapshot; fail closed on any tampering."""

    manifest_path = snapshot_dir / MANIFEST_JSON
    series_path = snapshot_dir / SERIES_CSV
    if not manifest_path.exists() or not series_path.exists():
        raise FileNotFoundError(f"multi-asset snapshot incomplete under {snapshot_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_bytes = series_path.read_bytes()
    actual_sha = _sha256_bytes(raw_bytes)
    if actual_sha != manifest.get("series_csv_sha256"):
        raise ValueError("multi-asset snapshot SHA-256 mismatch; refusing to read")

    frame = pd.read_csv(series_path)
    dates = pd.to_datetime(frame["date"])
    if dates.max().date() > DEV_CUTOFF:
        raise ValueError(f"multi-asset snapshot exceeds cutoff {DEV_CUTOFF}")

    out: dict[str, pd.Series] = {}
    for sid, grp in frame.groupby("series"):
        s = pd.Series(
            grp["value"].to_numpy(dtype=float),
            index=pd.DatetimeIndex(pd.to_datetime(grp["date"])),
            name=str(sid),
        ).sort_index()
        if s.index.has_duplicates:
            raise ValueError(f"duplicate dates in snapshot series {sid}")
        out[str(sid)] = s
    return out, manifest


def compare_overlap_pair(
    name: str,
    etf_ret: pd.Series,
    proxy_ret: pd.Series,
    *,
    required: bool,
) -> dict[str, Any]:
    """Audit consistency between a 2-B real ETF and its 2-A backfill proxy over overlap dates."""

    pair = pd.concat({"etf": etf_ret, "proxy": proxy_ret}, axis=1).dropna()
    if len(pair) < 20:
        return {
            "name": name,
            "required": required,
            "ok": not required,
            "overlap_days": int(len(pair)),
            "reason": "insufficient_overlap_days",
        }

    e = pair["etf"].to_numpy(dtype=float)
    p = pair["proxy"].to_numpy(dtype=float)
    diff = e - p
    years = (pair.index[-1] - pair.index[0]).days / DAYS_PER_YEAR
    etf_cum = float(np.prod(1.0 + e))
    proxy_cum = float(np.prod(1.0 + p))
    etf_cagr = etf_cum ** (1.0 / years) - 1.0
    proxy_cagr = proxy_cum ** (1.0 / years) - 1.0
    ann_diff = etf_cagr - proxy_cagr
    daily_corr = float(np.corrcoef(e, p)[0, 1])
    tracking_err = float(np.std(diff, ddof=1) * math.sqrt(TRADING_DAYS))

    # Weekly (5-day) compounded return correlation isolates intraday close-time jitter.
    w_etf = (1.0 + pair["etf"]).resample("W-FRI").prod() - 1.0
    w_proxy = (1.0 + pair["proxy"]).resample("W-FRI").prod() - 1.0
    w_pair = pd.concat({"e": w_etf, "p": w_proxy}, axis=1).dropna()
    weekly_corr = float(w_pair["e"].corr(w_pair["p"])) if len(w_pair) >= 10 else float("nan")

    worst_idx = int(np.argmax(np.abs(diff)))
    max_abs_daily = float(np.abs(diff[worst_idx]))
    max_diff_date = pair.index[worst_idx].date().isoformat()

    checks = {
        "days_ok": len(pair) >= OVERLAP_MIN_DAYS,
        "corr_ok": daily_corr >= OVERLAP_MIN_CORR,
        "annual_diff_ok": abs(ann_diff) <= OVERLAP_MAX_ANNUAL_DIFF,
    }
    ok = all(checks.values()) if required else True
    return {
        "name": name,
        "required": required,
        "ok": ok,
        "checks": checks,
        "start": pair.index[0].date().isoformat(),
        "end": pair.index[-1].date().isoformat(),
        "overlap_days": int(len(pair)),
        "daily_corr": daily_corr,
        "weekly_corr": weekly_corr,
        "etf_cagr": etf_cagr,
        "proxy_cagr": proxy_cagr,
        "annual_return_diff": ann_diff,
        "annual_tracking_error": tracking_err,
        "cumulative_ratio": etf_cum / proxy_cum if proxy_cum > 0 else float("nan"),
        "max_abs_daily_diff": max_abs_daily,
        "max_abs_daily_diff_date": max_diff_date,
    }


def build_spliced_returns(
    qqq_data: BenchData,
    raw_series: Mapping[str, pd.Series],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build 2-B primary + 2-A backfilled daily return matrix and overlap audit report."""

    cut = gp.cut(qqq_data)
    calendar = pd.DatetimeIndex(cut.returns.index)
    if calendar.max().date() > DEV_CUTOFF:
        raise ValueError("qqq_data exceeds development cutoff")

    gld_ret = level_to_returns(raw_series["GLD"], calendar)
    gcusd_ret = level_to_returns(raw_series["GCUSD"], calendar)
    ief_ret = level_to_returns(raw_series["IEF"], calendar)
    tlt_ret = level_to_returns(raw_series["TLT"], calendar)
    dgs10_ret = par_bond_returns(raw_series["DGS10"], calendar, years_maturity=10)
    dgs20_ret = par_bond_returns(raw_series["DGS20"], calendar, years_maturity=20)
    oil_strict = level_to_returns(raw_series["DCOILWTICO"], calendar)
    oil_ret = level_to_returns(raw_series["DCOILWTICO"], calendar, max_stale_days=10)
    usd_ret = level_to_returns(raw_series["DTWEXM"], calendar)
    oil_gap_mask = (calendar > pd.Timestamp(FULL_DEV_START)) & (
        calendar <= pd.Timestamp(DEV_CUTOFF)
    )
    oil_carried_beyond_5d = int(oil_strict.loc[oil_gap_mask].isna().sum())

    overlap_pairs = [
        compare_overlap_pair("gold_gld_vs_gcusd", gld_ret, gcusd_ret, required=True),
        compare_overlap_pair("treasury10y_ief_vs_dgs10", ief_ret, dgs10_ret, required=True),
        compare_overlap_pair("treasury20y_tlt_vs_dgs20", tlt_ret, dgs20_ret, required=False),
    ]

    # 2-B primary, 2-A backfill on earlier dates strictly before ETF returns start.
    gold_spliced = gld_ret.combine_first(gcusd_ret)
    t10_spliced = ief_ret.combine_first(dgs10_ret)

    gld_first = gld_ret.dropna().index[0].date().isoformat()
    ief_first = ief_ret.dropna().index[0].date().isoformat()

    frame = pd.DataFrame(
        {
            "qqq": cut.returns["qqq"].astype(float),
            "cash": cut.returns["cash"].astype(float),
            "gold": gold_spliced.astype(float),
            "treasury_10y": t10_spliced.astype(float),
            "oil": oil_ret.astype(float),
            "usd": usd_ret.astype(float),
            "gld_only": gld_ret.astype(float),
            "ief_only": ief_ret.astype(float),
        },
        index=calendar,
    )

    in_full = frame.loc[
        (calendar > pd.Timestamp(FULL_DEV_START)) & (calendar <= pd.Timestamp(DEV_CUTOFF)),
        ["qqq", "cash", "gold", "treasury_10y", "oil", "usd"],
    ]
    in_2b = frame.loc[
        (calendar > pd.Timestamp(PURE_2B_START)) & (calendar <= pd.Timestamp(DEV_CUTOFF)),
        ["qqq", "cash", "gld_only", "ief_only"],
    ]
    missing_full = int((~np.isfinite(in_full.to_numpy(dtype=float))).sum())
    missing_2b = int((~np.isfinite(in_2b.to_numpy(dtype=float))).sum())
    overlap_ok = all(item["ok"] for item in overlap_pairs)
    ok = overlap_ok and missing_full == 0 and missing_2b == 0 and len(in_full) >= 2500

    audit = {
        "ok": ok,
        "cutoff_date": DEV_CUTOFF.isoformat(),
        "max_loaded_date": calendar.max().date().isoformat(),
        "full_dev_days": int(len(in_full)),
        "pure_2b_days": int(len(in_2b)),
        "missing_full_cells": missing_full,
        "missing_2b_cells": missing_2b,
        "oil_carried_beyond_5d_days": oil_carried_beyond_5d,
        "splice_dates": {
            "gold_2b_start": gld_first,
            "treasury_10y_2b_start": ief_first,
        },
        "overlap_ok": overlap_ok,
        "overlap_pairs": overlap_pairs,
    }
    return frame, audit


def _trailing_return(series: pd.Series, lookback: int) -> pd.Series:
    vals = series.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan)
    # Compound trailing `lookback` daily returns ending at day t (inclusive).
    log_r = np.where(np.isfinite(vals), np.log1p(np.maximum(vals, -0.999999)), np.nan)
    for i in range(lookback - 1, len(vals)):
        win = log_r[i - lookback + 1 : i + 1]
        if np.isfinite(win).all():
            out[i] = math.expm1(float(np.sum(win)))
    return pd.Series(out, index=series.index)


def compute_target_weights(
    frame: pd.DataFrame,
    f0001_base: pd.Series,
    spec: PortfolioSpec,
) -> pd.DataFrame:
    """Target weights at close t (zero look-ahead; executed at close t+1 on month-ends)."""

    idx = frame.index
    zeros = pd.DataFrame(0.0, index=idx, columns=list(RISKY_ASSETS))

    if spec.mode == "hold":
        zeros["qqq"] = 1.0
        return zeros
    if spec.mode == "f0001":
        zeros["qqq"] = f0001_base.reindex(idx).astype(float)
        return zeros
    if spec.mode == "qqq70_gold30":
        zeros["qqq"] = 0.70
        zeros["gold"] = 0.30
        return zeros
    if spec.mode == "qqq60_ief20_gold20":
        zeros["qqq"] = 0.60
        zeros["treasury_10y"] = 0.20
        zeros["gold"] = 0.20
        return zeros
    if spec.mode == "qqq60_trend_shield40":
        zeros["qqq"] = 0.60
        g_tr = _trailing_return(frame["gold"], TREND_LOOKBACK)
        t_tr = _trailing_return(frame["treasury_10y"], TREND_LOOKBACK)
        zeros["gold"] = np.where(g_tr > 0.0, 0.20, 0.0)
        zeros["treasury_10y"] = np.where(t_tr > 0.0, 0.20, 0.0)
        return zeros
    if spec.mode in ("qqq70_cta_lo30", "qqq70_cta_ls30"):
        zeros["qqq"] = 0.70
        leg = 0.30 / 4.0
        allow_short = spec.mode == "qqq70_cta_ls30"
        for col in ("treasury_10y", "gold", "oil", "usd"):
            tr_ret = _trailing_return(frame[col], TREND_LOOKBACK)
            if allow_short:
                zeros[col] = np.where(tr_ret > 0.0, leg, np.where(tr_ret < 0.0, -leg, 0.0))
            else:
                zeros[col] = np.where(tr_ret > 0.0, leg, 0.0)
        return zeros
    raise ValueError(f"unknown mode: {spec.mode}")


def simulate_portfolio(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    cost: avl.CostModel,
    *,
    start: date,
    end: date = DEV_CUTOFF,
    leverage: float = 1.0,
) -> SimResult:
    """Monthly-rebalanced multi-asset simulation with drift and Scheme A margin financing."""

    if end > DEV_CUTOFF:
        raise ValueError(f"refuses end date after {DEV_CUTOFF}: {end}")
    index = pd.DatetimeIndex(frame.index)
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")

    risky_mat = frame[list(RISKY_ASSETS)].to_numpy(dtype=float)
    cash_vec = frame["cash"].to_numpy(dtype=float)
    tgt_mat = targets[list(RISKY_ASSETS)].to_numpy(dtype=float) * leverage
    month_end = schedule_mask(index, "month_end")

    opening = next((p for p in range(first - 1, -1, -1) if month_end[p]), None)
    if opening is None:
        raise ValueError("no month-end before start; extend lookback")

    weights = tgt_mat[opening].copy()
    if not np.isfinite(weights).all():
        raise ValueError("nonfinite opening weights")

    rets: list[float] = []
    gross_exps: list[float] = []
    borrows: list[float] = []
    turns: list[float] = []
    tcosts: list[float] = []
    fcosts: list[float] = []
    bust_date: str | None = None

    for q in range(first + 1, last + 1):
        if bust_date is not None:
            rets.append(0.0)
            gross_exps.append(0.0)
            borrows.append(0.0)
            turns.append(0.0)
            tcosts.append(0.0)
            fcosts.append(0.0)
            continue

        r_assets = risky_mat[q]
        r_cash = float(cash_vec[q])
        # Cash balance = 1 - sum(weights). Short positions (w < 0) credit proceeds into cash
        # (earning r_cash on collateral); net long > 1 borrows (sum(weights) - 1).
        sum_w = float(np.sum(weights))
        borrowed = max(sum_w - 1.0, 0.0)
        if borrowed > 0.0:
            if cost.fixed_borrow_rate is not None:
                fin = borrowed * (cost.fixed_borrow_rate / TRADING_DAYS)
            else:
                fin = borrowed * (r_cash + cost.spread / TRADING_DAYS)
            gross = float(np.dot(weights, r_assets)) - fin
        else:
            fin = 0.0
            gross = float(np.dot(weights, r_assets)) + (1.0 - sum_w) * r_cash

        if 1.0 + gross <= 0.0:
            bust_date = index[q].date().isoformat()
            rets.append(-1.0)
            gross_exps.append(float(np.sum(np.abs(weights))))
            borrows.append(borrowed)
            turns.append(0.0)
            tcosts.append(0.0)
            fcosts.append(fin)
            continue

        gross_exp = float(np.sum(np.abs(weights)))
        weights = weights * (1.0 + r_assets) / (1.0 + gross)

        decided = q - 1
        turnover = 0.0
        if month_end[decided]:
            new_w = tgt_mat[decided]
            turnover = float(np.sum(np.abs(new_w - weights)))
            weights = new_w.copy()

        t_cost = turnover * cost.bps / 10_000.0
        net = (1.0 + gross) * (1.0 - t_cost) - 1.0
        rets.append(net)
        gross_exps.append(gross_exp)
        borrows.append(borrowed)
        turns.append(turnover)
        tcosts.append(t_cost)
        fcosts.append(fin)

    dates = index[first + 1 : last + 1]
    return SimResult(
        returns=pd.Series(rets, index=dates),
        gross_exposure=pd.Series(gross_exps, index=dates),
        net_borrowed=pd.Series(borrows, index=dates),
        turnover=pd.Series(turns, index=dates),
        trade_cost=pd.Series(tcosts, index=dates),
        financing_cost=pd.Series(fcosts, index=dates),
        start=index[first],
        bust_date=bust_date,
    )


def summarize_sim(
    sim: SimResult, qqq_returns: pd.Series, cash_returns: pd.Series
) -> dict[str, Any]:
    r = sim.returns.to_numpy(dtype=float)
    q = qqq_returns.reindex(sim.returns.index).to_numpy(dtype=float)
    c = cash_returns.reindex(sim.returns.index).to_numpy(dtype=float)
    years = (sim.returns.index[-1] - sim.start).days / DAYS_PER_YEAR
    eq = np.concatenate([[1.0], np.cumprod(1.0 + r)])
    end_eq = float(eq[-1])
    geo = -1.0 if end_eq <= 0.0 else end_eq ** (1.0 / years) - 1.0
    cash_geo = float(np.prod(1.0 + c)) ** (1.0 / years) - 1.0
    vol = float(np.std(r, ddof=1) * math.sqrt(TRADING_DAYS)) if len(r) > 1 else 0.0
    dd = float(np.min(eq / np.maximum.accumulate(eq) - 1.0))
    excess = r - c
    ex_std = float(np.std(excess, ddof=1)) if len(excess) > 1 else 0.0
    sharpe = float(np.mean(excess) / ex_std * math.sqrt(TRADING_DAYS)) if ex_std > 0.0 else 0.0
    calmar = (geo - cash_geo) / abs(dd) if dd < 0.0 else 0.0
    corr_qqq = float(np.corrcoef(r, q)[0, 1]) if vol > 0.0 else 0.0
    return {
        "years": years,
        "geometric_annual_return": geo,
        "cash_geo_annual": cash_geo,
        "annual_volatility": vol,
        "max_drawdown": dd,
        "excess_sharpe": sharpe,
        "excess_calmar": calmar,
        "corr_with_qqq": corr_qqq,
        "average_gross_exposure": float(sim.gross_exposure.mean()),
        "average_borrowed": float(sim.net_borrowed.mean()),
        "annual_turnover": float(sim.turnover.sum()) / years,
        "financing_drag_annual": float(sim.financing_cost.sum()) / years,
        "turnover_drag_annual": float(sim.trade_cost.sum()) / years,
        "vol_drag_quadratic": 0.5 * vol * vol,
        "bust_date": sim.bust_date,
    }


def match_drawdown_leverage(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    hold_max_dd: float,
    *,
    start: date,
    end: date = DEV_CUTOFF,
    grid: Sequence[float] = LEVERAGE_GRID,
) -> dict[str, Any]:
    """Find largest L on main cost with max_drawdown >= hold_max_dd, then price all 3 costs."""

    chosen_l = grid[0]
    chosen_main = simulate_portfolio(
        frame, targets, avl.MAIN_COST, start=start, end=end, leverage=chosen_l
    )
    chosen_sum = summarize_sim(chosen_main, frame["qqq"], frame["cash"])
    for lev in grid[1:]:
        sim = simulate_portfolio(frame, targets, avl.MAIN_COST, start=start, end=end, leverage=lev)
        if sim.bust_date is not None:
            break
        s = summarize_sim(sim, frame["qqq"], frame["cash"])
        if float(s["max_drawdown"]) >= hold_max_dd - 1e-12:
            chosen_l = lev
            chosen_sum = s
        else:
            break

    rob = summarize_sim(
        simulate_portfolio(
            frame, targets, avl.ROBUST_COST, start=start, end=end, leverage=chosen_l
        ),
        frame["qqq"],
        frame["cash"],
    )
    hi = summarize_sim(
        simulate_portfolio(
            frame, targets, avl.HIGH_RATE_5PCT, start=start, end=end, leverage=chosen_l
        ),
        frame["qqq"],
        frame["cash"],
    )
    return {
        "matched_leverage": chosen_l,
        "hit_grid_top": chosen_l == grid[-1],
        "main": chosen_sum,
        "robust": rob,
        "high_rate_5pct": hi,
    }


def evaluate_window(
    frame: pd.DataFrame,
    f0001_base: pd.Series,
    specs: Sequence[PortfolioSpec],
    *,
    window_id: str,
    start: date,
    end: date = DEV_CUTOFF,
) -> dict[str, Any]:
    targets_map = {s.strategy_id: compute_target_weights(frame, f0001_base, s) for s in specs}

    hold_sim = simulate_portfolio(
        frame, targets_map["hold"], avl.MAIN_COST, start=start, end=end, leverage=1.0
    )
    hold_sum = summarize_sim(hold_sim, frame["qqq"], frame["cash"])
    hold_dd = float(hold_sum["max_drawdown"])

    rows: dict[str, dict[str, Any]] = {}
    for spec in specs:
        tgt = targets_map[spec.strategy_id]
        s1_main = summarize_sim(
            simulate_portfolio(frame, tgt, avl.MAIN_COST, start=start, end=end, leverage=1.0),
            frame["qqq"],
            frame["cash"],
        )
        s1_rob = summarize_sim(
            simulate_portfolio(frame, tgt, avl.ROBUST_COST, start=start, end=end, leverage=1.0),
            frame["qqq"],
            frame["cash"],
        )
        s1_hi = summarize_sim(
            simulate_portfolio(frame, tgt, avl.HIGH_RATE_5PCT, start=start, end=end, leverage=1.0),
            frame["qqq"],
            frame["cash"],
        )
        matched = (
            match_drawdown_leverage(frame, tgt, hold_dd, start=start, end=end)
            if spec.is_candidate
            else None
        )
        rows[spec.strategy_id] = {
            "strategy_id": spec.strategy_id,
            "label": spec.label,
            "is_candidate": spec.is_candidate,
            "base_1x": {"main": s1_main, "robust": s1_rob, "high_rate_5pct": s1_hi},
            "matched_dd": matched,
        }

    return {
        "window_id": window_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "strategies": rows,
    }


def evaluate_kill_criteria(
    windows: Mapping[str, Mapping[str, Any]], specs: Sequence[PortfolioSpec]
) -> dict[str, Any]:
    pure = windows["pure_2b_2004_2010"]["strategies"]
    full = windows["spliced_full_1999_2010"]["strategies"]

    hold_p = pure["hold"]["base_1x"]["main"]
    hold_f = full["hold"]["base_1x"]["main"]
    f1_f = full["f0001_1.0"]["base_1x"]["main"]

    details: list[dict[str, Any]] = []
    passed: list[str] = []

    for spec in specs:
        if not spec.is_candidate:
            continue
        sid = spec.strategy_id

        def _pick(entry: Mapping[str, Any]) -> tuple[str, float, Mapping[str, Any]]:
            b1 = entry["base_1x"]
            md = entry["matched_dd"]
            if float(md["main"]["geometric_annual_return"]) > float(
                b1["main"]["geometric_annual_return"]
            ):
                return "matched_dd", float(md["matched_leverage"]), md
            return "base_1x", 1.0, b1

        p_mode, p_l, p_pick = _pick(pure[sid])
        f_mode, f_l, f_pick = _pick(full[sid])

        c1_sharpe = float(pure[sid]["base_1x"]["main"]["excess_sharpe"]) > float(
            hold_p["excess_sharpe"]
        ) and float(full[sid]["base_1x"]["main"]["excess_sharpe"]) > float(hold_f["excess_sharpe"])
        c2_beat_main = (
            float(p_pick["main"]["max_drawdown"]) >= float(hold_p["max_drawdown"]) - 1e-12
            and float(f_pick["main"]["max_drawdown"]) >= float(hold_f["max_drawdown"]) - 1e-12
            and float(p_pick["main"]["geometric_annual_return"])
            - float(hold_p["geometric_annual_return"])
            >= KILL_MIN_EXCESS_OVER_HOLD
            and float(f_pick["main"]["geometric_annual_return"])
            - float(hold_f["geometric_annual_return"])
            >= KILL_MIN_EXCESS_OVER_HOLD
        )
        c3_robust_and_5pct = (
            float(p_pick["robust"]["geometric_annual_return"])
            > float(hold_p["geometric_annual_return"])
            and float(p_pick["high_rate_5pct"]["geometric_annual_return"])
            > float(hold_p["geometric_annual_return"])
            and float(f_pick["robust"]["geometric_annual_return"])
            > float(hold_f["geometric_annual_return"])
            and float(f_pick["high_rate_5pct"]["geometric_annual_return"])
            > float(hold_f["geometric_annual_return"])
        )
        c4_near_f0001 = (
            float(f_pick["main"]["geometric_annual_return"])
            >= float(f1_f["geometric_annual_return"]) - KILL_MAX_GAP_VS_F0001
        )

        ok = c1_sharpe and c2_beat_main and c3_robust_and_5pct and c4_near_f0001
        if ok:
            passed.append(sid)

        details.append(
            {
                "strategy_id": sid,
                "pure_2b_pick": f"{p_mode}({p_l:.2f}x)",
                "full_dev_pick": f"{f_mode}({f_l:.2f}x)",
                "pass_all": ok,
                "checks": {
                    "c1_1x_sharpe_beats_hold_both_windows": c1_sharpe,
                    "c2_main_dd_le_hold_and_beats_hold_1pp_both": c2_beat_main,
                    "c3_robust_and_5pct_beat_hold_both": c3_robust_and_5pct,
                    "c4_full_main_within_0_5pp_of_f0001": c4_near_f0001,
                },
                "deltas": {
                    "pure_main_vs_hold": float(p_pick["main"]["geometric_annual_return"])
                    - float(hold_p["geometric_annual_return"]),
                    "pure_5pct_vs_hold": float(p_pick["high_rate_5pct"]["geometric_annual_return"])
                    - float(hold_p["geometric_annual_return"]),
                    "full_main_vs_hold": float(f_pick["main"]["geometric_annual_return"])
                    - float(hold_f["geometric_annual_return"]),
                    "full_5pct_vs_hold": float(f_pick["high_rate_5pct"]["geometric_annual_return"])
                    - float(hold_f["geometric_annual_return"]),
                    "full_main_vs_f0001": float(f_pick["main"]["geometric_annual_return"])
                    - float(f1_f["geometric_annual_return"]),
                },
            }
        )

    verdict = "pass_dev_screen" if passed else "kill_dev_screen"
    text = (
        f"开发窗口初筛通过（{', '.join(passed)}）：在纯 2-B 真实 ETF 与 1999–2010 全开发窗口"
        "及 5% 高息压力下均跨过止损线，值得讨论下一步预注册与 2011+ 样本外检验。"
        if passed
        else "开发窗口止损触发（kill_dev_screen）：无候选结构同时跨过纯 2-B 与全开发窗口"
        "四道止损线，不调参、不读 2011+ 验证窗口。"
    )
    return {"verdict": verdict, "passed_ids": passed, "text": text, "details": details}


def run_probe(
    qqq_data: BenchData,
    raw_series: Mapping[str, pd.Series],
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    specs: Sequence[PortfolioSpec] | None = None,
) -> dict[str, Any]:
    cut_qqq = gp.cut(qqq_data)
    frame, audit = build_spliced_returns(cut_qqq, raw_series)
    if not audit["ok"]:
        return {
            "task_id": TASK_ID,
            "requirement": REQUIREMENT,
            "provenance": dict(provenance),
            "check": audit,
            "stopped": {
                "reason": "data_or_overlap_check_failed",
                "text": "2-A/2-B 重叠一致性或数据完整性检查未通过，fail-closed 停止。",
            },
            "production_effect": "none",
            "broker_action": "none",
        }

    spec_list = default_specs() if specs is None else tuple(specs)
    f0001_base = lt.f0001_targets(cut_qqq, root_dir)

    windows = {
        "pure_2b_2004_2010": evaluate_window(
            frame, f0001_base, spec_list, window_id="pure_2b_2004_2010", start=PURE_2B_START
        ),
        "spliced_full_1999_2010": evaluate_window(
            frame, f0001_base, spec_list, window_id="spliced_full_1999_2010", start=FULL_DEV_START
        ),
    }
    verdict = evaluate_kill_criteria(windows, spec_list)
    return {
        "task_id": TASK_ID,
        "requirement": REQUIREMENT,
        "provenance": dict(provenance),
        "check": audit,
        "windows": windows,
        "verdict": verdict,
        "stopped": None,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _pts(x: float) -> str:
    return f"{x * 100:+.2f}"


def _md_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines


def render_markdown(result: Mapping[str, Any]) -> str:
    chk = result["check"]
    lines = [
        "# TRADING-2575：QQQ 多资产低相关分散提夏普再配平开发窗口诊断",
        "",
        f"- 任务：`{result['task_id']}`；需求：`{result['requirement']}`",
        f"- 数据截断：机械截断于 `{chk['cutoff_date']}`（实际最大日期 `{chk['max_loaded_date']}`，"
        "零 2011+ 数据请求与读取）",
        f"- 拼接口径（2-B 主 + 2-A 补齐）：`GLD` 起自 `{chk['splice_dates']['gold_2b_start']}`，"
        f"`IEF` 起自 `{chk['splice_dates']['treasury_10y_2b_start']}`；"
        f"重叠一致性 **`{'PASS' if chk['overlap_ok'] else 'FAIL'}`**",
        f"- 判定：**{result['verdict']['verdict']}** —— {result['verdict']['text']}",
        "",
        "## 1. 2-A 与 2-B 重叠区域数据一致性核验（Fail-Closed 门禁）",
        "",
    ]

    ov_rows = []
    for item in chk["overlap_pairs"]:
        role = "主拼接（必过）" if item["required"] else "参照披露"
        ov_rows.append(
            [
                f"`{item['name']}`",
                role,
                f"{item['start']}..{item['end']}",
                str(item["overlap_days"]),
                f"{item['daily_corr']:.4f}",
                f"{item['weekly_corr']:.4f}",
                f"{_pct(item['etf_cagr'])} / {_pct(item['proxy_cagr'])}",
                _pts(item["annual_return_diff"]),
                _pct(item["annual_tracking_error"]),
                f"{_pct(item['max_abs_daily_diff'])} ({item['max_abs_daily_diff_date']})",
                "PASS" if item["ok"] else "FAIL",
            ]
        )
    lines += _md_table(
        [
            "配对",
            "角色",
            "重叠区间",
            "重叠日",
            "日相关",
            "周相关",
            "ETF/代理年化",
            "年化差(ETF-代理)",
            "年化跟踪误差",
            "最大单日差",
            "门禁",
        ],
        ov_rows,
    )

    for wkey, wtitle in (
        (
            "pure_2b_2004_2010",
            "2. 主评估窗口：纯 2-B 真实 ETF（2004-11-30..2010-12-31，零代理误差）",
        ),
        (
            "spliced_full_1999_2010",
            "3. 全开发窗口：2-B 主 + 2-A 补齐（1999-03-31..2010-12-31，含双慢熊）",
        ),
    ):
        w = result["windows"][wkey]
        st = w["strategies"]
        hold_geo = st["hold"]["base_1x"]["main"]["geometric_annual_return"]
        lines += [f"## {wtitle}", ""]
        rows = []
        for sid, entry in st.items():
            b1 = entry["base_1x"]["main"]
            md = entry["matched_dd"]
            if md is None:
                l_txt = "—"
                lm = lr = lh = ldd = "—"
            else:
                l_txt = f"{md['matched_leverage']:.2f}x"
                m_geo = md["main"]["geometric_annual_return"]
                lm = f"{_pct(m_geo)} ({_pts(m_geo - hold_geo)})"
                lr = _pct(md["robust"]["geometric_annual_return"])
                lh = _pct(md["high_rate_5pct"]["geometric_annual_return"])
                ldd = _pct(md["main"]["max_drawdown"])
            rows.append(
                [
                    f"`{sid}`",
                    _pct(b1["geometric_annual_return"]),
                    _pct(b1["max_drawdown"]),
                    _pct(b1["annual_volatility"]),
                    f"{b1['excess_sharpe']:.2f}",
                    f"{b1['corr_with_qqq']:.2f}",
                    l_txt,
                    lm,
                    ldd,
                    lr,
                    lh,
                ]
            )
        lines += _md_table(
            [
                "结构",
                "1x 年化",
                "1x 回撤",
                "1x 波动",
                "1x 夏普",
                "与QQQ相关",
                "同回撤 L*",
                "L* 主算年化(对Hold)",
                "L* 回撤",
                "L* 稳健年化",
                "L* 5%高息年化",
            ],
            rows,
        )

    lines += ["## 4. 开发窗口止损线逐项核对", ""]
    k_rows = []
    for item in result["verdict"]["details"]:
        c = item["checks"]
        d = item["deltas"]
        k_rows.append(
            [
                f"`{item['strategy_id']}`",
                f"{item['pure_2b_pick']} / {item['full_dev_pick']}",
                "PASS" if c["c1_1x_sharpe_beats_hold_both_windows"] else "FAIL",
                f"{'PASS' if c['c2_main_dd_le_hold_and_beats_hold_1pp_both'] else 'FAIL'} "
                f"(2B{_pts(d['pure_main_vs_hold'])}/全{_pts(d['full_main_vs_hold'])})",
                f"{'PASS' if c['c3_robust_and_5pct_beat_hold_both'] else 'FAIL'} "
                f"(5%高息 2B{_pts(d['pure_5pct_vs_hold'])}/全{_pts(d['full_5pct_vs_hold'])})",
                f"{'PASS' if c['c4_full_main_within_0_5pp_of_f0001'] else 'FAIL'} "
                f"(对F0001 {_pts(d['full_main_vs_f0001'])})",
                "**PASS**" if item["pass_all"] else "**KILL**",
            ]
        )
    lines += _md_table(
        [
            "候选结构",
            "取优口径(2B/全)",
            "1.双窗1x夏普>Hold",
            "2.双窗主算≥Hold+1且回撤≤Hold",
            "3.双窗稳健与5%高息>Hold",
            "4.全窗距F0001≤0.5",
            "总判",
        ],
        k_rows,
    )

    lines += [
        "---",
        "披露：结构 D1/D2 中的原油（`DCOILWTICO`）为现货价格代理，未扣期货负展期（Contango）损耗，"
        f"且 1999 年有 `{chk.get('oil_carried_beyond_5d_days', 0)}` 个 FRED 缺报日"
        "按最多 10 日前值顺延（0% 日收益）；"
        "结构 A/B/C 在 2004-11-30 起为 100% 真实 ETF（`QQQ`/`GLD`/`IEF`），此前用 2-A 代理补齐。",
        "本表仅为开发窗口诊断，不构成验证窗口结论、投资建议或生产规则。",
        "`production_effect=none`；`broker_action=none`。",
        "",
    ]
    return "\n".join(lines)


def load_qqq_dev_data(snapshot_root: Path) -> tuple[BenchData, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    _, plan = tr.load_trial(root_dir, DATA_TRIAL)
    data = from_snapshot(snapshot_root / plan.data.snapshot_id, qqq_series="actual")
    return gp.cut(data), loaded, root_dir


@app.callback()
def main() -> None:
    """TRADING-2575：QQQ 多资产低相关分散提夏普开发窗口诊断。"""


@app.command("fetch")
def fetch_command(
    snapshot: Annotated[Path, typer.Option(help="本地多资产快照目录（不进 git）。")],
) -> None:
    load_dotenv()
    api_key = os.environ.get(qlh.FMP_API_KEY_ENV, "").strip()
    if not api_key:
        typer.echo("multi_asset: 缺少 FMP_API_KEY，无法拉取 2-B ETF 数据。")
        raise typer.Exit(code=1)
    fetch_multi_asset_snapshot(
        snapshot,
        rate_provider=FredRateProvider(),
        price_provider=FmpPriceProvider(api_key=api_key),
        end=DEV_CUTOFF,
    )
    manifest = json.loads((snapshot / MANIFEST_JSON).read_text(encoding="utf-8"))
    typer.echo(
        f"multi_asset: 快照已保存至 {snapshot}；总行数 {manifest['row_count']}，"
        f"截止 {manifest['fetch_end']}，SHA256 {manifest['series_csv_sha256'][:12]}"
    )


@app.command("check")
def check_command(
    snapshot: Annotated[Path, typer.Option(help="多资产快照目录。")],
    out: Annotated[Path, typer.Option(help="检查输出目录。")],
    qqq_root: Annotated[
        Path, typer.Option(help="QQQ 长历史快照根目录。")
    ] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    qqq_data, _, _ = load_qqq_dev_data(qqq_root)
    raw_series, _ = load_multi_asset_snapshot(snapshot)
    _, audit = build_spliced_returns(qqq_data, raw_series)
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(
        json.dumps(gp._clean(dict(audit)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    pairs = "；".join(
        f"{p['name']} corr={p['daily_corr']:.3f}/wk={p['weekly_corr']:.3f} "
        f"diff={p['annual_return_diff'] * 100:+.2f}pp ({'PASS' if p['ok'] else 'FAIL'})"
        for p in audit["overlap_pairs"]
    )
    typer.echo(f"multi_asset check: {'PASS' if audit['ok'] else 'FAIL'} | {pairs}")
    if not audit["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    snapshot: Annotated[Path, typer.Option(help="多资产快照目录。")],
    out: Annotated[Path, typer.Option(help="结果输出目录（拒绝覆盖）。")],
    qqq_root: Annotated[
        Path, typer.Option(help="QQQ 长历史快照根目录。")
    ] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("multi_asset: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    qqq_data, _, root_dir = load_qqq_dev_data(qqq_root)
    raw_series, _ = load_multi_asset_snapshot(snapshot)
    result = run_probe(qqq_data, raw_series, root_dir, provenance=git_provenance(PROJECT_ROOT))
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(
        json.dumps(gp._clean(dict(result["check"])), indent=2, ensure_ascii=False, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    if result["stopped"] is not None:
        typer.echo(result["stopped"]["text"])
        raise typer.Exit(code=1)
    out.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")
    typer.echo(f"multi_asset: 判定 {result['verdict']['verdict']}；结果目录 {out}")


if __name__ == "__main__":
    app()
