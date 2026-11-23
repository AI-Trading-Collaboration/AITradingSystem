"""One-shot preregistered 2011-2021 validation probe for Path 2 multi-asset Return Stacking
(TRADING-2575).

Executes the preregistered protocol locked in
`docs/requirements/TRADING-2575_QQQ_Multi_Asset_Diversification_Dev_Probe.md` Section 6 before
any 2011+ data were downloaded or read:
- Evaluation window: 2010-12-31 close opening -> 2011-01-03..2021-02-19 returns (~2,549 days).
- Hard cutoff: 2021-02-19 across fetch, load, check, and run. Zero 2021-02-22+ consistency
  window data are requested, stored, or read.
- Data: 100% real 2-B ETFs (QQQ/DTB3 from checked qlh snapshot, GLD/IEF from FMP
  dividend-adjusted EOD with SHA-256 manifest and 2010 overlap drift check against dev snapshot).
- Primary candidates (fixed L = 1.50x, no hindsight L* or parameter search):
  1. stack_static_1.5: 1.50 * (60% QQQ + 20% IEF + 20% GLD) = 90% QQQ + 30% IEF + 30% GLD
  2. stack_trend_1.5: 1.50 * (60% QQQ + 20% GLD[252d>0] + 20% IEF[252d>0])
- Print-only controls (never affect verdict): 1x static, 1x trend shield, 70/30 gold at 1.35x,
  and F0001 1.0x.
- One-shot CLI refuses overwrite. production_effect=none; broker_action=none.
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
from typing import Annotated, Any, Literal

import numpy as np
import pandas as pd
import typer
from dotenv import load_dotenv

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.data.market_data import FmpPriceProvider, PriceDataProvider
from ai_trading_system.research_bench import falsify as fl
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_snapshot
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import LoadedSettings, load_settings
from ai_trading_system.research_bench.simulate import schedule_mask
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import asym_vxn_leverage as avl
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.research_probes import multi_asset_diversification as mad

TASK_ID = "TRADING-2575_QQQ_MULTI_ASSET_DIVERSIFICATION"
REQUIREMENT = "docs/requirements/TRADING-2575_QQQ_Multi_Asset_Diversification_Dev_Probe.md"
DATA_TRIAL = lt.DATA_TRIAL
TRADING_DAYS = 252

# Hard validation boundaries aligned with config/research/primary_research_window_policy.yaml
# (2011-01-03..2021-02-19) and Requirement Section 6. Zero 2021-02-22+ data may be touched.
VAL_FETCH_START = date(2009, 10, 1)
VAL_START = date(2010, 12, 31)
VAL_END = date(2021, 2, 19)
VAL_ETFS: tuple[str, ...] = ("GLD", "IEF")

# Pre-registered 2010 dev-overlap consistency & validation verdict criteria (Requirement §6).
DEV_OVERLAP_START = date(2010, 1, 4)
DEV_OVERLAP_END = date(2010, 12, 31)
DEV_OVERLAP_MAX_DAILY_DIFF = 1e-4
MIN_VAL_TRADING_DAYS = 2500
MIN_DEV_OVERLAP_DAYS = 240

VERDICT_MAX_DD_TOL = 0.010
VERDICT_MAIN_MIN_EXCESS = 0.0
VERDICT_ROBUST_MAX_GAP = 0.015

# Deterministic Politis-Romano stationary block bootstrap parameters (21 trading days ~ 1 month).
BOOTSTRAP_DRAWS = 2000
BOOTSTRAP_MEAN_BLOCK = 21.0
BOOTSTRAP_SEED = 20261123
BOOTSTRAP_CONFIDENCE = 0.95

SERIES_CSV = "series.csv"
MANIFEST_JSON = "manifest.json"
CHECK_JSON = "multi_asset_val_2011_check.json"
RESULT_JSON = "multi_asset_validation_2011.json"
RESULT_MD = "multi_asset_validation_2011.md"

CandidateRole = Literal["benchmark", "primary", "control"]


@dataclass(frozen=True)
class ValCandidate:
    strategy_id: str
    label: str
    mode: mad.StructureMode
    leverage: float
    role: CandidateRole


def default_candidates() -> tuple[ValCandidate, ...]:
    """Frozen validation roster from Requirement §6 (no parameter search)."""

    return (
        ValCandidate("hold", "买入持有 QQQ (1.0x 基准)", "hold", 1.0, "benchmark"),
        ValCandidate(
            "stack_static_1.5",
            "主测 1：静态三盾叠加 1.50x (90% QQQ + 30% IEF + 30% GLD)",
            "qqq60_ief20_gold20",
            1.50,
            "primary",
        ),
        ValCandidate(
            "stack_trend_1.5",
            "主测 2：趋势双盾叠加 1.50x (90% QQQ + 30% GLD/30% IEF 252日正趋势)",
            "qqq60_trend_shield40",
            1.50,
            "primary",
        ),
        ValCandidate(
            "qqq60_ief20_gold20_1.0",
            "对照：静态三盾 1.00x (60% QQQ + 20% IEF + 20% GLD)",
            "qqq60_ief20_gold20",
            1.00,
            "control",
        ),
        ValCandidate(
            "qqq60_trend_shield40_1.0",
            "对照：趋势双盾 1.00x (60% QQQ + 20% GLD/20% IEF 趋势)",
            "qqq60_trend_shield40",
            1.00,
            "control",
        ),
        ValCandidate(
            "qqq70_gold30_1.35",
            "对照：黄金单盾 1.35x (94.5% QQQ + 40.5% GLD)",
            "qqq70_gold30",
            1.35,
            "control",
        ),
        ValCandidate(
            "f0001_1.0",
            "对照：F0001 原版 1.00x (纯 QQQ 月频趋势)",
            "f0001",
            1.00,
            "control",
        ),
    )


app = typer.Typer(help="TRADING-2575：2011–2021 验证窗口一次性预注册检验。")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def fetch_validation_snapshot(
    snapshot_dir: Path,
    *,
    price_provider: PriceDataProvider,
    end: date = VAL_END,
    downloaded_at: datetime | None = None,
) -> Path:
    """Download GLD and IEF dividend-adjusted EOD strictly capped at 2021-02-19."""

    if end > VAL_END:
        raise ValueError(f"refuses to fetch validation data after {VAL_END}: {end}")

    frames: list[pd.DataFrame] = []
    for ticker in VAL_ETFS:
        reqs = qlh.etf_reference_requests(ticker, VAL_FETCH_START, end)
        rows = qlh._download_reference(price_provider, reqs)
        rows = rows.loc[pd.to_datetime(rows["date"]).dt.date <= end].copy()
        if rows.empty:
            raise ValueError(f"FMP returned no rows for {ticker} up to {end}")
        part = rows.rename(columns={"adj_close": "value"}).assign(series=ticker)
        frames.append(part[["date", "series", "value"]])

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.strftime("%Y-%m-%d")
    combined["value"] = combined["value"].astype(float)
    combined = (
        combined.drop_duplicates(subset=["date", "series"], keep="last")
        .sort_values(["series", "date"])
        .reset_index(drop=True)
    )
    if pd.to_datetime(combined["date"]).max().date() > VAL_END:
        raise ValueError(f"fetched validation data exceeded {VAL_END}")

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
        "schema_version": "path2_val_2011_snapshot.v1",
        "task_id": TASK_ID,
        "fetch_start": VAL_FETCH_START.isoformat(),
        "fetch_end": end.isoformat(),
        "cutoff_enforced": VAL_END.isoformat(),
        "downloaded_at": (downloaded_at or datetime.now(tz=UTC)).isoformat(),
        "series_csv_sha256": digest,
        "row_count": int(len(combined)),
        "providers": {
            "fmp": {
                "tickers": list(VAL_ETFS),
                "endpoint": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted",
            },
        },
        "series": series_summary,
        "data_policy": {"local_only": True, "redistribution_allowed": False},
    }
    snapshot_dir.joinpath(MANIFEST_JSON).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return snapshot_dir


def load_validation_snapshot(snapshot_dir: Path) -> tuple[dict[str, pd.Series], dict[str, Any]]:
    """Load and SHA-256 verify the 2011-2021 validation snapshot; refuse > 2021-02-19."""

    manifest_path = snapshot_dir / MANIFEST_JSON
    series_path = snapshot_dir / SERIES_CSV
    if not manifest_path.exists() or not series_path.exists():
        raise FileNotFoundError(f"validation snapshot incomplete under {snapshot_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_bytes = series_path.read_bytes()
    actual_sha = _sha256_bytes(raw_bytes)
    if actual_sha != manifest.get("series_csv_sha256"):
        raise ValueError("validation snapshot SHA-256 mismatch; refusing to read")

    frame = pd.read_csv(series_path)
    dates = pd.to_datetime(frame["date"])
    if dates.max().date() > VAL_END:
        raise ValueError(f"validation snapshot exceeds hard cutoff {VAL_END}")

    out: dict[str, pd.Series] = {}
    for sid, grp in frame.groupby("series"):
        s = pd.Series(
            grp["value"].to_numpy(dtype=float),
            index=pd.DatetimeIndex(pd.to_datetime(grp["date"])),
            name=str(sid),
        ).sort_index()
        if s.index.has_duplicates:
            raise ValueError(f"duplicate dates in validation snapshot series {sid}")
        out[str(sid)] = s
    missing = [t for t in VAL_ETFS if t not in out]
    if missing:
        raise ValueError(f"validation snapshot missing required ETFs: {', '.join(missing)}")
    return out, manifest


def check_dev_overlap(
    val_series: Mapping[str, pd.Series],
    dev_series: Mapping[str, pd.Series],
    calendar: pd.DatetimeIndex,
    *,
    min_overlap_days: int = MIN_DEV_OVERLAP_DAYS,
) -> dict[str, Any]:
    """Compare 2010 GLD/IEF daily returns between validation and dev snapshots for drift."""

    ov_cal = calendar[
        (calendar >= pd.Timestamp(DEV_OVERLAP_START)) & (calendar <= pd.Timestamp(DEV_OVERLAP_END))
    ]
    pairs: list[dict[str, Any]] = []
    for ticker in VAL_ETFS:
        if ticker not in dev_series:
            pairs.append({"ticker": ticker, "ok": False, "reason": "missing_in_dev_snapshot"})
            continue
        v_ret = mad.level_to_returns(val_series[ticker], calendar).reindex(ov_cal)
        d_ret = mad.level_to_returns(dev_series[ticker], calendar).reindex(ov_cal)
        both = pd.concat({"val": v_ret, "dev": d_ret}, axis=1).dropna()
        if len(both) < min_overlap_days:
            pairs.append(
                {
                    "ticker": ticker,
                    "ok": False,
                    "overlap_days": int(len(both)),
                    "reason": "insufficient_2010_overlap",
                }
            )
            continue
        diff = np.abs(both["val"].to_numpy(dtype=float) - both["dev"].to_numpy(dtype=float))
        max_idx = int(np.argmax(diff))
        max_diff = float(diff[max_idx])
        ok = max_diff <= DEV_OVERLAP_MAX_DAILY_DIFF
        pairs.append(
            {
                "ticker": ticker,
                "ok": ok,
                "start": both.index[0].date().isoformat(),
                "end": both.index[-1].date().isoformat(),
                "overlap_days": int(len(both)),
                "max_abs_daily_diff": max_diff,
                "max_diff_date": both.index[max_idx].date().isoformat(),
            }
        )
    return {
        "checked": True,
        "ok": all(item["ok"] for item in pairs),
        "pairs": pairs,
    }


def build_validation_frame(
    qqq_data: BenchData,
    raw_series: Mapping[str, pd.Series],
    *,
    dev_raw_series: Mapping[str, pd.Series] | None = None,
    start: date = VAL_START,
    end: date = VAL_END,
    min_val_days: int = MIN_VAL_TRADING_DAYS,
    min_dev_overlap_days: int = MIN_DEV_OVERLAP_DAYS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build 100% real 2-B ETF daily return matrix mechanically cut at 2021-02-19."""

    if end > VAL_END:
        raise ValueError(f"refuses validation end date after {VAL_END}: {end}")
    cut = gp.cut(qqq_data, cutoff=end)
    calendar = pd.DatetimeIndex(cut.returns.index)
    if calendar.max().date() > VAL_END:
        raise ValueError(f"qqq_data exceeds hard validation cutoff {VAL_END}")

    gld_ret = mad.level_to_returns(raw_series["GLD"], calendar)
    ief_ret = mad.level_to_returns(raw_series["IEF"], calendar)

    frame = pd.DataFrame(
        {
            "qqq": cut.returns["qqq"].astype(float),
            "cash": cut.returns["cash"].astype(float),
            "gold": gld_ret.astype(float),
            "treasury_10y": ief_ret.astype(float),
            "oil": 0.0,
            "usd": 0.0,
        },
        index=calendar,
    )

    in_val = frame.loc[
        (calendar > pd.Timestamp(start)) & (calendar <= pd.Timestamp(end)),
        ["qqq", "cash", "gold", "treasury_10y"],
    ]
    missing_val = int((~np.isfinite(in_val.to_numpy(dtype=float))).sum())

    # Check opening month-end before `start` has valid 252-day trailing warmup for trend shields.
    first = int(calendar.searchsorted(pd.Timestamp(start)))
    month_end = schedule_mask(calendar, "month_end")
    opening = next((p for p in range(first - 1, -1, -1) if month_end[p]), None)
    warmup_ok = False
    opening_date: str | None = None
    if opening is not None:
        opening_date = calendar[opening].date().isoformat()
        g_tr = mad._trailing_return(frame["gold"], mad.TREND_LOOKBACK)
        t_tr = mad._trailing_return(frame["treasury_10y"], mad.TREND_LOOKBACK)
        warmup_ok = bool(np.isfinite(g_tr.iloc[opening]) and np.isfinite(t_tr.iloc[opening]))

    dev_overlap: dict[str, Any] = {"checked": False, "ok": True, "pairs": []}
    if dev_raw_series is not None:
        dev_overlap = check_dev_overlap(
            raw_series, dev_raw_series, calendar, min_overlap_days=min_dev_overlap_days
        )

    ok = (
        missing_val == 0
        and len(in_val) >= min_val_days
        and warmup_ok
        and bool(dev_overlap["ok"])
        and calendar.max().date() <= VAL_END
    )
    audit = {
        "ok": ok,
        "cutoff_date": VAL_END.isoformat(),
        "max_loaded_date": calendar.max().date().isoformat(),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "first_return_date": in_val.index[0].date().isoformat() if len(in_val) else None,
        "last_return_date": in_val.index[-1].date().isoformat() if len(in_val) else None,
        "opening_decision_date": opening_date,
        "warmup_252d_ok": warmup_ok,
        "val_trading_days": int(len(in_val)),
        "missing_val_cells": missing_val,
        "dev_overlap_2010": dev_overlap,
    }
    return frame, audit


def simulate_validation_portfolio(
    frame: pd.DataFrame,
    targets: pd.DataFrame,
    cost: avl.CostModel,
    *,
    start: date = VAL_START,
    end: date = VAL_END,
    leverage: float = 1.0,
) -> mad.SimResult:
    """Monthly-rebalanced multi-asset simulation hard-capped at 2021-02-19 (Scheme A financing)."""

    if end > VAL_END:
        raise ValueError(f"refuses validation end date after {VAL_END}: {end}")
    index = pd.DatetimeIndex(frame.index)
    if index.max().date() > VAL_END:
        raise ValueError(f"frame exceeds hard validation cutoff {VAL_END}")

    first = int(index.searchsorted(pd.Timestamp(start)))
    last = int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")

    risky_mat = frame[list(mad.RISKY_ASSETS)].to_numpy(dtype=float)
    cash_vec = frame["cash"].to_numpy(dtype=float)
    tgt_mat = targets[list(mad.RISKY_ASSETS)].to_numpy(dtype=float) * leverage
    month_end = schedule_mask(index, "month_end")

    opening = next((p for p in range(first - 1, -1, -1) if month_end[p]), None)
    if opening is None:
        raise ValueError("no month-end before start; extend warmup")

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
    return mad.SimResult(
        returns=pd.Series(rets, index=dates),
        gross_exposure=pd.Series(gross_exps, index=dates),
        net_borrowed=pd.Series(borrows, index=dates),
        turnover=pd.Series(turns, index=dates),
        trade_cost=pd.Series(tcosts, index=dates),
        financing_cost=pd.Series(fcosts, index=dates),
        start=index[first],
        bust_date=bust_date,
    )


def _geo_252(arr: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    # Annualized geometric return over trading-day axis=-1 (supports 1D or 2D draws).
    n = arr.shape[-1]
    log_sum = np.sum(np.log1p(np.maximum(arr, -0.999999)), axis=-1)
    return np.expm1(log_sum * (TRADING_DAYS / n))


def _max_dd(arr: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    eq = np.cumprod(1.0 + arr, axis=-1)
    ones = np.ones(arr.shape[:-1] + (1,), dtype=float)
    eq_full = np.concatenate([ones, eq], axis=-1)
    peaks = np.maximum.accumulate(eq_full, axis=-1)
    return np.min(eq_full / peaks - 1.0, axis=-1)


def _sharpe(arr: np.ndarray[Any, Any], cash: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    ex = arr - cash
    sd = np.std(ex, axis=-1, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(sd > 0.0, np.mean(ex, axis=-1) / sd * math.sqrt(TRADING_DAYS), 0.0)


def bootstrap_paired_ci(
    strat_returns: pd.Series,
    hold_returns: pd.Series,
    cash_returns: pd.Series,
    *,
    draws: int = BOOTSTRAP_DRAWS,
    mean_block: float = BOOTSTRAP_MEAN_BLOCK,
    seed: int = BOOTSTRAP_SEED,
    confidence: float = BOOTSTRAP_CONFIDENCE,
) -> dict[str, Any]:
    """Stationary block bootstrap two-sided CI for strategy minus hold on daily returns."""

    s = strat_returns.to_numpy(dtype=float)
    h = hold_returns.reindex(strat_returns.index).to_numpy(dtype=float)
    c = cash_returns.reindex(strat_returns.index).to_numpy(dtype=float)
    rng = np.random.default_rng(seed)

    geo_diffs: list[np.ndarray[Any, Any]] = []
    dd_diffs: list[np.ndarray[Any, Any]] = []
    sr_diffs: list[np.ndarray[Any, Any]] = []
    remaining = draws
    chunk = 250
    while remaining > 0:
        n_draw = min(chunk, remaining)
        remaining -= n_draw
        idx = fl.stationary_bootstrap_indices(len(s), n_draw, mean_block, rng)
        sb, hb, cb = s[idx], h[idx], c[idx]
        geo_diffs.append(_geo_252(sb) - _geo_252(hb))
        dd_diffs.append(_max_dd(sb) - _max_dd(hb))
        sr_diffs.append(_sharpe(sb, cb) - _sharpe(hb, cb))

    tail = (1.0 - confidence) / 2.0

    def _ci(chunks: list[np.ndarray[Any, Any]]) -> dict[str, float]:
        vals = np.concatenate(chunks)
        return {
            "low": float(np.quantile(vals, tail)),
            "median": float(np.quantile(vals, 0.5)),
            "high": float(np.quantile(vals, 1.0 - tail)),
            "prob_positive": float(np.mean(vals > 0.0)),
        }

    return {
        "draws": draws,
        "mean_block_days": mean_block,
        "confidence": confidence,
        "geo_diff": _ci(geo_diffs),
        "max_dd_diff": _ci(dd_diffs),
        "sharpe_diff": _ci(sr_diffs),
    }


def yearly_and_loyo(
    strat_returns: pd.Series,
    hold_returns: pd.Series,
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    """Calendar-year returns vs hold, plus leave-one-year-out worst/best geo diff."""

    idx = pd.DatetimeIndex(strat_returns.index)
    h = hold_returns.reindex(idx)
    years = sorted({int(y) for y in idx.year})
    by_year: dict[str, dict[str, float]] = {}
    loyo_diffs: dict[str, float] = {}

    for yr in years:
        mask = idx.year == yr
        s_yr = float(np.prod(1.0 + strat_returns.loc[mask].to_numpy(dtype=float)) - 1.0)
        h_yr = float(np.prod(1.0 + h.loc[mask].to_numpy(dtype=float)) - 1.0)
        by_year[str(yr)] = {
            "strategy": s_yr,
            "hold": h_yr,
            "diff": s_yr - h_yr,
        }
        keep = ~mask
        if int(keep.sum()) > 10:
            s_keep = strat_returns.loc[keep].to_numpy(dtype=float)
            h_keep = h.loc[keep].to_numpy(dtype=float)
            loyo_diffs[str(yr)] = float(_geo_252(s_keep) - _geo_252(h_keep))

    worst_yr = min(loyo_diffs, key=lambda k: loyo_diffs[k]) if loyo_diffs else None
    best_yr = max(loyo_diffs, key=lambda k: loyo_diffs[k]) if loyo_diffs else None
    return by_year, {
        "by_removed_year": loyo_diffs,
        "worst_removed_year": worst_yr,
        "worst_geo_diff": loyo_diffs[worst_yr] if worst_yr is not None else float("nan"),
        "best_removed_year": best_yr,
        "best_geo_diff": loyo_diffs[best_yr] if best_yr is not None else float("nan"),
    }


def summarize_assets(frame: pd.DataFrame, *, start: date, end: date) -> dict[str, dict[str, float]]:
    """Buy-and-hold summary of QQQ, GLD, IEF, and DTB3 over the validation window."""

    idx = pd.DatetimeIndex(frame.index)
    sub = frame.loc[(idx > pd.Timestamp(start)) & (idx <= pd.Timestamp(end))]
    start_ts = idx[int(idx.searchsorted(pd.Timestamp(start)))]
    years = (sub.index[-1] - start_ts).days / DAYS_PER_YEAR
    cash = sub["cash"].to_numpy(dtype=float)
    out: dict[str, dict[str, float]] = {}
    for col, label in (
        ("qqq", "QQQ"),
        ("gold", "GLD"),
        ("treasury_10y", "IEF"),
        ("cash", "DTB3"),
    ):
        r = sub[col].to_numpy(dtype=float)
        eq = np.concatenate([[1.0], np.cumprod(1.0 + r)])
        geo = float(eq[-1] ** (1.0 / years) - 1.0)
        vol = float(np.std(r, ddof=1) * math.sqrt(TRADING_DAYS))
        dd = float(np.min(eq / np.maximum.accumulate(eq) - 1.0))
        ex = r - cash
        ex_sd = float(np.std(ex, ddof=1))
        sr = float(np.mean(ex) / ex_sd * math.sqrt(TRADING_DAYS)) if ex_sd > 0.0 else 0.0
        corr_q = float(np.corrcoef(r, sub["qqq"].to_numpy(dtype=float))[0, 1]) if vol > 0 else 0.0
        out[label] = {
            "geometric_annual_return": geo,
            "annual_volatility": vol,
            "max_drawdown": dd,
            "excess_sharpe": sr,
            "corr_with_qqq": corr_q,
        }
    return out


def evaluate_validation_verdict(
    strategies: Mapping[str, Mapping[str, Any]],
    candidates: Sequence[ValCandidate],
) -> dict[str, Any]:
    """Apply frozen Requirement §6 one-shot verdict strictly to primary candidates."""

    hold_main = strategies["hold"]["main"]
    hold_geo = float(hold_main["geometric_annual_return"])
    hold_dd = float(hold_main["max_drawdown"])
    hold_sr = float(hold_main["excess_sharpe"])

    details: list[dict[str, Any]] = []
    passed: list[str] = []

    for cand in candidates:
        if cand.role != "primary":
            continue
        entry = strategies[cand.strategy_id]
        m = entry["main"]
        r = entry["robust"]
        h5 = entry["high_rate_5pct"]

        m_geo = float(m["geometric_annual_return"])
        r_geo = float(r["geometric_annual_return"])
        h5_geo = float(h5["geometric_annual_return"])
        m_dd = float(m["max_drawdown"])
        m_sr = float(m["excess_sharpe"])

        c1_dd = m_dd >= hold_dd - VERDICT_MAX_DD_TOL - 1e-12
        c2_ret = (m_geo >= hold_geo + VERDICT_MAIN_MIN_EXCESS - 1e-12) and (
            r_geo >= hold_geo - VERDICT_ROBUST_MAX_GAP - 1e-12
        )
        c3_sr = m_sr > hold_sr
        ok = c1_dd and c2_ret and c3_sr
        if ok:
            passed.append(cand.strategy_id)

        details.append(
            {
                "strategy_id": cand.strategy_id,
                "pass_all": ok,
                "checks": {
                    "c1_dd_within_1pp_of_hold": c1_dd,
                    "c2_main_ge_hold_and_robust_within_1_5pp": c2_ret,
                    "c3_main_sharpe_beats_hold": c3_sr,
                },
                "deltas": {
                    "main_geo_vs_hold": m_geo - hold_geo,
                    "robust_geo_vs_hold": r_geo - hold_geo,
                    "high_rate_5pct_geo_vs_hold": h5_geo - hold_geo,
                    "main_dd_vs_hold": m_dd - hold_dd,
                    "main_sharpe_vs_hold": m_sr - hold_sr,
                },
            }
        )

    verdict = "pass_validation" if passed else "fail_validation"
    text = (
        f"2011–2021 一次性验证通过（{', '.join(passed)}）：在 QQQ 超级牛市与黄金漫长熊市中仍守住"
        "最大回撤约束、几何年化不输 QQQ 且提高超额夏普，获得申请读一次 2021-02-22..2026 "
        "一致性窗口的资格。"
        if passed
        else "2011–2021 一次性验证未通过（fail_validation）：两条预注册主测候选（stack_static_1.5、"
        "stack_trend_1.5）均未同时满足回撤、收益与超额夏普三条底线；严格按预注册纪律收题，"
        "不改比例或杠杆倍数重读 2011–2021。"
    )
    return {"verdict": verdict, "passed_ids": passed, "text": text, "details": details}


def run_validation(
    qqq_data: BenchData,
    raw_series: Mapping[str, pd.Series],
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    dev_raw_series: Mapping[str, pd.Series] | None = None,
    candidates: Sequence[ValCandidate] | None = None,
    start: date = VAL_START,
    end: date = VAL_END,
    min_val_days: int = MIN_VAL_TRADING_DAYS,
    min_dev_overlap_days: int = MIN_DEV_OVERLAP_DAYS,
    bootstrap_draws: int = BOOTSTRAP_DRAWS,
) -> dict[str, Any]:
    """Execute the one-shot 2011-2021 validation; fail closed if data checks fail."""

    if end > VAL_END:
        raise ValueError(f"refuses validation end date after {VAL_END}: {end}")
    cut_qqq = gp.cut(qqq_data, cutoff=end)
    frame, audit = build_validation_frame(
        cut_qqq,
        raw_series,
        dev_raw_series=dev_raw_series,
        start=start,
        end=end,
        min_val_days=min_val_days,
        min_dev_overlap_days=min_dev_overlap_days,
    )
    if not audit["ok"]:
        return {
            "task_id": TASK_ID,
            "requirement": REQUIREMENT,
            "provenance": dict(provenance),
            "check": audit,
            "stopped": {
                "reason": "validation_data_check_failed",
                "text": (
                    "2011–2021 验证窗口数据完整性、预热或 2010 重叠核验未通过，fail-closed 停止。"
                ),
            },
            "production_effect": "none",
            "broker_action": "none",
        }

    cand_list = default_candidates() if candidates is None else tuple(candidates)
    f0001_base = lt.f0001_targets(cut_qqq, root_dir)

    targets_map: dict[str, pd.DataFrame] = {}
    for cand in cand_list:
        spec = mad.PortfolioSpec(
            cand.strategy_id, cand.label, cand.mode, is_candidate=(cand.role == "primary")
        )
        targets_map[cand.strategy_id] = mad.compute_target_weights(frame, f0001_base, spec)

    hold_sim = simulate_validation_portfolio(
        frame, targets_map["hold"], avl.MAIN_COST, start=start, end=end, leverage=1.0
    )

    strategies: dict[str, dict[str, Any]] = {}
    for cand in cand_list:
        tgt = targets_map[cand.strategy_id]
        sim_main = simulate_validation_portfolio(
            frame, tgt, avl.MAIN_COST, start=start, end=end, leverage=cand.leverage
        )
        sim_rob = simulate_validation_portfolio(
            frame, tgt, avl.ROBUST_COST, start=start, end=end, leverage=cand.leverage
        )
        sim_hi = simulate_validation_portfolio(
            frame, tgt, avl.HIGH_RATE_5PCT, start=start, end=end, leverage=cand.leverage
        )
        m_sum = mad.summarize_sim(sim_main, frame["qqq"], frame["cash"])
        r_sum = mad.summarize_sim(sim_rob, frame["qqq"], frame["cash"])
        h_sum = mad.summarize_sim(sim_hi, frame["qqq"], frame["cash"])

        yearly, loyo = yearly_and_loyo(sim_main.returns, hold_sim.returns)
        boot = (
            None
            if cand.strategy_id == "hold"
            else bootstrap_paired_ci(
                sim_main.returns,
                hold_sim.returns,
                frame["cash"],
                draws=bootstrap_draws,
            )
        )
        strategies[cand.strategy_id] = {
            "strategy_id": cand.strategy_id,
            "label": cand.label,
            "role": cand.role,
            "leverage": cand.leverage,
            "main": m_sum,
            "robust": r_sum,
            "high_rate_5pct": h_sum,
            "yearly": yearly,
            "leave_one_year_out": loyo,
            "bootstrap_vs_hold": boot,
        }

    verdict = evaluate_validation_verdict(strategies, cand_list)
    return {
        "task_id": TASK_ID,
        "requirement": REQUIREMENT,
        "provenance": dict(provenance),
        "window": {
            "name": "qqq_validation_2011_2021",
            "opening_close": start.isoformat(),
            "first_return_date": audit["first_return_date"],
            "last_return_date": audit["last_return_date"],
            "cutoff_enforced": VAL_END.isoformat(),
            "trading_days": audit["val_trading_days"],
        },
        "check": audit,
        "asset_context": summarize_assets(frame, start=start, end=end),
        "strategies": strategies,
        "verdict": verdict,
        "stopped": None,
        "production_effect": "none",
        "broker_action": "none",
    }


def _pct(x: float) -> str:
    return f"{x * 100:.2f}%"


def _pts(x: float) -> str:
    return f"{x * 100:+.2f}"


def render_markdown(result: Mapping[str, Any]) -> str:
    chk = result["check"]
    win = result["window"]
    st = result["strategies"]
    hold_geo = float(st["hold"]["main"]["geometric_annual_return"])
    hold_dd = float(st["hold"]["main"]["max_drawdown"])

    lines = [
        "# TRADING-2575：路径 2 多资产 Return Stacking 2011–2021 一次性验证报告",
        "",
        f"- 任务：`{result['task_id']}`；预注册需求：`{result['requirement']}` §6",
        f"- 验证窗口：`{win['opening_close']}` 收盘建仓，首日收益 `{win['first_return_date']}`，"
        f"终止 `{win['last_return_date']}`（`{win['trading_days']}` 个交易日；"
        f"硬截断 `{win['cutoff_enforced']}`，零 `2021-02-22+` 一致性窗口读取）",
        f"- 数据口径：100% 真实 2-B ETF（`QQQ`/`DTB3` 来自已检快照，`GLD`/`IEF` 来自 FMP "
        f"`dividend-adjusted`）；缺失格 `{chk['missing_val_cells']}`；252日预热 "
        f"`{'PASS' if chk['warmup_252d_ok'] else 'FAIL'}`；2010 开发重叠一致性 "
        f"`{'PASS' if chk['dev_overlap_2010']['ok'] else 'FAIL'}`",
        f"- **一次性验证判定：`{result['verdict']['verdict']}`** —— {result['verdict']['text']}",
        "",
        "## 1. 2011–2021 底层资产环境背景（100% 真实 ETF / DTB3）",
        "",
    ]

    asset_rows = []
    for ticker, stats in result["asset_context"].items():
        asset_rows.append(
            [
                f"`{ticker}`",
                _pct(stats["geometric_annual_return"]),
                _pct(stats["annual_volatility"]),
                _pct(stats["max_drawdown"]),
                f"{stats['excess_sharpe']:.2f}",
                f"{stats['corr_with_qqq']:.2f}",
            ]
        )
    lines += mad._md_table(
        ["资产", "几何年化", "年化波动", "最大回撤", "超额夏普", "与QQQ相关"], asset_rows
    )

    lines += ["## 2. 主测候选与对照策略全口径表现（固定预注册倍数，零参数搜索）", ""]
    perf_rows = []
    for sid, entry in st.items():
        role_tag = {"benchmark": "基准", "primary": "**主测**", "control": "对照(只印)"}[
            entry["role"]
        ]
        m = entry["main"]
        r = entry["robust"]
        h5 = entry["high_rate_5pct"]
        geo_m = float(m["geometric_annual_return"])
        dd_m = float(m["max_drawdown"])
        perf_rows.append(
            [
                f"`{sid}`",
                role_tag,
                f"{entry['leverage']:.2f}x",
                f"{_pct(geo_m)} ({_pts(geo_m - hold_geo)})",
                f"{_pct(dd_m)} ({_pts(dd_m - hold_dd)})",
                _pct(m["annual_volatility"]),
                f"{m['excess_sharpe']:.2f}",
                _pct(r["geometric_annual_return"]),
                _pct(h5["geometric_annual_return"]),
                f"{m['average_borrowed'] * 100:.1f}%",
                _pct(m["financing_drag_annual"]),
                f"{m['annual_turnover'] * 100:.1f}%",
            ]
        )
    lines += mad._md_table(
        [
            "策略",
            "角色",
            "倍数",
            "主算年化(对Hold)",
            "主算回撤(对Hold)",
            "年化波动",
            "超额夏普",
            "稳健年化",
            "5%高息年化",
            "平均借钱",
            "年化融资息",
            "年化换手",
        ],
        perf_rows,
    )

    lines += ["## 3. 预注册一次性通过/拒绝判据核对（仅主测生效）", ""]
    v_rows = []
    for item in result["verdict"]["details"]:
        c = item["checks"]
        d = item["deltas"]
        v_rows.append(
            [
                f"`{item['strategy_id']}`",
                f"{'PASS' if c['c1_dd_within_1pp_of_hold'] else 'FAIL'} "
                f"({_pts(d['main_dd_vs_hold'])})",
                f"{'PASS' if c['c2_main_ge_hold_and_robust_within_1_5pp'] else 'FAIL'} "
                f"(主{_pts(d['main_geo_vs_hold'])}/稳{_pts(d['robust_geo_vs_hold'])})",
                f"{'PASS' if c['c3_main_sharpe_beats_hold'] else 'FAIL'} "
                f"({d['main_sharpe_vs_hold']:+.2f})",
                _pts(d["high_rate_5pct_geo_vs_hold"]),
                "**PASS**" if item["pass_all"] else "**FAIL**",
            ]
        )
    lines += mad._md_table(
        [
            "主测候选",
            "1.回撤不深于Hold超1点",
            "2.主算≥Hold且稳健距≤1.5点",
            "3.主算夏普>Hold",
            "参考:5%高息对Hold",
            "单条判定",
        ],
        v_rows,
    )

    lines += ["## 4. 统计稳健性（2,000 次平稳块自助法 95% CI 与留一年最差超额）", ""]
    b_rows = []
    for sid, entry in st.items():
        boot = entry["bootstrap_vs_hold"]
        if boot is None:
            continue
        gd = boot["geo_diff"]
        dd = boot["max_dd_diff"]
        sd = boot["sharpe_diff"]
        loyo = entry["leave_one_year_out"]
        b_rows.append(
            [
                f"`{sid}`",
                f"[{_pts(gd['low'])}, {_pts(gd['high'])}] (P>0: {gd['prob_positive'] * 100:.1f}%)",
                f"[{_pts(dd['low'])}, {_pts(dd['high'])}]",
                f"[{sd['low']:+.2f}, {sd['high']:+.2f}]",
                f"{_pts(loyo['worst_geo_diff'])} (去{loyo['worst_removed_year']})",
            ]
        )
    lines += mad._md_table(
        [
            "策略",
            "年化差 95% CI (点)",
            "回撤差 95% CI (正=更浅)",
            "夏普差 95% CI",
            "留一年最差年化差",
        ],
        b_rows,
    )

    lines += ["## 5. 分年度收益拆解（主算，2011–2021.02）", ""]
    years = sorted(st["hold"]["yearly"].keys())
    yr_headers = [
        "年份",
        "`hold`",
        "`stack_static_1.5`",
        "`stack_trend_1.5`",
        "`qqq60_ief20_gold20_1.0`",
        "`f0001_1.0`",
    ]
    yr_rows = []
    for yr in years:
        yr_rows.append(
            [
                yr,
                _pct(st["hold"]["yearly"][yr]["strategy"]),
                f"{_pct(st['stack_static_1.5']['yearly'][yr]['strategy'])} "
                f"({_pts(st['stack_static_1.5']['yearly'][yr]['diff'])})",
                f"{_pct(st['stack_trend_1.5']['yearly'][yr]['strategy'])} "
                f"({_pts(st['stack_trend_1.5']['yearly'][yr]['diff'])})",
                _pct(st["qqq60_ief20_gold20_1.0"]["yearly"][yr]["strategy"]),
                _pct(st["f0001_1.0"]["yearly"][yr]["strategy"]),
            ]
        )
    lines += mad._md_table(yr_headers, yr_rows)

    if chk["dev_overlap_2010"]["checked"]:
        lines += ["## 6. 2010 年开发快照重叠核验（防 FMP 复权漂移）", ""]
        ov_rows = [
            [
                f"`{p['ticker']}`",
                f"{p.get('start', '—')}..{p.get('end', '—')}",
                str(p.get("overlap_days", 0)),
                f"{p['max_abs_daily_diff']:.6f} ({p['max_diff_date']})"
                if "max_abs_daily_diff" in p
                else p.get("reason", "FAIL"),
                "PASS" if p["ok"] else "FAIL",
            ]
            for p in chk["dev_overlap_2010"]["pairs"]
        ]
        lines += mad._md_table(["ETF", "重叠区间", "重叠日", "最大单日收益差", "状态"], ov_rows)

    lines += [
        "---",
        "披露：本次检验为 `2011-01-03..2021-02-19` 一次性预注册验证读数，未读取任何 "
        "`2021-02-22` 及之后一致性窗口数据；不构成实盘建议、纸盘准入或生产变更。",
        "`production_effect=none`；`broker_action=none`。",
        "",
    ]
    return "\n".join(lines)


def load_qqq_val_data(snapshot_root: Path) -> tuple[BenchData, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    _, plan = tr.load_trial(root_dir, DATA_TRIAL)
    data = from_snapshot(snapshot_root / plan.data.snapshot_id, qqq_series="actual")
    return gp.cut(data, cutoff=VAL_END), loaded, root_dir


@app.callback()
def main() -> None:
    """TRADING-2575：2011–2021 验证窗口一次性预注册检验。"""


@app.command("fetch")
def fetch_command(
    snapshot: Annotated[Path, typer.Option(help="2011–2021 验证本地快照目录（不进 git）。")],
) -> None:
    load_dotenv()
    api_key = os.environ.get(qlh.FMP_API_KEY_ENV, "").strip()
    if not api_key:
        typer.echo("val_2011: 缺少 FMP_API_KEY，无法拉取 GLD/IEF 真实 ETF 数据。")
        raise typer.Exit(code=1)
    fetch_validation_snapshot(
        snapshot,
        price_provider=FmpPriceProvider(api_key=api_key),
        end=VAL_END,
    )
    manifest = json.loads((snapshot / MANIFEST_JSON).read_text(encoding="utf-8"))
    typer.echo(
        f"val_2011: 快照已保存至 {snapshot}；总行数 {manifest['row_count']}，"
        f"截止 {manifest['fetch_end']}，SHA256 {manifest['series_csv_sha256'][:12]}"
    )


@app.command("check")
def check_command(
    snapshot: Annotated[Path, typer.Option(help="2011–2021 验证快照目录。")],
    out: Annotated[Path, typer.Option(help="检查输出目录。")],
    dev_snapshot: Annotated[
        Path | None, typer.Option(help="可选：开发窗口快照目录，用于核验 2010 重叠零漂移。")
    ] = None,
    qqq_root: Annotated[
        Path, typer.Option(help="QQQ 长历史快照根目录。")
    ] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    qqq_data, _, _ = load_qqq_val_data(qqq_root)
    raw_series, _ = load_validation_snapshot(snapshot)
    dev_raw = mad.load_multi_asset_snapshot(dev_snapshot)[0] if dev_snapshot is not None else None
    _, audit = build_validation_frame(qqq_data, raw_series, dev_raw_series=dev_raw)
    out.mkdir(parents=True, exist_ok=True)
    out.joinpath(CHECK_JSON).write_text(
        json.dumps(gp._clean(dict(audit)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    typer.echo(
        f"val_2011 check: {'PASS' if audit['ok'] else 'FAIL'} | "
        f"days={audit['val_trading_days']} missing={audit['missing_val_cells']} "
        f"warmup={audit['warmup_252d_ok']} dev2010={audit['dev_overlap_2010']['ok']} "
        f"max_date={audit['max_loaded_date']}"
    )
    if not audit["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    snapshot: Annotated[Path, typer.Option(help="2011–2021 验证快照目录。")],
    out: Annotated[Path, typer.Option(help="结果输出目录（一次性，拒绝覆盖）。")],
    dev_snapshot: Annotated[
        Path | None, typer.Option(help="可选：开发窗口快照目录，用于核验 2010 重叠零漂移。")
    ] = None,
    qqq_root: Annotated[
        Path, typer.Option(help="QQQ 长历史快照根目录。")
    ] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("val_2011: 已有一次性验证结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    qqq_data, _, root_dir = load_qqq_val_data(qqq_root)
    raw_series, _ = load_validation_snapshot(snapshot)
    dev_raw = mad.load_multi_asset_snapshot(dev_snapshot)[0] if dev_snapshot is not None else None
    result = run_validation(
        qqq_data,
        raw_series,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        dev_raw_series=dev_raw,
    )
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
    typer.echo(f"val_2011: 判定 {result['verdict']['verdict']}；结果目录 {out}")


if __name__ == "__main__":
    app()
