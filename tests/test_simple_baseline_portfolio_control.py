from __future__ import annotations

import csv
import json
import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import render_reader_brief_html


def test_reader_brief_renders_portfolio_control_research_summary(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    artifact_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_portfolio_control_safety_summary.json"
    )
    _write_json(
        artifact_path,
        {
            "report_type": "daily_reader_portfolio_control_safety_summary",
            "status": "DAILY_SUMMARY_SAFE",
            "production_effect": "none",
            "broker_action": "none",
            "portfolio_control_research_status": {
                "top_simple_baseline_candidate": "qqq_80_sgov_20",
                "current_research_only_target_weights": {"QQQ": 0.8, "SGOV": 0.2},
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "major_blockers": [{"reason": "manual owner review still required"}],
            },
        },
    )
    forward_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_forward_aging_summary.json"
    )
    _write_json(
        forward_path,
        {
            "report_type": "daily_reader_forward_aging_summary",
            "status": "DAILY_FORWARD_SUMMARY_SAFE",
            "production_effect": "none",
            "broker_action": "none",
            "portfolio_control_forward_aging": {
                "primary_candidate": "equal_risk_qqq_sgov",
                "challenger_candidate": "dyn_tqqq_capped_trend",
                "latest_observation_date": "2023-10-10",
                "matured_20d_count": 3,
                "matured_60d_count": 2,
                "matured_120d_count": 1,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
        },
    )
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)

    summary = reader_brief._portfolio_control_research_summary()
    forward_summary = reader_brief._portfolio_control_forward_aging_summary()
    html = render_reader_brief_html(
        {
            "portfolio_control_research": summary,
            "portfolio_control_forward_aging": forward_summary,
        }
    )

    assert summary["status"] == "DAILY_SUMMARY_SAFE"
    assert summary["top_simple_baseline_candidate"] == "qqq_80_sgov_20"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["major_blocker_count"] == 1
    assert "Portfolio Control Research" in html
    assert "qqq_80_sgov_20" in html
    assert forward_summary["primary_candidate"] == "equal_risk_qqq_sgov"
    assert forward_summary["matured_120d_count"] == 1
    assert "Portfolio Control Forward Aging" in html
    assert "dyn_tqqq_capped_trend" in html
    assert "broker_action" in html


def _write_simple_baseline_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    dates = _business_dates(date(2021, 2, 22), 883)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0008 + 0.002 * math.sin(day_index / 17.0)
        if 145 <= day_index <= 165:
            qqq_return -= 0.004
        if 260 <= day_index <= 275:
            qqq_return -= 0.003
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.0002
        levels["SGOV"] *= 1.0 + 0.00015
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            adj_close = close + (0.01 if ticker == "SGOV" else 0.0)
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},{close * 1.002:.4f},"
                f"{close * 0.998:.4f},{close:.4f},{adj_close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0005:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path


def _write_simple_baseline_manifest(
    tmp_path: Path,
    prices_path: Path,
    *,
    symbols: tuple[str, ...] = ("QQQ", "TQQQ", "SGOV"),
) -> Path:
    manifest_path = tmp_path / "download_manifest.csv"
    dates = _cache_dates(prices_path)
    row_counts = _price_row_counts(prices_path)
    with manifest_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "downloaded_at",
                "source_id",
                "provider",
                "endpoint",
                "request_parameters",
                "output_path",
                "row_count",
                "checksum_sha256",
            ],
        )
        writer.writeheader()
        for symbol in symbols:
            params = {
                "symbols": [symbol],
                "start": min(dates).isoformat(),
                "end": max(dates).isoformat(),
                "interval": "1d",
                "repair_mode": "price_only" if symbol == "TQQQ" else "download",
                "symbol_mapping": {
                    symbol: {"source_symbol": symbol, "canonical_symbol": symbol}
                },
            }
            writer.writerow(
                {
                    "downloaded_at": "2024-07-10T00:00:00Z",
                    "source_id": "fmp_eod_daily_prices",
                    "provider": "Financial Modeling Prep",
                    "endpoint": "/stable/historical-price-eod/full",
                    "request_parameters": json.dumps(params, sort_keys=True),
                    "output_path": str(prices_path),
                    "row_count": row_counts.get(symbol, 0),
                    "checksum_sha256": f"fixture-{symbol.lower()}",
                }
            )
    return manifest_path


def _write_symbol_filtered_cache(
    source_path: Path,
    target_path: Path,
    *,
    included_symbol: str | None = None,
    excluded_symbol: str | None = None,
) -> Path:
    lines = source_path.read_text(encoding="utf-8").splitlines(keepends=True)
    filtered = [lines[0]]
    for line in lines[1:]:
        symbol = line.split(",", maxsplit=3)[1]
        if included_symbol and symbol != included_symbol:
            continue
        if excluded_symbol and symbol == excluded_symbol:
            continue
        filtered.append(line)
    target_path.write_text("".join(filtered), encoding="utf-8")
    return target_path


def _cache_dates(path: Path) -> list[date]:
    dates: set[date] = set()
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        dates.add(date.fromisoformat(line.split(",", maxsplit=1)[0]))
    return sorted(dates)


def _price_row_counts(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        symbol = line.split(",", maxsplit=3)[1]
        counts[symbol] = counts.get(symbol, 0) + 1
    return counts


def _business_dates(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_minimal_real_run_support(output_root: Path) -> None:
    _write_json(
        output_root / "simple_baseline_real_run_summary.json",
        {
            "report_type": "simple_baseline_real_run_summary",
            "status": "REAL_RUN_COMPLETED",
            "summary": {"top_recommended_candidate": "equal_risk_qqq_sgov"},
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
    _write_json(
        output_root / "simple_baseline_owner_decision_pack.json",
        {
            "report_type": "simple_baseline_owner_decision_pack",
            "status": "OWNER_DECISION_REQUIRED",
            "summary": {"owner_next_action": "narrow_to_watchlist_without_activation"},
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )
    _write_json(
        output_root / "simple_baseline_paper_shadow_watchlist.json",
        {
            "report_type": "simple_baseline_paper_shadow_watchlist",
            "status": "WATCHLIST_CREATED_NO_ACTIVATION",
            "summary": {
                "watchlist_count": 5,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
            "watchlist": [
                {
                    "candidate_strategy_id": strategy_id,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
                for strategy_id in (
                    "equal_risk_qqq_sgov",
                    "dyn_tqqq_capped_trend",
                    "qqq_200dma_risk_off",
                    "dyn_balanced_qqq_tqqq_sgov",
                    "qqq_50_sgov_50",
                )
            ],
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_required": True,
        },
    )


