from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median

import pandas as pd

EPSILON = 1e-12


def build_allocation_stability_diagnostics(
    daily: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    max_daily_turnover: float | None = None,
    max_rebalance_trade_weight: float | None = None,
) -> dict[str, object]:
    daily_frame = _with_signal_date(daily)
    weights_frame = _with_signal_date(weights)
    signal_dates = _signal_dates(daily_frame, weights_frame)
    if not signal_dates:
        return {
            "schema_version": 1,
            "diagnostic_type": "allocation_stability",
            "status": "NO_DATA",
            "reason_codes": ["NO_BACKTEST_ROWS"],
            "signal_day_count": 0,
        }

    daily_turnover = _daily_turnover_rows(daily_frame)
    turnover_values = [float(row["turnover"]) for row in daily_turnover]
    policy_turnovers = turnover_values[1:] if len(turnover_values) > 1 else []
    absolute_deltas = _absolute_weight_deltas(weights_frame)
    first_signal_date = signal_dates[0]
    post_initial_deltas = _post_initial_weight_deltas(weights_frame, first_signal_date)
    exposure = _exposure_summary(weights_frame, signal_dates)
    constraint_summary = _constraint_summary(weights_frame, signal_dates)
    rebalance_count = sum(1 for value in turnover_values if value > EPSILON)
    max_rebalance_turnover = max(policy_turnovers) if policy_turnovers else 0.0
    max_post_initial_delta = max(post_initial_deltas) if post_initial_deltas else 0.0
    status, reason_codes = _stability_status(
        max_rebalance_turnover=max_rebalance_turnover,
        max_post_initial_delta=max_post_initial_delta,
        max_daily_turnover=max_daily_turnover,
        max_rebalance_trade_weight=max_rebalance_trade_weight,
    )

    return {
        "schema_version": 1,
        "diagnostic_type": "allocation_stability",
        "status": status,
        "reason_codes": reason_codes,
        "signal_day_count": len(signal_dates),
        "first_signal_date": signal_dates[0],
        "last_signal_date": signal_dates[-1],
        "policy": {
            "max_daily_turnover": _optional_round(max_daily_turnover),
            "max_rebalance_trade_weight": _optional_round(max_rebalance_trade_weight),
            "initial_deployment_excluded_from_policy_check": True,
        },
        "daily_turnover": daily_turnover,
        "daily_turnover_average": _average(turnover_values),
        "daily_turnover_max": _maximum(turnover_values),
        "max_rebalance_turnover": _round(max_rebalance_turnover),
        "average_absolute_weight_delta": _average(absolute_deltas),
        "median_absolute_weight_delta": _median(absolute_deltas),
        "max_single_day_weight_delta": _maximum(absolute_deltas),
        "max_single_day_weight_delta_after_initial": _round(max_post_initial_delta),
        "rebalance_count": rebalance_count,
        "rebalance_frequency": _round(rebalance_count / len(signal_dates)),
        "regime_transition_count": _regime_transition_count(daily_frame),
        "constraint_hit_count": constraint_summary["constraint_hit_count"],
        "constraint_hit_rate": constraint_summary["constraint_hit_rate"],
        "constraint_hit_by_id": constraint_summary["constraint_hit_by_id"],
        "cash_weight_min": exposure["cash_weight_min"],
        "cash_weight_max": exposure["cash_weight_max"],
        "cash_weight_average": exposure["cash_weight_average"],
        "equity_exposure_average": exposure["equity_exposure_average"],
        "semiconductor_exposure_average": exposure["semiconductor_exposure_average"],
        "asset_exposure_time": exposure["asset_exposure_time"],
        "average_holding_period": exposure["average_holding_period"],
        "exposure_by_asset_average": exposure["exposure_by_asset_average"],
    }


def render_allocation_stability_markdown(payload: dict[str, object]) -> str:
    lines = [
        "# ETF Allocation Stability Diagnostics",
        "",
        f"- Status：{payload.get('status', 'UNKNOWN')}",
        f"- Signal Days：{payload.get('signal_day_count', 0)}",
        f"- First Signal Date：{payload.get('first_signal_date', 'n/a')}",
        f"- Last Signal Date：{payload.get('last_signal_date', 'n/a')}",
        f"- Reason Codes：{', '.join(str(item) for item in payload.get('reason_codes', []))}",
        "",
        "## Turnover And Deltas",
        "",
        f"- Average Daily Turnover：{_fmt_pct(payload.get('daily_turnover_average'))}",
        f"- Max Daily Turnover：{_fmt_pct(payload.get('daily_turnover_max'))}",
        f"- Max Rebalance Turnover：{_fmt_pct(payload.get('max_rebalance_turnover'))}",
        (
            "- Average Absolute Weight Delta："
            f"{_fmt_pct(payload.get('average_absolute_weight_delta'))}"
        ),
        (
            "- Median Absolute Weight Delta："
            f"{_fmt_pct(payload.get('median_absolute_weight_delta'))}"
        ),
        (
            "- Max Single-Day Weight Delta："
            f"{_fmt_pct(payload.get('max_single_day_weight_delta'))}"
        ),
        f"- Rebalance Count：{payload.get('rebalance_count', 0)}",
        f"- Rebalance Frequency：{_fmt_pct(payload.get('rebalance_frequency'))}",
        f"- Regime Transition Count：{payload.get('regime_transition_count', 0)}",
        "",
        "## Constraints And Exposure",
        "",
        f"- Constraint Hit Rate：{_fmt_pct(payload.get('constraint_hit_rate'))}",
        f"- Cash Weight Min：{_fmt_pct(payload.get('cash_weight_min'))}",
        f"- Cash Weight Max：{_fmt_pct(payload.get('cash_weight_max'))}",
        f"- Cash Weight Average：{_fmt_pct(payload.get('cash_weight_average'))}",
        f"- Equity Exposure Average：{_fmt_pct(payload.get('equity_exposure_average'))}",
        (
            "- Semiconductor Exposure Average："
            f"{_fmt_pct(payload.get('semiconductor_exposure_average'))}"
        ),
        "",
        "## Asset Exposure Time",
        "",
        "| Asset | Exposure Time | Average Holding Period (days) |",
        "|---|---:|---:|",
    ]
    exposure_time = payload.get("asset_exposure_time", {})
    holding = payload.get("average_holding_period", {})
    by_asset = holding.get("by_asset_days", {}) if isinstance(holding, dict) else {}
    if isinstance(exposure_time, dict):
        for symbol, value in sorted(exposure_time.items()):
            lines.append(f"| {symbol} | {_fmt_pct(value)} | {_fmt_number(by_asset.get(symbol))} |")
    return "\n".join(lines) + "\n"


def write_allocation_stability_diagnostics(
    payload: dict[str, object],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_path.write_text(render_allocation_stability_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def _with_signal_date(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "signal_date" not in frame.columns:
        return frame.copy()
    result = frame.copy()
    result["_signal_date"] = pd.to_datetime(result["signal_date"], errors="coerce").dt.date
    result = result.dropna(subset=["_signal_date"])
    return result.sort_values("_signal_date")


def _signal_dates(daily: pd.DataFrame, weights: pd.DataFrame) -> list[str]:
    date_values = []
    if "_signal_date" in daily:
        date_values.extend(daily["_signal_date"].tolist())
    if "_signal_date" in weights:
        date_values.extend(weights["_signal_date"].tolist())
    return sorted({item.isoformat() for item in date_values if pd.notna(item)})


def _daily_turnover_rows(daily: pd.DataFrame) -> list[dict[str, object]]:
    if daily.empty or "_signal_date" not in daily or "turnover" not in daily:
        return []
    rows: list[dict[str, object]] = []
    for _, row in daily.sort_values("_signal_date").iterrows():
        rows.append(
            {
                "signal_date": row["_signal_date"].isoformat(),
                "turnover": _round(_float(row.get("turnover"))),
            }
        )
    return rows


def _absolute_weight_deltas(weights: pd.DataFrame) -> list[float]:
    if weights.empty or "trade_delta" not in weights:
        return []
    return [
        abs(_float(value))
        for value in pd.to_numeric(weights["trade_delta"], errors="coerce").dropna()
    ]


def _post_initial_weight_deltas(weights: pd.DataFrame, first_signal_date: str) -> list[float]:
    if weights.empty or "_signal_date" not in weights:
        return []
    filtered = weights.loc[weights["_signal_date"].astype(str) != first_signal_date]
    return _absolute_weight_deltas(filtered)


def _regime_transition_count(daily: pd.DataFrame) -> int:
    if daily.empty or "regime" not in daily:
        return 0
    regimes = [str(item) for item in daily.sort_values("_signal_date")["regime"].tolist()]
    return sum(
        1 for previous, current in zip(regimes, regimes[1:], strict=False) if current != previous
    )


def _constraint_summary(
    weights: pd.DataFrame,
    signal_dates: list[str],
) -> dict[str, object]:
    constraints_by_date: dict[str, set[str]] = defaultdict(set)
    if not weights.empty and "_signal_date" in weights and "constraints_applied" in weights:
        for _, row in weights.iterrows():
            signal_date = row["_signal_date"].isoformat()
            constraints_by_date[signal_date].update(_json_list(row.get("constraints_applied")))
    hit_dates = {date_key for date_key, items in constraints_by_date.items() if items}
    hit_by_id: Counter[str] = Counter()
    for items in constraints_by_date.values():
        for constraint_id in items:
            hit_by_id[constraint_id] += 1
    signal_day_count = len(signal_dates)
    return {
        "constraint_hit_count": len(hit_dates),
        "constraint_hit_rate": (
            _round(len(hit_dates) / signal_day_count) if signal_day_count else 0.0
        ),
        "constraint_hit_by_id": dict(sorted(hit_by_id.items())),
    }


def _exposure_summary(weights: pd.DataFrame, signal_dates: list[str]) -> dict[str, object]:
    if weights.empty or "_signal_date" not in weights:
        return {
            "cash_weight_min": None,
            "cash_weight_max": None,
            "cash_weight_average": None,
            "equity_exposure_average": None,
            "semiconductor_exposure_average": None,
            "asset_exposure_time": {},
            "average_holding_period": {"portfolio_average_days": None, "by_asset_days": {}},
            "exposure_by_asset_average": {},
        }
    pivot = (
        weights.pivot_table(
            index="_signal_date",
            columns="symbol",
            values="target_weight",
            aggfunc="last",
        )
        .reindex(pd.to_datetime(signal_dates).date)
        .fillna(0.0)
        .sort_index()
    )
    cash = pd.to_numeric(pivot.get("CASH", pd.Series(0.0, index=pivot.index)), errors="coerce")
    semiconductor = pd.Series(0.0, index=pivot.index)
    for symbol in ("SMH", "SOXX"):
        if symbol in pivot:
            semiconductor = semiconductor + pd.to_numeric(pivot[symbol], errors="coerce").fillna(
                0.0
            )
    exposure_time = {
        str(symbol): _round(float((pd.to_numeric(pivot[symbol], errors="coerce") > EPSILON).mean()))
        for symbol in pivot.columns
    }
    average_by_asset = {
        str(symbol): _round(float(pd.to_numeric(pivot[symbol], errors="coerce").mean()))
        for symbol in pivot.columns
    }
    holding_period = _average_holding_period(pivot)
    return {
        "cash_weight_min": _round(float(cash.min())),
        "cash_weight_max": _round(float(cash.max())),
        "cash_weight_average": _round(float(cash.mean())),
        "equity_exposure_average": _round(float((1.0 - cash).mean())),
        "semiconductor_exposure_average": _round(float(semiconductor.mean())),
        "asset_exposure_time": dict(sorted(exposure_time.items())),
        "average_holding_period": holding_period,
        "exposure_by_asset_average": dict(sorted(average_by_asset.items())),
    }


def _average_holding_period(pivot: pd.DataFrame) -> dict[str, object]:
    by_asset: dict[str, float] = {}
    all_periods: list[int] = []
    for symbol in pivot.columns:
        periods = _positive_runs(pd.to_numeric(pivot[symbol], errors="coerce").fillna(0.0))
        by_asset[str(symbol)] = _average(periods)
        all_periods.extend(periods)
    return {
        "portfolio_average_days": _average(all_periods),
        "by_asset_days": dict(sorted(by_asset.items())),
        "method": "consecutive_signal_days_with_positive_target_weight",
    }


def _positive_runs(series: pd.Series) -> list[int]:
    periods: list[int] = []
    current = 0
    for value in series:
        if float(value) > EPSILON:
            current += 1
            continue
        if current:
            periods.append(current)
            current = 0
    if current:
        periods.append(current)
    return periods


def _stability_status(
    *,
    max_rebalance_turnover: float,
    max_post_initial_delta: float,
    max_daily_turnover: float | None,
    max_rebalance_trade_weight: float | None,
) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    if max_daily_turnover is not None and max_rebalance_turnover > max_daily_turnover + 1e-8:
        reason_codes.append("MAX_REBALANCE_TURNOVER_ABOVE_POLICY")
    if (
        max_rebalance_trade_weight is not None
        and max_post_initial_delta > max_rebalance_trade_weight + 1e-8
    ):
        reason_codes.append("MAX_SINGLE_DAY_WEIGHT_DELTA_ABOVE_POLICY")
    if reason_codes:
        return "TOO_JUMPY", reason_codes
    return "STABLE", ["WITHIN_CONFIGURED_REBALANCE_LIMITS"]


def _json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return [str(value)]
    if not isinstance(parsed, list):
        return [str(parsed)]
    return [str(item) for item in parsed]


def _float(value: object) -> float:
    parsed = pd.to_numeric(value, errors="coerce")
    if pd.isna(parsed):
        return 0.0
    return float(parsed)


def _average(values: list[float] | list[int]) -> float:
    return _round(mean(values)) if values else 0.0


def _median(values: list[float]) -> float:
    return _round(median(values)) if values else 0.0


def _maximum(values: list[float]) -> float:
    return _round(max(values)) if values else 0.0


def _optional_round(value: float | None) -> float | None:
    return None if value is None else _round(value)


def _round(value: float) -> float:
    return round(float(value), 10)


def _fmt_pct(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.2%}"


def _fmt_number(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{_float(value):.2f}"
