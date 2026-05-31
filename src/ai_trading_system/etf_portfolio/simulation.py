from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.allocation import allocation_to_frame
from ai_trading_system.etf_portfolio.models import ETFAllocationRecord
from ai_trading_system.etf_portfolio.no_lookahead import (
    evaluation_only_columns,
    raise_for_no_lookahead_violations,
    validate_no_lookahead_records,
)

FORWARD_WINDOWS = (1, 5, 20, 60)


def record_simulation_snapshot(
    *,
    allocation_records: list[ETFAllocationRecord],
    ledger_path: Path,
    report_path: Path | None = None,
) -> Path:
    new_rows = allocation_to_frame(allocation_records)
    new_rows["evaluation_only"] = False
    if report_path is not None:
        new_rows["report_path"] = str(report_path)
    if ledger_path.exists():
        existing = pd.read_csv(ledger_path)
        if "evaluation_only" not in existing.columns:
            existing["evaluation_only"] = _evaluation_value_mask(existing)
        key_cols = ["date", "model_version", "symbol"]
        existing_keys = {
            tuple(row[column] for column in key_cols) for _, row in new_rows.iterrows()
        }
        existing = existing.loc[
            ~existing[key_cols].apply(lambda row: tuple(row), axis=1).isin(existing_keys)
        ]
        output = pd.concat([existing, new_rows], ignore_index=True)
    else:
        output = new_rows
    output = output.sort_values(["date", "model_version", "symbol"]).reset_index(drop=True)
    raise_for_no_lookahead_violations(
        validate_no_lookahead_records(simulation_records=output)
    )
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(ledger_path, index=False)
    return ledger_path


def evaluate_simulation_ledger(
    *,
    ledger_path: Path,
    prices: pd.DataFrame,
    as_of: date,
) -> Path:
    if not ledger_path.exists():
        raise FileNotFoundError(f"ETF simulation ledger does not exist: {ledger_path}")
    ledger = pd.read_csv(ledger_path)
    if ledger.empty:
        ledger.to_csv(ledger_path, index=False)
        return ledger_path
    close_pivot = _price_pivot(prices)
    evaluated = ledger.copy()
    for window in FORWARD_WINDOWS:
        evaluated[f"forward_return_{window}d"] = evaluated.apply(
            lambda row, period=window: _forward_return(close_pivot, row, as_of, period),
            axis=1,
        )
    for window in (20, 60):
        evaluated[f"max_drawdown_next_{window}d"] = evaluated.apply(
            lambda row, period=window: _forward_drawdown(close_pivot, row, as_of, period),
            axis=1,
        )
    evaluated["relative_return_vs_spy_20d"] = evaluated.apply(
        lambda row: _relative_return(close_pivot, row, as_of, 20, "SPY"),
        axis=1,
    )
    evaluated["relative_return_vs_qqq_20d"] = evaluated.apply(
        lambda row: _relative_return(close_pivot, row, as_of, 20, "QQQ"),
        axis=1,
    )
    evaluated["weight_contribution_20d"] = evaluated.apply(_weight_contribution_20d, axis=1)
    evaluated = _add_portfolio_benchmark_columns(evaluated)
    evaluated["signal_hit_20d"] = evaluated.apply(_signal_hit_20d, axis=1)
    evaluated["evaluation_only"] = True
    raise_for_no_lookahead_violations(
        validate_no_lookahead_records(simulation_records=evaluated)
    )
    evaluated.to_csv(ledger_path, index=False)
    return ledger_path


def render_simulation_report(ledger_path: Path, window: str = "60d") -> str:
    if not ledger_path.exists():
        raise FileNotFoundError(f"ETF simulation ledger does not exist: {ledger_path}")
    ledger = pd.read_csv(ledger_path)
    lines = [
        f"# ETF Simulation Report - {window}",
        "",
        f"- Ledger：`{ledger_path}`",
        f"- Records：{len(ledger)}",
        "",
        "## Model Versions",
        "",
    ]
    if ledger.empty:
        lines.append("暂无模拟舱记录。")
    else:
        for version, group in ledger.groupby("model_version", sort=True):
            hit_rate = _hit_rate(group)
            lines.append(
                f"- {version}：records={len(group)}；"
                f"20d hit rate={hit_rate if hit_rate is not None else 'n/a'}"
            )
    lines.extend(["", "## Portfolio vs Benchmarks", ""])
    if ledger.empty:
        lines.append("暂无 portfolio-vs-benchmark 样本。")
    else:
        for version, group in ledger.groupby("model_version", sort=True):
            summary = _portfolio_vs_benchmark_summary(group)
            lines.append(f"- {version}：{summary}")
    lines.extend(["", "## Notes", "", "- forward return 不足窗口时保持 null，不填 0。"])
    return "\n".join(lines) + "\n"


def summarize_simulation_for_brief(
    ledger_path: Path,
    *,
    as_of: date | None = None,
) -> str:
    if not ledger_path.exists():
        return f"模拟舱暂无记录：未找到 ledger `{ledger_path}`。"
    ledger = pd.read_csv(ledger_path)
    if ledger.empty:
        return f"模拟舱暂无记录：ledger `{ledger_path}` 为空。"
    selected = ledger.copy()
    if as_of is not None and "date" in selected.columns:
        parsed_dates = pd.to_datetime(selected["date"], errors="coerce")
        selected = selected.loc[parsed_dates <= pd.Timestamp(as_of)].copy()
    if selected.empty:
        return f"模拟舱暂无截至 {as_of.isoformat() if as_of else 'n/a'} 的记录。"

    lines = [
        f"- Ledger: `{ledger_path}`",
        f"- Records: {len(selected)}",
    ]
    for version, group in selected.groupby("model_version", sort=True):
        latest_date = pd.to_datetime(group["date"], errors="coerce").max()
        latest_label = (
            latest_date.date().isoformat() if pd.notna(latest_date) else "n/a"
        )
        hit_rate = _hit_rate(group)
        lines.append(
            f"- {version}：records={len(group)}；latest_snapshot={latest_label}；"
            f"20d hit rate={hit_rate if hit_rate is not None else 'n/a'}；"
            f"{_portfolio_vs_benchmark_summary(group)}"
        )
    return "\n".join(lines)


def write_simulation_report(ledger_path: Path, output_path: Path, window: str = "60d") -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_simulation_report(ledger_path, window=window), encoding="utf-8")
    return output_path


def _price_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    return frame.pivot(index="_date", columns="symbol", values="_price").sort_index()


def _evaluation_value_mask(frame: pd.DataFrame) -> list[bool]:
    columns = evaluation_only_columns(frame.columns)
    if not columns:
        return [False] * len(frame)
    return frame[list(columns)].notna().any(axis=1).astype(bool).tolist()


def _forward_return(
    close_pivot: pd.DataFrame,
    row: pd.Series,
    as_of: date,
    window: int,
) -> float | None:
    symbol = str(row["symbol"])
    signal_date = pd.Timestamp(str(row["date"]))
    future_dates = [item for item in close_pivot.index if signal_date < item <= pd.Timestamp(as_of)]
    if symbol == "CASH":
        return 0.0 if len(future_dates) >= window else None
    if len(future_dates) < window or symbol not in close_pivot.columns:
        return None
    end_date = future_dates[window - 1]
    start_value = close_pivot.loc[signal_date, symbol] if signal_date in close_pivot.index else None
    end_value = close_pivot.loc[end_date, symbol]
    if pd.isna(start_value) or pd.isna(end_value) or float(start_value) == 0:
        return None
    return float(end_value) / float(start_value) - 1.0


def _forward_drawdown(
    close_pivot: pd.DataFrame,
    row: pd.Series,
    as_of: date,
    window: int,
) -> float | None:
    symbol = str(row["symbol"])
    signal_date = pd.Timestamp(str(row["date"]))
    future_dates = [item for item in close_pivot.index if signal_date < item <= pd.Timestamp(as_of)]
    if symbol == "CASH":
        return 0.0 if len(future_dates) >= window else None
    if len(future_dates) < window or symbol not in close_pivot.columns:
        return None
    window_dates = future_dates[:window]
    series = close_pivot.loc[window_dates, symbol].dropna()
    if series.empty:
        return None
    running_max = series.cummax()
    return float((series / running_max - 1.0).min())


def _relative_return(
    close_pivot: pd.DataFrame,
    row: pd.Series,
    as_of: date,
    window: int,
    benchmark_symbol: str,
) -> float | None:
    asset_return = _forward_return(close_pivot, row, as_of, window)
    if asset_return is None:
        return None
    benchmark_row = row.copy()
    benchmark_row["symbol"] = benchmark_symbol
    benchmark_return = _forward_return(close_pivot, benchmark_row, as_of, window)
    if benchmark_return is None:
        return None
    return asset_return - benchmark_return


def _weight_contribution_20d(row: pd.Series) -> float | None:
    value = row.get("forward_return_20d")
    weight = row.get("target_weight")
    if pd.isna(value) or pd.isna(weight):
        return None
    return float(weight) * float(value)


def _add_portfolio_benchmark_columns(ledger: pd.DataFrame) -> pd.DataFrame:
    output = ledger.copy()
    for column in (
        "portfolio_return_20d",
        "portfolio_relative_return_vs_spy_20d",
        "portfolio_relative_return_vs_qqq_20d",
    ):
        output[column] = None
    group_cols = ["date", "model_version"]
    for _, group in output.groupby(group_cols, sort=False):
        contribution = pd.to_numeric(group["weight_contribution_20d"], errors="coerce")
        if contribution.isna().any():
            continue
        portfolio_return = float(contribution.sum())
        spy_return = _benchmark_return_from_group(group, "SPY")
        qqq_return = _benchmark_return_from_group(group, "QQQ")
        index = group.index
        output.loc[index, "portfolio_return_20d"] = portfolio_return
        if spy_return is not None:
            output.loc[index, "portfolio_relative_return_vs_spy_20d"] = (
                portfolio_return - spy_return
            )
        if qqq_return is not None:
            output.loc[index, "portfolio_relative_return_vs_qqq_20d"] = (
                portfolio_return - qqq_return
            )
    return output


def _benchmark_return_from_group(group: pd.DataFrame, symbol: str) -> float | None:
    rows = group.loc[group["symbol"] == symbol, "forward_return_20d"]
    if rows.empty:
        return None
    value = rows.iloc[0]
    if pd.isna(value):
        return None
    return float(value)


def _signal_hit_20d(row: pd.Series) -> bool | None:
    value = row.get("forward_return_20d")
    if pd.isna(value):
        return None
    score = row.get("composite_score")
    if pd.isna(score):
        return None
    direction = "positive" if float(score) >= 50 else "negative"
    return (float(value) >= 0) if direction == "positive" else (float(value) < 0)


def _hit_rate(group: pd.DataFrame) -> str | None:
    if "signal_hit_20d" not in group:
        return None
    values = []
    for value in group["signal_hit_20d"]:
        if isinstance(value, bool):
            values.append(value)
        elif str(value).lower() in {"true", "false"}:
            values.append(str(value).lower() == "true")
    if not values:
        return None
    return f"{sum(values) / len(values):.1%}"


def _portfolio_vs_benchmark_summary(group: pd.DataFrame) -> str:
    summary_rows = group.drop_duplicates(subset=["date", "model_version"]).copy()
    portfolio_return = _mean_percent(summary_rows.get("portfolio_return_20d"))
    spy_relative = _mean_percent(summary_rows.get("portfolio_relative_return_vs_spy_20d"))
    qqq_relative = _mean_percent(summary_rows.get("portfolio_relative_return_vs_qqq_20d"))
    return (
        f"avg 20d portfolio return={portfolio_return}; "
        f"avg relative vs SPY={spy_relative}; "
        f"avg relative vs QQQ={qqq_relative}"
    )


def _mean_percent(values: pd.Series | None) -> str:
    if values is None:
        return "n/a"
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return "n/a"
    return f"{float(numeric.mean()):.1%}"
