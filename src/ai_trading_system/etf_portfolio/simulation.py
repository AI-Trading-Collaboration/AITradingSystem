from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
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
SIMULATION_LEDGER_SCHEMA_VERSION = 2
DECISION_RECORD_TYPE = "decision"
EVALUATION_RECORD_TYPE = "evaluation"
FORWARD_RETURN_COLUMNS = tuple(f"forward_return_{window}d" for window in FORWARD_WINDOWS)
FUTURE_RETURN_COLUMNS = tuple(f"future_{window}d_return" for window in FORWARD_WINDOWS)
EVALUATION_COLUMNS = (
    *FORWARD_RETURN_COLUMNS,
    *FUTURE_RETURN_COLUMNS,
    "max_drawdown_next_20d",
    "max_drawdown_next_60d",
    "relative_return_vs_spy_20d",
    "relative_return_vs_qqq_20d",
    "weight_contribution_20d",
    "portfolio_return_20d",
    "portfolio_relative_return_vs_spy_20d",
    "portfolio_relative_return_vs_qqq_20d",
    "signal_hit_20d",
    "evaluation_as_of_date",
)


def record_simulation_snapshot(
    *,
    allocation_records: list[ETFAllocationRecord],
    ledger_path: Path,
    report_path: Path | None = None,
) -> Path:
    new_rows = allocation_to_frame(allocation_records)
    new_rows = _enrich_decision_rows(new_rows, report_path=report_path)
    if report_path is not None:
        new_rows["report_path"] = str(report_path)
    if ledger_path.exists():
        existing = _normalize_ledger_schema(pd.read_csv(ledger_path))
        key_cols = ["date", "model_version", "symbol"]
        existing_keys = {
            tuple(row[column] for column in key_cols) for _, row in new_rows.iterrows()
        }
        existing = existing.loc[
            ~(
                (existing["record_type"] == DECISION_RECORD_TYPE)
                & existing[key_cols].apply(lambda row: tuple(row), axis=1).isin(existing_keys)
            )
        ]
        output = pd.concat([existing, new_rows], ignore_index=True)
    else:
        output = new_rows
    output = _sort_ledger(output)
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
    ledger = _normalize_ledger_schema(ledger)
    decisions = _decision_rows(ledger)
    if decisions.empty:
        ledger.to_csv(ledger_path, index=False)
        return ledger_path
    evaluated = decisions.copy()
    for window in FORWARD_WINDOWS:
        evaluated[f"forward_return_{window}d"] = evaluated.apply(
            lambda row, period=window: _forward_return(close_pivot, row, as_of, period),
            axis=1,
        )
        evaluated[f"future_{window}d_return"] = evaluated[f"forward_return_{window}d"]
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
    evaluated["record_type"] = EVALUATION_RECORD_TYPE
    evaluated["evaluation_only"] = True
    evaluated["evaluation_as_of_date"] = as_of.isoformat()
    evaluated["schema_version"] = SIMULATION_LEDGER_SCHEMA_VERSION
    evaluated["decision_record_id"] = evaluated.apply(_decision_record_id_from_row, axis=1)
    key_cols = ["date", "model_version", "symbol", "evaluation_as_of_date"]
    evaluation_keys = {
        tuple(row[column] for column in key_cols) for _, row in evaluated.iterrows()
    }
    retained = ledger.loc[
        ~(
            (ledger["record_type"] == EVALUATION_RECORD_TYPE)
            & ledger[key_cols].apply(lambda row: tuple(row), axis=1).isin(evaluation_keys)
        )
    ]
    output = _sort_ledger(pd.concat([retained, evaluated], ignore_index=True))
    raise_for_no_lookahead_violations(
        validate_no_lookahead_records(simulation_records=output)
    )
    output.to_csv(ledger_path, index=False)
    return ledger_path


def render_simulation_report(ledger_path: Path, window: str = "60d") -> str:
    if not ledger_path.exists():
        raise FileNotFoundError(f"ETF simulation ledger does not exist: {ledger_path}")
    ledger = _normalize_ledger_schema(pd.read_csv(ledger_path))
    decision_rows = _decision_rows(ledger)
    evaluation_rows = _evaluation_rows(ledger)
    lines = [
        f"# ETF Simulation Report - {window}",
        "",
        f"- Ledger：`{ledger_path}`",
        f"- Records：{len(ledger)}",
        f"- Decision Records：{len(decision_rows)}",
        f"- Evaluation Records：{len(evaluation_rows)}",
        "",
        "## Model Versions",
        "",
    ]
    if decision_rows.empty:
        lines.append("暂无模拟舱记录。")
    else:
        for version, group in decision_rows.groupby("model_version", sort=True):
            evaluation_group = evaluation_rows.loc[evaluation_rows["model_version"] == version]
            hit_rate = _hit_rate(evaluation_group)
            lines.append(
                f"- {version}：decision_records={len(group)}；"
                f"evaluation_records={len(evaluation_group)}；"
                f"20d hit rate={hit_rate if hit_rate is not None else 'n/a'}"
            )
    lines.extend(["", "## Portfolio vs Benchmarks", ""])
    if evaluation_rows.empty:
        lines.append("暂无 portfolio-vs-benchmark 样本。")
    else:
        for version, group in evaluation_rows.groupby("model_version", sort=True):
            summary = _portfolio_vs_benchmark_summary(group)
            lines.append(f"- {version}：{summary}")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- decision rows 只记录决策时可见信息，evaluation rows 才承载 forward return。",
            "- forward return 不足窗口时保持 null，不填 0。",
        ]
    )
    return "\n".join(lines) + "\n"


def summarize_simulation_for_brief(
    ledger_path: Path,
    *,
    as_of: date | None = None,
) -> str:
    if not ledger_path.exists():
        return f"模拟舱暂无记录：未找到 ledger `{ledger_path}`。"
    ledger = _normalize_ledger_schema(pd.read_csv(ledger_path))
    if ledger.empty:
        return f"模拟舱暂无记录：ledger `{ledger_path}` 为空。"
    selected = ledger.copy()
    if as_of is not None and "date" in selected.columns:
        parsed_dates = pd.to_datetime(selected["date"], errors="coerce")
        selected = selected.loc[parsed_dates <= pd.Timestamp(as_of)].copy()
    if selected.empty:
        return f"模拟舱暂无截至 {as_of.isoformat() if as_of else 'n/a'} 的记录。"
    decision_rows = _decision_rows(selected)
    evaluation_rows = _evaluation_rows(selected)

    lines = [
        f"- Ledger: `{ledger_path}`",
        f"- Records: {len(selected)}",
        f"- Decision records: {len(decision_rows)}",
        f"- Evaluation records: {len(evaluation_rows)}",
    ]
    for version, group in decision_rows.groupby("model_version", sort=True):
        latest_date = pd.to_datetime(group["date"], errors="coerce").max()
        latest_label = (
            latest_date.date().isoformat() if pd.notna(latest_date) else "n/a"
        )
        evaluation_group = evaluation_rows.loc[evaluation_rows["model_version"] == version]
        hit_rate = _hit_rate(evaluation_group)
        lines.append(
            f"- {version}：decision_records={len(group)}；"
            f"evaluation_records={len(evaluation_group)}；latest_snapshot={latest_label}；"
            f"20d hit rate={hit_rate if hit_rate is not None else 'n/a'}；"
            f"{_portfolio_vs_benchmark_summary(evaluation_group)}"
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


def _enrich_decision_rows(frame: pd.DataFrame, *, report_path: Path | None) -> pd.DataFrame:
    output = frame.copy()
    output["schema_version"] = SIMULATION_LEDGER_SCHEMA_VERSION
    output["record_type"] = DECISION_RECORD_TYPE
    output["decision_date"] = output["date"]
    output["evaluation_only"] = False
    output["evaluation_as_of_date"] = None
    output["observe_only"] = True
    output["production_effect"] = "none"
    if report_path is not None:
        output["report_path"] = str(report_path)
    for column in EVALUATION_COLUMNS:
        if column not in output:
            output[column] = None
    for _, group in output.groupby(["date", "model_version"], sort=False):
        index = group.index
        output.loc[index, "asset_scores"] = _json_map(group, "composite_score")
        output.loc[index, "target_weights"] = _json_map(group, "target_weight")
        output.loc[index, "previous_target_weights"] = _json_map(group, "previous_weight")
        output.loc[index, "weight_deltas"] = _json_map(group, "trade_delta")
        output.loc[index, "signal_snapshot_hash"] = _snapshot_hash(
            group,
            ["date", "symbol", "composite_score", "reason_codes", "model_version"],
        )
        output.loc[index, "feature_snapshot_hash"] = _snapshot_hash(
            group,
            ["date", "symbol", "model_version", "config_hash", "data_quality_status"],
        )
    output["decision_record_id"] = output.apply(_decision_record_id_from_row, axis=1)
    return output


def _normalize_ledger_schema(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if "evaluation_only" not in output.columns:
        output["evaluation_only"] = _evaluation_value_mask(output)
    if "record_type" not in output.columns:
        output["record_type"] = [
            EVALUATION_RECORD_TYPE if _truthy(value) else DECISION_RECORD_TYPE
            for value in output["evaluation_only"]
        ]
    if "schema_version" not in output.columns:
        output["schema_version"] = SIMULATION_LEDGER_SCHEMA_VERSION
    if "decision_date" not in output.columns and "date" in output.columns:
        output["decision_date"] = output["date"]
    if "evaluation_as_of_date" not in output.columns:
        output["evaluation_as_of_date"] = None
    if "observe_only" not in output.columns:
        output["observe_only"] = True
    if "production_effect" not in output.columns:
        output["production_effect"] = "none"
    for column in EVALUATION_COLUMNS:
        if column not in output.columns:
            output[column] = None
    if "decision_record_id" not in output.columns and {"date", "model_version", "symbol"}.issubset(
        output.columns
    ):
        output["decision_record_id"] = output.apply(_decision_record_id_from_row, axis=1)
    return output


def _sort_ledger(frame: pd.DataFrame) -> pd.DataFrame:
    output = _normalize_ledger_schema(frame)
    sort_columns = [
        column
        for column in ("date", "model_version", "symbol", "record_type", "evaluation_as_of_date")
        if column in output.columns
    ]
    return output.sort_values(sort_columns).reset_index(drop=True)


def _decision_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if "record_type" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["record_type"] == DECISION_RECORD_TYPE].copy()


def _evaluation_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if "record_type" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["record_type"] == EVALUATION_RECORD_TYPE].copy()


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
    if group.empty:
        return "avg 20d portfolio return=n/a; avg relative vs SPY=n/a; avg relative vs QQQ=n/a"
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


def _json_map(group: pd.DataFrame, value_column: str) -> str:
    payload: dict[str, object] = {}
    if value_column not in group.columns:
        return "{}"
    for _, row in group.sort_values("symbol").iterrows():
        value = row.get(value_column)
        if pd.isna(value):
            continue
        payload[str(row["symbol"])] = _json_scalar(value)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _snapshot_hash(group: pd.DataFrame, columns: list[str]) -> str:
    records: list[dict[str, object]] = []
    available_columns = [column for column in columns if column in group.columns]
    for _, row in group.sort_values("symbol").iterrows():
        records.append(
            {column: _json_scalar(row.get(column)) for column in available_columns}
        )
    encoded = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _decision_record_id_from_row(row: pd.Series) -> str:
    payload = {
        "date": str(row.get("date", "")),
        "model_version": str(row.get("model_version", "")),
        "symbol": str(row.get("symbol", "")),
        "config_hash": str(row.get("config_hash", "")),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _json_scalar(value: object) -> object:
    if pd.isna(value):
        return None
    if isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
