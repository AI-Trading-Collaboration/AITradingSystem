"""Pure settlement of checked research judgments; never retain judgment prose."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_judgments.schema import effective_prob, parse_timestamp
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_EVAL_PATH = PROJECT_ROOT / "config" / "research" / "research_judgment_eval.yaml"
# Serialization precision is a protocol invariant shared by sign tests and frozen outcomes.
RETURN_DECIMALS = 10


class SettleParams(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    market_timezone: str
    market_close: str = Field(pattern=r"^[0-2][0-9]:[0-5][0-9]$")
    max_calendar_gap_days: int = Field(gt=0)
    max_abs_daily_move: float = Field(gt=0, allow_inf_nan=False)

    @field_validator("market_timezone")
    @classmethod
    def known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("unknown market timezone") from exc
        return value

    @field_validator("market_close")
    @classmethod
    def valid_close(cls, value: str) -> str:
        time.fromisoformat(value)
        return value


class EvalConfig(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    schema_version: Literal[1]
    settlement: SettleParams


def load_settle_params(path: Path = DEFAULT_EVAL_PATH) -> SettleParams:
    try:
        raw = safe_load_yaml_path(path)
    except (OSError, yaml.YAMLError, RecursionError) as exc:
        raise ValueError("invalid settlement configuration") from exc
    return EvalConfig.model_validate(raw).settlement


@dataclass(frozen=True)
class Outcome:
    id: str
    kind: str
    ticker: str
    source: Literal["ledger", "price"]
    state: Literal["settled", "pending", "void", "data_gap"]
    outcome: int | None
    reason: str
    prob_original: float
    prob_scored: float
    ledger_status: str
    conflict: bool = False
    overdue: bool = False
    anchor_date: str | None = None
    end_date: str | None = None
    ticker_return: float | None = None
    bench_return: float | None = None
    rel_return: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _base(record: dict[str, Any], source: Literal["ledger", "price"]) -> Outcome:
    return Outcome(
        id=record["id"],
        kind=record["kind"],
        ticker=record["ticker"],
        source=source,
        state="pending",
        outcome=None,
        reason="",
        prob_original=record["prob"],
        prob_scored=effective_prob(record),
        ledger_status=record["status"],
    )


def settle_ledger(record: dict[str, Any], asof: date) -> Outcome:
    base = _base(record, "ledger")
    status = record["status"]
    if status in ("hit", "miss"):
        return replace(base, state="settled", outcome=int(status == "hit"))
    if status == "void":
        return replace(base, state="void")
    if status == "open":
        return replace(base, overdue=asof > date.fromisoformat(record["deadline"]))
    return replace(base, state="data_gap", reason="unknown_status")


def settle_price(record: dict[str, Any], closes: pd.DataFrame, params: SettleParams) -> Outcome:
    base = _base(record, "price")
    if record["status"] == "void":
        return replace(base, state="void", reason="ledger_void")
    ticker = record["ticker"]
    market = record["market"]
    benchmark = market["benchmark"]
    if ticker == benchmark:
        return replace(base, state="data_gap", reason="ticker_is_benchmark")
    if ticker not in closes.columns or benchmark not in closes.columns:
        return replace(base, state="data_gap", reason="price_missing")
    pair = closes[[ticker, benchmark]].dropna().sort_index()
    if pair.empty:
        return replace(base, state="data_gap", reason="price_missing")
    calendar = pair.index
    anchor: int | None
    if market["anchor"] == "event_prev_close":
        event = next(
            (
                i
                for i, stamp in enumerate(calendar)
                if stamp.date() >= date.fromisoformat(record["deadline"])
            ),
            None,
        )
        if event is None:
            return replace(base, reason="no_event_day_yet")
        if event == 0:
            return replace(base, state="data_gap", reason="no_prior_close")
        anchor = event - 1
    else:
        created = parse_timestamp(record["createdAt"])
        assert created is not None  # J1 checks timestamps before settlement.
        local_close = time.fromisoformat(params.market_close)
        timezone = ZoneInfo(params.market_timezone)
        anchor = next(
            (
                i
                for i, stamp in enumerate(calendar)
                if datetime.combine(stamp.date(), local_close, tzinfo=timezone) > created
            ),
            None,
        )
        if anchor is None:
            return replace(base, reason="no_entry_close_yet")
    end = anchor + market["horizon"]
    if end >= len(calendar):
        return replace(base, reason="window_not_complete")
    window = pair.iloc[anchor : end + 1]
    dates = window.index
    if any(
        (right.date() - left.date()).days > params.max_calendar_gap_days
        for left, right in zip(dates[:-1], dates[1:], strict=True)
    ):
        return replace(base, state="data_gap", reason="calendar_gap")
    if (window <= 0).any().any():
        return replace(base, state="data_gap", reason="bad_price")
    if (window.pct_change(fill_method=None).abs() > params.max_abs_daily_move).any().any():
        return replace(base, state="data_gap", reason="suspect_jump")
    t_ret = float(window.iloc[-1][ticker] / window.iloc[0][ticker] - 1)
    b_ret = float(window.iloc[-1][benchmark] / window.iloc[0][benchmark] - 1)
    rel = round(t_ret - b_ret, RETURN_DECIMALS)
    result = replace(
        base,
        anchor_date=dates[0].date().isoformat(),
        end_date=dates[-1].date().isoformat(),
        ticker_return=round(t_ret, RETURN_DECIMALS),
        bench_return=round(b_ret, RETURN_DECIMALS),
        rel_return=rel,
    )
    if rel == 0:
        return replace(result, state="void", reason="tie")
    outcome = int(rel > 0 if market["direction"] == "outperform" else rel < 0)
    conflict = record["status"] in ("hit", "miss") and int(record["status"] == "hit") != outcome
    return replace(result, state="settled", outcome=outcome, conflict=conflict)


def settle_rows(
    rows: list[dict[str, Any]], closes: pd.DataFrame, params: SettleParams, asof: date
) -> list[Outcome]:
    return [
        settle_price(row, closes, params) if row["kind"] == "价格" else settle_ledger(row, asof)
        for row in sorted(rows, key=lambda row: row["id"])
        if row["purpose"] == "research"
    ]
