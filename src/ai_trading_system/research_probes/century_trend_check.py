"""1871–1928 慢熊段趋势覆盖的样本外核对，只印事先写死的月频比较。

Shiller 月均价格和股息构成总收益，用 bench 原有规则划段；价格趋势的月频等价
与同平均敞口、每月再平衡的固定组合逐段比较。月均、滞后 1 的 1929–2010
保真核对成立且至少有四段慢熊时，才按至少两个配置各有 2/3 正差、中位差为正
判读。月末保真、滞后 0 和零现金仅作敏感性展示。规则不是拟合结果。
不改门、不登记试验、不写研究账本、不提供策略准入或 production/broker 授权；
美股大盘是代理而非 QQQ 证据，GS10/12 是现金近似而非可交易国债收益。
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench.data import from_us_market
from ai_trading_system.research_bench.segments import (
    DROP,
    LONG_BULL_MONTHS,
    SLOW_MONTHS,
    Segment,
    drop_segments,
    long_bull_segments,
    monthly_level,
    segment_return,
    window_returns,
)
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_data import signal_sources

RUN_ID = "TRADING-2565-41"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-21:period3_direction"
SNAPSHOT_ID = "sig-ed9e988ffe7c"
PRICE_SOURCE = "shiller_price"
DIVIDEND_SOURCE = "shiller_dividend"
RATE_SOURCE = "shiller_gs10"
FIRST_MONTH = "1871-01"
LAST_MONTH = "1928-12"
EVAL_FIRST = "1872-03"
WINDOW_MONTHS = 696
RATE_LOW = 2.0
RATE_HIGH = 8.0
REPRODUCE_TOLERANCE = 1e-12
# 需求第 3 节的月频等价：敞口是这些回看月数里收益为正的比例。
LOOKBACKS = {"slow_only_m": (12,), "fast_slow_m": (1, 12), "trend4_m": (1, 3, 6, 12)}
CONFIGS = tuple(LOOKBACKS)
LAGS = (0, 1)
LAG = 1
CASHES = ("gs10", "zero")
CASH = "gs10"
COST_BPS = 25.0
TAIL_SHARE = 0.05
# 需求第 5 节：运行前写死的只读判读，非策略准入门槛。
HOLD_NUMERATOR = 2
HOLD_DENOMINATOR = 3
MIN_HOLDING_CONFIGS = 2
MIN_SLOW_BEARS = 4
FIDELITY_START = date(1929, 1, 2)
FIDELITY_END = date(2010, 12, 31)
FIDELITY_WINDOW = Window("us_market_1929_2010", "development", FIDELITY_START, FIDELITY_END)
FIDELITY_FIRST = "1929-01"
FIDELITY_LAST = "2010-12"
DAILY_SLOW_BEARS = 10
# 慢熊探针 TRADING-2565-34 在 us_market_1929_2010 的 10 个慢熊段里 D > 0 的段数。
# 判读用月均、滞后 1（需求第 10 节修订 1），各差不超过 2 段才成立；月末两种只印。
DAILY_POSITIVE = {"slow_only_m": 8, "fast_slow_m": 10, "trend4_m": 9}
FIDELITY_SEGMENTS = 2
FIDELITY_VARIANTS = ("average_lag1", "month_end_lag1", "month_end_lag0")
DECISIVE_VARIANT = "average_lag1"
RESULT_JSON = "century_trend_result.json"
RESULT_MD = "century_trend_result.md"
SECTION_TEXT = {
    "main": "滞后 1、现金用长期利率（判读用）",
    "lag0": "滞后 0（只核数据偏差）",
    "cash_zero": "现金 = 0（敏感性）",
}
SIGNAL_NOTE = (
    "- 1871–1928 的信号不含股息，保真用的是总收益水平，12 个月收益的符号在价格收益落在"
    " (−股息率, 0) 的月份会不同，是说明不是判据。"
)
VARIANT_TEXT = {
    "average_lag1": "月均、滞后 1（判读用）",
    "month_end_lag1": "月末、滞后 1",
    "month_end_lag0": "月末、滞后 0",
}
KIND_TEXT = {"slow_bear": "慢熊", "fast_drop": "快跌", "long_bull": "长牛"}

app = typer.Typer(help="1871–1928 慢熊段趋势覆盖核对：只读，不改门、不写研究账本。")


def shiller_inputs(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    months = pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")
    columns = {}
    for column, source in (("P", PRICE_SOURCE), ("D", DIVIDEND_SOURCE), ("GS10", RATE_SOURCE)):
        if source not in frames:
            raise ValueError(f"snapshot has no {source}")
        frame = frames[source]
        if "value" not in frame.columns:
            raise ValueError(f"{source} frame needs a value column")
        dates = pd.DatetimeIndex(frame.index).to_period("M")
        inside = (dates >= months[0]) & (dates <= months[-1])
        selected = dates[inside]
        if selected.has_duplicates:
            raise ValueError(f"{source} repeats a month")
        values = pd.to_numeric(frame.loc[inside, "value"], errors="coerce")
        columns[column] = pd.Series(values.to_numpy(), index=selected).reindex(months)
    return pd.DataFrame(columns, index=months)


def total_returns(inputs: pd.DataFrame) -> pd.Series:
    returns = ((inputs["P"] + inputs["D"] / 12) / inputs["P"].shift(1) - 1).iloc[1:]
    returns.index = pd.PeriodIndex(returns.index, freq="M").to_timestamp()
    return returns.rename("total_return")


def cash_returns(inputs: pd.DataFrame, cash: str) -> pd.Series:
    if cash == "gs10":
        returns = (inputs["GS10"].shift(1) / 100 / 12).iloc[1:]
    elif cash == "zero":
        returns = pd.Series(0.0, index=inputs.index[1:])
    else:
        raise ValueError(f"unknown cash {cash}")
    returns.index = pd.PeriodIndex(returns.index, freq="M").to_timestamp()
    return returns.rename("cash")


def data_check(inputs: pd.DataFrame) -> dict[str, Any]:
    missing = {column: int(inputs[column].isna().sum()) for column in ("P", "D", "GS10")}
    nonpositive = {column: int((inputs[column] <= 0).sum()) for column in ("P", "D")}
    rates = inputs["GS10"]
    outside = int(((rates < RATE_LOW) | (rates > RATE_HIGH)).sum())
    returns = total_returns(inputs)
    level = (1 + returns).cumprod()
    prior = level.shift(1)
    if not prior.empty:
        prior.iloc[0] = 1.0
    errors = (level / prior - 1 - returns).abs()
    finite = errors.loc[np.isfinite(errors)]
    error = None if finite.empty else float(finite.max())
    return {
        "months": len(inputs),
        "first": str(inputs.index[0]) if len(inputs) else None,
        "last": str(inputs.index[-1]) if len(inputs) else None,
        "missing": missing,
        "nonpositive": nonpositive,
        "rate_out_of_range": outside,
        "reproduce_error": error,
        "ok": (
            len(inputs) == WINDOW_MONTHS
            and not any(missing.values())
            and not any(nonpositive.values())
            and outside == 0
            and error is not None
            and error <= REPRODUCE_TOLERANCE
        ),
    }


def exposure(level: pd.Series, lookbacks: Sequence[int]) -> pd.Series:
    votes = []
    for months in lookbacks:
        change = level / level.shift(months) - 1
        votes.append((change > 0).astype(float).where(change.notna()))
    return pd.concat(votes, axis=1).mean(axis=1, skipna=False).rename("exposure")


def held(signal: pd.Series, lag: int) -> pd.Series:
    if lag not in LAGS:
        raise ValueError(f"lag must be one of {LAGS}")
    return signal.shift(1 + lag)


@dataclass(frozen=True)
class Pair:
    strategy: pd.Series
    benchmark: pd.Series
    exposure: pd.Series
    average_exposure: float
    turnover: pd.Series


def pair_returns(
    market: pd.Series, cash: pd.Series, holding: pd.Series, first: str, last: str
) -> Pair:
    months = pd.period_range(first, last, freq="M").to_timestamp()
    aligned = pd.DataFrame(
        {
            "market": market.reindex(months),
            "cash": cash.reindex(months),
            "holding": holding.reindex(months),
        }
    )
    missing = aligned.isna().any(axis=1)
    if missing.any():
        raise ValueError(f"missing value in {months[missing][0]:%Y-%m}")
    m, c, e = aligned["market"], aligned["cash"], aligned["holding"]
    average = float(e.mean())
    turnover = e.diff().abs()
    turnover.iloc[0] = 0.0
    return Pair(e * m + (1 - e) * c, average * m + (1 - average) * c, e, average, turnover)


def find_segments(returns: pd.Series) -> list[Segment]:
    level = (1 + returns).cumprod()
    level.index = pd.DatetimeIndex(level.index).to_period("M")
    drops = drop_segments(level)
    return sorted([*drops, *long_bull_segments(level, drops)], key=lambda item: item.start)


def annualized(total: float, months: int) -> float:
    return float((1 + total) ** (12 / months) - 1)


def segment_rows(pair: Pair, items: Sequence[Segment]) -> list[dict[str, Any]]:
    rows = []
    for item in items:
        own = segment_return(pair.strategy, item)
        reference = segment_return(pair.benchmark, item)
        inside = pair.exposure.loc[f"{item.start}-01" : f"{item.end}-01"]
        row: dict[str, Any] = {
            "id": item.id,
            "kind": item.kind,
            "start": item.start,
            "end": item.end,
            "months": item.months,
            "change": item.change,
            "strategy_return": own,
            "benchmark_return": reference,
            "difference": own - reference,
            "average_exposure": float(inside.mean()),
        }
        if item.kind == "long_bull":
            own_annual, reference_annual = (
                annualized(own, item.months),
                annualized(reference, item.months),
            )
            row.update(
                strategy_annual=own_annual,
                benchmark_annual=reference_annual,
                annual_difference=own_annual - reference_annual,
            )
        rows.append(row)
    return rows


def summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for kind in ("slow_bear", "fast_drop"):
        values = [row["difference"] for row in rows if row["kind"] == kind]
        positive = sum(value > 0 for value in values)
        result[kind] = {
            "count": len(values),
            "median_difference": float(np.median(values)) if values else None,
            "positive": positive,
            "positive_share": positive / len(values) if values else None,
            "min_difference": min(values) if values else None,
        }
    annual = [row["annual_difference"] for row in rows if row["kind"] == "long_bull"]
    result["long_bull"] = {
        "count": len(annual),
        "median_annual_difference": float(np.median(annual)) if annual else None,
    }
    return result


def geometric(returns: pd.Series) -> float:
    return annualized(float(np.prod(1 + returns.to_numpy(dtype=float))) - 1, len(returns))


def max_drawdown(returns: pd.Series) -> float:
    level = np.cumprod(1 + returns.to_numpy(dtype=float))
    peak = np.maximum.accumulate(np.concatenate(([1.0], level)))[1:]
    return float(np.min(level / peak - 1))


def tail_mean(returns: pd.Series, share: float = TAIL_SHARE) -> float:
    count = math.ceil(round(share * len(returns), 9))
    return float(returns.nsmallest(count).mean())


def whole_window(pair: Pair, cost_bps: float = COST_BPS) -> dict[str, Any]:
    result = {}
    for name, measure in (
        ("geometric", geometric),
        ("max_drawdown", max_drawdown),
        ("tail_mean", tail_mean),
    ):
        own, reference = measure(pair.strategy), measure(pair.benchmark)
        result[name] = {"strategy": own, "benchmark": reference, "difference": own - reference}
    result["cost"] = {
        "bps": cost_bps,
        "turnover_per_year": float(pair.turnover.sum()) * 12 / len(pair.strategy),
        "geometric_difference": (
            geometric(pair.strategy - pair.turnover * cost_bps / 10_000) - geometric(pair.benchmark)
        ),
    }
    return result


def config_section(
    market: pd.Series,
    cash: pd.Series,
    signal: pd.Series,
    lag: int,
    first: str,
    last: str,
    items: Sequence[Segment],
) -> dict[str, Any]:
    pair = pair_returns(market, cash, held(signal, lag), first, last)
    rows = segment_rows(pair, items)
    return {
        "average_exposure": pair.average_exposure,
        "rows": rows,
        "summary": summary(rows),
        "whole": whole_window(pair),
    }


def sections(
    market: pd.Series,
    cash: pd.Series,
    signal_level: pd.Series,
    lag: int,
    first: str,
    last: str,
    items: Sequence[Segment],
) -> dict[str, Any]:
    return {
        name: config_section(
            market, cash, exposure(signal_level, LOOKBACKS[name]), lag, first, last, items
        )
        for name in CONFIGS
    }


def config_holds(slow_bear: Mapping[str, Any]) -> bool:
    median = slow_bear["median_difference"]
    return bool(
        slow_bear["count"] > 0
        and HOLD_DENOMINATOR * slow_bear["positive"] >= HOLD_NUMERATOR * slow_bear["count"]
        and median is not None
        and median > 0
    )


def reading(sections: Mapping[str, Any], slow_bears: int, fidelity_holds: bool) -> dict[str, Any]:
    configs = {name: config_holds(sections[name]["summary"]["slow_bear"]) for name in CONFIGS}
    holding = sum(configs.values())
    if slow_bears < MIN_SLOW_BEARS:
        verdict, reason = "unread", "few_slow_bears"
    elif not fidelity_holds:
        verdict, reason = "unread", "fidelity"
    else:
        verdict, reason = ("holds" if holding >= MIN_HOLDING_CONFIGS else "fails"), None
    return {"configs": configs, "holding": holding, "verdict": verdict, "reason": reason}


def fidelity_variant(
    market: pd.Series, cash: pd.Series, signal_level: pd.Series, lag: int, items: Sequence[Segment]
) -> dict[str, Any]:
    found = sections(market, cash, signal_level, lag, FIDELITY_FIRST, FIDELITY_LAST, items)
    positive = {name: found[name]["summary"]["slow_bear"]["positive"] for name in CONFIGS}
    gap = {name: positive[name] - DAILY_POSITIVE[name] for name in CONFIGS}
    return {
        "positive": positive,
        "gap": gap,
        "within": all(abs(value) <= FIDELITY_SEGMENTS for value in gap.values()),
        "slow_bears": sum(item.kind == "slow_bear" for item in items),
        "summaries": {name: found[name]["summary"] for name in CONFIGS},
    }


def fidelity(daily_market: pd.Series, daily_cash: pd.Series) -> dict[str, Any]:
    market = daily_market.loc[: pd.Timestamp(FIDELITY_END)]
    cash = daily_cash.loc[: pd.Timestamp(FIDELITY_END)]
    daily_level = (1 + market).cumprod()
    means = daily_level.groupby(pd.DatetimeIndex(daily_level.index).to_period("M")).mean()
    means.index = pd.PeriodIndex(means.index, freq="M").to_timestamp()
    average_returns = means / means.shift(1) - 1
    average_items = [
        item
        for item in find_segments(average_returns.loc[f"{FIDELITY_FIRST}-01" :])
        if item.kind != "long_bull"
    ]
    monthly = (1 + market).groupby(pd.DatetimeIndex(market.index).to_period("M")).prod() - 1
    monthly.index = pd.PeriodIndex(monthly.index, freq="M").to_timestamp()
    rf = (1 + cash).groupby(pd.DatetimeIndex(cash.index).to_period("M")).prod() - 1
    rf.index = pd.PeriodIndex(rf.index, freq="M").to_timestamp()
    end_signal = (1 + monthly).cumprod()
    end_items = drop_segments(monthly_level(window_returns(market, FIDELITY_WINDOW)))
    variants = {
        "average_lag1": fidelity_variant(average_returns, rf, means, 1, average_items),
        "month_end_lag1": fidelity_variant(monthly, rf, end_signal, 1, end_items),
        "month_end_lag0": fidelity_variant(monthly, rf, end_signal, 0, end_items),
    }
    return {
        "window": {"first": FIDELITY_FIRST, "last": FIDELITY_LAST},
        "daily": {"slow_bears": DAILY_SLOW_BEARS, "positive": dict(DAILY_POSITIVE)},
        "decisive": DECISIVE_VARIANT,
        "holds": variants[DECISIVE_VARIANT]["within"],
        "variants": variants,
    }


def run_probe(
    frames: Mapping[str, pd.DataFrame], *, snapshot_id: str, provenance: Mapping[str, Any]
) -> dict[str, Any]:
    inputs = shiller_inputs(frames)
    check = data_check(inputs)
    if not check["ok"]:
        raise ValueError("data check failed")
    data = from_us_market(frames, snapshot_id=snapshot_id)
    market = total_returns(inputs)
    price = pd.Series(
        inputs["P"].to_numpy(), index=pd.PeriodIndex(inputs.index, freq="M").to_timestamp()
    )
    items = find_segments(market.loc[f"{EVAL_FIRST}-01" :])
    tables = {
        key: sections(market, cash_returns(inputs, cash), price, lag, EVAL_FIRST, LAST_MONTH, items)
        for key, lag, cash in (("main", LAG, CASH), ("lag0", 0, CASH), ("cash_zero", LAG, "zero"))
    }
    checked = fidelity(data.returns["qqq"], data.returns["cash"])
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": snapshot_id,
        "window": {"first": FIRST_MONTH, "last": LAST_MONTH, "evaluated_from": EVAL_FIRST},
        "rules": {
            "drop": DROP,
            "slow_months": SLOW_MONTHS,
            "long_bull_months": LONG_BULL_MONTHS,
            "lookbacks": {name: list(LOOKBACKS[name]) for name in CONFIGS},
            "signal": "price",
            "lag": LAG,
            "cash": CASH,
            "hold_share": f"{HOLD_NUMERATOR}/{HOLD_DENOMINATOR}",
            "min_holding_configs": MIN_HOLDING_CONFIGS,
            "min_slow_bears": MIN_SLOW_BEARS,
            "fidelity_segments": FIDELITY_SEGMENTS,
            "cost_bps": COST_BPS,
            "tail_share": TAIL_SHARE,
        },
        "provenance": dict(provenance),
        "check": check,
        "segments": [asdict(item) for item in items],
        "sections": tables,
        "fidelity": checked,
        "reading": reading(
            tables["main"], sum(s.kind == "slow_bear" for s in items), checked["holds"]
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _clean(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_clean(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _points(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:+.1f}"


def _percent(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def _read_text(read: Mapping[str, Any]) -> str:
    text = {
        "holds": "趋势覆盖的慢熊效果在 1871–1928 样本外成立",
        "fails": "1871–1928 没站住",
        "unread": "只印不读",
    }[read["verdict"]]
    reason = {"few_slow_bears": "慢熊段少于 4 段", "fidelity": "保真核对不成立"}.get(read["reason"])
    return text if reason is None else f"{text}（{reason}）"


def _check_text(check: Mapping[str, Any], snapshot_id: str) -> str:
    missing, nonpositive = check["missing"], check["nonpositive"]
    error = check["reproduce_error"]
    formatted = "—" if error is None else f"{error:.1e}"
    status = "检查通过" if check["ok"] else "检查不通过"
    return (
        f"century_trend_check: 快照 {snapshot_id}；{check['first']}..{check['last']} "
        f"共 {check['months']} 个月；缺失 P {missing['P']} / D {missing['D']} "
        f"/ GS10 {missing['GS10']}；"
        f"P ≤ 0 {nonpositive['P']} 个月、D ≤ 0 {nonpositive['D']} 个月；"
        f"GS10 在 2–8 以外 {check['rate_out_of_range']} 个月；总收益重算误差 {formatted}；{status}"
    )


def render_markdown(result: Mapping[str, Any]) -> str:
    window, rules = result["window"], result["rules"]
    lines = [
        "# 1871–1928 慢熊段上的趋势覆盖：样本外核对",
        "",
        f"- 运行 {result['run_id']}；决定 {result['owner_decision']}；只读，不改门、不计试验。",
        f"- 快照 {result['snapshot_id']}；窗口 {window['first']}..{window['last']}，"
        f"评估 {window['evaluated_from']}..{window['last']}；跌 {_percent(rules['drop'])}，"
        f"慢熊至少 {rules['slow_months']} 个月，长牛超过 {rules['long_bull_months']} 个月。",
        SIGNAL_NOTE,
        f"- {_check_text(result['check'], result['snapshot_id'])}",
        "",
        "## 段",
        "",
        "| 段 | 种类 | 起 | 止 | 月数 | 指数变化 |",
        "|---|---|---|---|---:|---:|",
    ]
    for item in result["segments"]:
        lines.append(
            f"| {item['id']} | {KIND_TEXT[item['kind']]} | {item['start']} | {item['end']} | "
            f"{item['months']} | {_percent(item['change'])} |"
        )
    main = result["sections"]["main"]
    for kind, title in (("slow_bear", "慢熊段"), ("fast_drop", "快跌段")):
        lines.extend(
            [
                "",
                f"## {title}（{SECTION_TEXT['main']}，D = A − B，点）",
                "",
                "| 配置 | 段 | A | B | D | 段内平均敞口 |",
                "|---|---|---:|---:|---:|---:|",
            ]
        )
        for name in CONFIGS:
            for row in main[name]["rows"]:
                if row["kind"] == kind:
                    lines.append(
                        f"| {name} | {row['id']} | {_points(row['strategy_return'])} | "
                        f"{_points(row['benchmark_return'])} | {_points(row['difference'])} | "
                        f"{_percent(row['average_exposure'])} |"
                    )
    lines.extend(
        [
            "",
            "## 长牛段（年化，点/年）",
            "",
            "| 配置 | 段 | A | B | 差 |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for name in CONFIGS:
        for row in main[name]["rows"]:
            if row["kind"] == "long_bull":
                lines.append(
                    f"| {name} | {row['id']} | {_points(row['strategy_annual'])} | "
                    f"{_points(row['benchmark_annual'])} | {_points(row['annual_difference'])} |"
                )
    for key, title in SECTION_TEXT.items():
        lines.extend(
            [
                "",
                f"## 汇总：{title}",
                "",
                "| 配置 | 慢熊段数 | D 中位 | D > 0 占比 | 最小 D | "
                "长牛代价中位（点/年） | 平均敞口 |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for name in CONFIGS:
            section = result["sections"][key][name]
            slow, bull = section["summary"]["slow_bear"], section["summary"]["long_bull"]
            lines.append(
                f"| {name} | {slow['count']} | {_points(slow['median_difference'])} | "
                f"{_percent(slow['positive_share'])} | {_points(slow['min_difference'])} | "
                f"{_points(bull['median_annual_difference'])} | "
                f"{_percent(section['average_exposure'])} |"
            )
    lines.extend(
        [
            "",
            "## 全窗口（只读）",
            "",
            "| 配置 | 几何年化差 | 最大回撤差 | 最差 5% 月均值差 | 25 bps 后几何差 | 年换手 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for name in CONFIGS:
        whole = main[name]["whole"]
        lines.append(
            f"| {name} | {_points(whole['geometric']['difference'])} | "
            f"{_points(whole['max_drawdown']['difference'])} | "
            f"{_points(whole['tail_mean']['difference'])} | "
            f"{_points(whole['cost']['geometric_difference'])} | "
            f"{whole['cost']['turnover_per_year']:.1f} |"
        )
    check = result["fidelity"]
    lines.extend(
        [
            "",
            "## 保真核对（美股大盘 1929–2010，D > 0 的慢熊段数）",
            "",
            "| 口径 | 慢熊段数 | " + " | ".join(CONFIGS) + " | 成立 |",
            "|---|---:|---:|---:|---:|---|",
            f"| 日频 | {check['daily']['slow_bears']} | "
            + " | ".join(str(check["daily"]["positive"][name]) for name in CONFIGS)
            + " | — |",
        ]
    )
    for key in FIDELITY_VARIANTS:
        variant = check["variants"][key]
        cells = " | ".join(
            f"{variant['positive'][name]}（{variant['gap'][name]:+d}）" for name in CONFIGS
        )
        status = "是" if variant["within"] else "否"
        lines.append(f"| {VARIANT_TEXT[key]} | {variant['slow_bears']} | {cells} | {status} |")
    read = result["reading"]
    lines.extend(["", "## 读法（运行前写死）", ""])
    for name in CONFIGS:
        lines.append(f"- {name}：{'跨世纪成立' if read['configs'][name] else '没站住'}")
    lines.extend(
        [
            f"- 三个配置里成立 {read['holding']} 个；读作：{_read_text(read)}",
            "",
            "production_effect=none；broker_action=none。",
            "",
        ]
    )
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / RESULT_JSON).write_text(
        json.dumps(_clean(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_frames(snapshot_id: str) -> dict[str, pd.DataFrame]:
    return signal_sources.load_snapshot(PROJECT_ROOT, snapshot_id).frames


def _load_or_exit(snapshot_id: str) -> dict[str, pd.DataFrame]:
    try:
        return load_frames(snapshot_id)
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"century_trend_check: 数据载入失败：{exc}")
        raise typer.Exit(1) from exc


@app.callback()
def callback() -> None:
    """只读核对的子命令入口。"""


@app.command("check")
def check_command(snapshot: Annotated[str, typer.Option("--snapshot")] = SNAPSHOT_ID) -> None:
    frames = _load_or_exit(snapshot)
    try:
        inputs = shiller_inputs(frames)
    except ValueError as exc:
        typer.echo(f"century_trend_check: 数据不能用：{exc}")
        raise typer.Exit(1) from exc
    check = data_check(inputs)
    typer.echo(_check_text(check, snapshot))
    if not check["ok"]:
        raise typer.Exit(1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option("--out")],
    snapshot: Annotated[str, typer.Option("--snapshot")] = SNAPSHOT_ID,
) -> None:
    if any((out / name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("century_trend_check: 已有结果，拒绝覆盖。")
        raise typer.Exit(1)
    frames = _load_or_exit(snapshot)
    try:
        result = run_probe(frames, snapshot_id=snapshot, provenance=git_provenance(PROJECT_ROOT))
    except ValueError as exc:
        typer.echo(f"century_trend_check: 不能运行：{exc}")
        raise typer.Exit(1) from exc
    write_result(result, out)
    typer.echo(_check_text(result["check"], snapshot))
    for item in result["segments"]:
        typer.echo(
            f"century_trend_check: {item['id']} {item['start']}..{item['end']} "
            f"{_percent(item['change'])} {item['months']} 个月 {KIND_TEXT[item['kind']]}"
        )
    for name in CONFIGS:
        slow = result["sections"]["main"][name]["summary"]["slow_bear"]
        typer.echo(
            f"century_trend_check: {name} 慢熊 {slow['count']} 段，"
            f"D 中位 {_points(slow['median_difference'])} 点，"
            f"D > 0 {slow['positive']}/{slow['count']}"
        )
    checked = result["fidelity"]
    gap = checked["variants"][DECISIVE_VARIANT]["gap"]
    gaps = "、".join(f"{name} 差 {gap[name]:+d}" for name in CONFIGS)
    typer.echo(
        f"century_trend_check: 保真 {VARIANT_TEXT[DECISIVE_VARIANT]} {gaps}，"
        f"{'成立' if checked['holds'] else '不成立'}"
    )
    typer.echo(f"century_trend_check: 读作 {_read_text(result['reading'])}；结果目录 {out}")


if __name__ == "__main__":
    app()
