"""在 QQQ 一致性窗口读一次 2022 慢熊段的 D（TRADING-2565-74）。

依据 owner_decision:TRADING-2565:2026-10-26:consistency_2022_read、第 6 期章程第 18 节。
读 QQQ-Q3 的 fast_slow（F0001）和 slow_only（F0003）：D 是慢熊段策略收益减
同平均敞口（整个窗口的平均）固定对照的收益，未投资部分照评估台付现金收益。
开发窗口核对不过不登记；登记后只用许可证开窗；划段不过不读；算完记两条一致性
运行，最后写文件。目标、模拟、固定对照、划段、汇总和台账都调评估台，评估台不改。
区间按前瞻记录原文的一位小数判，两端都含，全精度两端只印。
运行记录的规则行只记、不改 QQQ-Q3 的结论。check 只读，不登记、不开一致性窗口。
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_output, git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments as sg
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData, from_snapshot, strategy_returns
from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.prereg import rule_problems
from ai_trading_system.research_bench.run import (
    PRIMARY_BENCHMARK,
    WindowPermit,
    benchmark_simulations,
    open_window,
)
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    WindowRole,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import Rebalance, Simulation, simulate
from ai_trading_system.research_data import qqq_long_history as qlh

RUN_ID = "TRADING-2565-74"
DECISION = "owner_decision:TRADING-2565:2026-10-26:consistency_2022_read"
QUESTION = "QQQ-Q3"
ROLE: WindowRole = "consistency_check"
STRATEGIES = ("fast_slow", "slow_only")
RECORDS = {"fast_slow": "F0001", "slow_only": "F0003"}
# TRADING-2565-34 的 JSON（slow-bear/rc-report.md 第 373、375 行）.stages.stage2 的
# average_exposure、slow_bear.median_difference、slow_bear.min_difference
ANCHORS = {
    "fast_slow": {
        "average_exposure": 0.5941523375781023,
        "median_difference": 0.09223942070915914,
        "min_difference": 0.06567862091761645,
    },
    "slow_only": {
        "average_exposure": 0.6557873485868102,
        "median_difference": 0.18687386804531436,
        "min_difference": 0.13144606813888593,
    },
}
# 前瞻记录原文的区间（一位小数的点），两端都含，判用这个
RANGES = {"fast_slow": (0.026, 0.120), "slow_only": (-0.048, 0.167)}
# 同两行 .stages.stage1.slow_bear 的 min_difference、median_difference，只印
FULL_RANGES = {
    "fast_slow": (0.02633206433891666, 0.11970601274143344),
    "slow_only": (-0.04777164845950099, 0.1669990867122531),
}
# 和评估台逐个数对的容限，不是研究阈值
TOLERANCE = 1e-12
# 建数据核对从这天数起（252 日回看用到的最早那年）
DATA_CHECK_START = date(2020, 2, 1)
LABEL = "一致性窗口 2022 段（旧任务用过、走势已知）"
RECORD_NOTE = "只记、不改 QQQ-Q3 的结论"
SUMMARY, REPORT = "consistency_2022_read.json", "consistency_2022_read.md"
# 错误信息里的数一律换掉，一致性窗口的数不经错误信息出来
NUMBER = re.compile(r"[-+.]?\d[\d.eE+-]*")

Record = dict[str, Any]
Anchors = Mapping[str, Mapping[str, float]]
Ranges = Mapping[str, tuple[float, float]]
Progress = Callable[[str], None]
STAGES = {
    "dev_check": "开发窗口核对",
    "register": "登记",
    "segments": "开窗和划段核对",
    "done": "读完",
}
app = typer.Typer()


def error_text(exc: Exception) -> str:
    lines = str(exc).splitlines()
    text = type(exc).__name__
    if lines and lines[0]:
        text += f": {lines[0]}"
    return NUMBER.sub("#", text)[: lg.ERROR_TEXT_LIMIT]


def _finite(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _result(result: Record) -> Record:
    return {key: _finite(value) for key, value in result.items()}


def _window_row(window: Window) -> Record:
    return {
        "window_id": window.window_id,
        "role": window.role,
        "start": window.start.isoformat(),
        "end": None if window.end is None else window.end.isoformat(),
    }


def _plan(
    question: lg.Question, data: BenchData, strategy_id: str
) -> tuple[pd.Series, Rebalance, int]:
    entry = question.prereg.strategy(strategy_id)
    targets = strategy_targets(
        lg.load_strategy(question, strategy_id), data.inputs, dict(entry.params)
    )
    return targets, entry.rebalance.to_rebalance(), entry.delay_days


def _pair(
    question: lg.Question, data: BenchData, loaded: LoadedSettings, strategy_id: str, window: Window
) -> tuple[Simulation, Simulation, Simulation]:
    targets, rebalance, delay = _plan(question, data, strategy_id)
    own = simulate(
        strategy_returns(data, "cash"),
        targets,
        start=window.start,
        end=window.end,
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
        delay_days=delay,
    )
    benchmarks = benchmark_simulations(
        data,
        window,
        average_exposure=float(own.exposure.mean()),
        rebalance=rebalance,
        cost_bps=loaded.cost_bps,
        leveraged=False,
    )
    return own, benchmarks[PRIMARY_BENCHMARK], benchmarks["buy_and_hold_qqq"]


def _gap(left: float | None, right: float | None) -> float | None:
    if left is None or right is None or not math.isfinite(left) or not math.isfinite(right):
        return None
    return abs(left - right)


def dev_check(
    question: lg.Question, data: BenchData, loaded: LoadedSettings, anchors: Anchors
) -> Record:
    window = resolve_window(loaded, question.prereg.question_type, "development")
    found = sg.segments(sg.window_returns(data.returns["qqq"], window))
    rows = []
    for strategy_id in STRATEGIES:
        own, fixed, _ = _pair(question, data, loaded, strategy_id, window)
        summary = sg.slow_bear_summary(sg.segment_rows(own, fixed, found))
        values = {"average_exposure": float(own.exposure.mean()), **summary}
        for name in ("average_exposure", "median_difference", "min_difference"):
            value, anchor = values[name], anchors[strategy_id][name]
            difference = _gap(value, anchor)
            rows.append(
                {
                    "strategy": strategy_id,
                    "name": name,
                    "value": value,
                    "anchor": anchor,
                    "difference": difference,
                    "passed": difference is not None and difference <= TOLERANCE,
                }
            )
    return _result(
        {
            "window": _window_row(window),
            "slow_bears": sum(item.kind == "slow_bear" for item in found),
            "passed": all(row["passed"] for row in rows),
            "rows": rows,
        }
    )


def _question_problems(question: lg.Question) -> list[str]:
    problems = []
    if question.question_id != QUESTION:
        problems.append(f"题号不是 {QUESTION}")
    if question.prereg.data.signal_snapshot_id is not None:
        problems.append("data.signal_snapshot_id 不是 None")
    if any(item not in question.prereg.validation.strategies for item in STRATEGIES):
        problems.append("两条策略不全在 validation.strategies 里")
    return problems


def _decision_problems(question: lg.Question, loaded: LoadedSettings) -> list[str]:
    top = git_output(question.directory, "rev-parse", "--show-toplevel")
    ref = loaded.settings.approval_ref
    if top is None or not git_output(
        Path(top), "grep", "-F", "-l", "-e", DECISION, ref, "--", "docs/requirements"
    ):
        return [f"决定号不在 {ref} 的 docs/requirements 里"]
    return []


def _ledger_check(question: lg.Question, loaded: LoadedSettings) -> Record:
    problems = [
        *_question_problems(question),
        *rule_problems(question.prereg, loaded.settings),
        *lg.approval_problems(question, loaded.settings.approval_ref),
        *_decision_problems(question, loaded),
        *lg.verify_reads(question.directory),
        *lg.verify_runs(question.directory),
    ]
    reads = lg.window_reads(question.directory)
    if not any(item.get("role") == "validation" for item in reads):
        problems.append("没有 validation 读取")
    return {
        "passed": not problems,
        "problems": problems,
        "reads": len(reads),
        "next_read": f"{question.question_id}-w{len(reads) + 1:04d}",
    }


def _data_check(data: BenchData) -> Record:
    columns: dict[str, Record] = {}
    for name, frame in (("qqq", data.returns), ("cash", data.returns), ("ndx_return", data.inputs)):
        series = frame.loc[frame.index >= pd.Timestamp(DATA_CHECK_START), name]
        valid = series.dropna()
        columns[name] = {
            "days": len(valid),
            "missing": int(series.isna().sum()),
            "last": None if valid.empty else pd.Timestamp(valid.index[-1]).date().isoformat(),
        }
    return {
        "start": DATA_CHECK_START.isoformat(),
        "columns": columns,
        "passed": all(item["days"] > 0 and item["missing"] == 0 for item in columns.values()),
    }


def check(
    question: lg.Question,
    data: BenchData,
    loaded: LoadedSettings,
    *,
    anchors: Anchors | None = None,
) -> Record:
    result: Record = {
        "run_id": RUN_ID,
        "dev_check": None,
        "ledger": None,
        "data": None,
        "errors": [],
    }
    actions: tuple[tuple[str, Callable[[], Record]], ...] = (
        (
            "dev_check",
            lambda: dev_check(question, data, loaded, ANCHORS if anchors is None else anchors),
        ),
        ("ledger", lambda: _ledger_check(question, loaded)),
        ("data", lambda: _data_check(data)),
    )
    for name, action in actions:
        try:
            result[name] = action()
        except Exception as exc:
            result["errors"].append(f"{name}: {error_text(exc)}")
    result["passed"] = not result["errors"] and all(
        result[name] is not None and result[name]["passed"]
        for name in ("dev_check", "ledger", "data")
    )
    return _result(result)


def recovery_month(level: pd.Series, segment: sg.Segment) -> str | None:
    if segment.peak is None or segment.trough is None:
        return None
    months = pd.PeriodIndex(level.index, freq="M")
    high = float(level.loc[segment.peak])
    recovered = level.loc[(months > pd.Period(segment.trough, freq="M")) & (level >= high)]
    return None if recovered.empty else str(recovered.index[0])


def _segment_check(level: pd.Series, found: Sequence[sg.Segment]) -> Record:
    rows = [
        {
            "id": item.id,
            "kind": item.kind,
            "start": item.start,
            "end": item.end,
            "peak": item.peak,
            "trough": item.trough,
            "recovered": recovery_month(level, item)
            if item.kind in ("slow_bear", "fast_drop")
            else None,
            "months": item.months,
        }
        for item in found
    ]
    bears = [row for row in rows if row["kind"] == "slow_bear"]
    return {
        "first_month": str(level.index[0]),
        "last_month": str(level.index[-1]),
        "slow_bears": len(bears),
        "passed": len(bears) == 1 and bears[0]["recovered"] is not None,
        "rows": rows,
    }


def outside_segments(
    level: pd.Series, bear: sg.Segment, found: Sequence[sg.Segment]
) -> list[sg.Segment]:
    months = pd.PeriodIndex(level.index, freq="M")
    peak, trough = pd.Period(bear.peak, freq="M"), pd.Period(bear.trough, freq="M")
    before = sg.Segment(
        f"pre_peak_{peak.strftime('%Y%m')}",
        "pre_peak",
        str(months[0]),
        str(peak),
        None,
        None,
        peak.ordinal - months[0].ordinal + 1,
        None,
    )
    result = [before]
    first, last = trough + 1, months[-1]
    if first <= last:
        after = next(
            (
                item
                for item in found
                if item.kind == "long_bull" and item.start == str(first) and item.end == str(last)
            ),
            None,
        )
        result.append(
            after
            if after is not None
            else sg.Segment(
                f"post_trough_{first.strftime('%Y%m')}",
                "post_trough",
                str(first),
                str(last),
                None,
                None,
                last.ordinal - first.ordinal + 1,
                None,
            )
        )
    return result


def verdicts(
    difference: float, bounds: tuple[float, float], printed: tuple[float, float]
) -> Record:
    inside = bounds[0] <= difference <= bounds[1]
    full_inside = printed[0] <= difference <= printed[1]
    return {"positive": difference > 0, "in_range": inside, "between": inside != full_inside}


def _reading(
    question: lg.Question,
    data: BenchData,
    loaded: LoadedSettings,
    strategy_id: str,
    window: Window,
    bear: sg.Segment,
    outside: Sequence[sg.Segment],
    bounds: tuple[float, float],
    full_bounds: tuple[float, float],
) -> Record:
    own, fixed, qqq = _pair(question, data, loaded, strategy_id, window)
    row = sg.segment_rows(own, fixed, [bear])[0]
    other = sg.segment_rows(own, fixed, outside)
    for item, segment in zip(other, outside, strict=True):
        item.update(
            start=segment.start,
            end=segment.end,
            months=segment.months,
            unfinished=segment.kind in ("long_bull", "post_trough"),
        )
    whole: Record = {
        "log_difference": float(np.log1p(own.returns).sum() - np.log1p(fixed.returns).sum())
    }
    for name, sim in (("strategy", own), ("benchmark", fixed), ("qqq", qqq)):
        summary = summarize(sim, data.returns["cash"], loaded)
        whole[name] = {
            key: summary[key]
            for key in ("average_exposure", "geometric_annual_return", "max_drawdown")
        }
    exposure = sg._inside(own.exposure, bear)
    monthly = exposure.groupby(pd.DatetimeIndex(exposure.index).to_period("M")).mean()
    return {
        "record": RECORDS[strategy_id],
        "verdict": "read",
        "difference": row["difference"],
        **verdicts(row["difference"], bounds, full_bounds),
        "range": list(bounds),
        "full_range": list(full_bounds),
        "strategy_return": row["strategy_return"],
        "benchmark_return": row["benchmark_return"],
        "qqq_return": sg.segment_return(sg.window_returns(data.returns["qqq"], window), bear),
        "segment_exposure": row["average_exposure"],
        "window_exposure": float(own.exposure.mean()),
        "outside": other,
        "window": whole,
        "monthly_exposure": [
            {"month": str(month), "exposure": float(value)} for month, value in monthly.items()
        ],
    }


def _run_comparison(record: Record, reading: Record) -> Record:
    if reading["verdict"] == "incomplete":
        return {"consistent": None, "rows": []}
    rows = []
    for side, name in (
        ("strategy", "average_exposure"),
        ("strategy", "geometric_annual_return"),
        ("strategy", "max_drawdown"),
        ("benchmark", "geometric_annual_return"),
    ):
        summary = (
            record["result"]["strategy"]
            if side == "strategy"
            else record["result"]["benchmarks"][PRIMARY_BENCHMARK]
        )
        recorded, computed = summary[name], reading["window"][side][name]
        difference = _gap(recorded, computed)
        rows.append(
            {
                "name": f"{side}.{name}",
                "recorded": recorded,
                "computed": computed,
                "difference": difference,
                "consistent": difference is not None and difference <= TOLERANCE,
            }
        )
    return {"consistent": all(row["consistent"] for row in rows), "rows": rows}


def _record_runs(
    question: lg.Question,
    data: BenchData,
    loaded: LoadedSettings,
    permit: WindowPermit,
    result: Record,
    progress: Progress,
) -> None:
    for strategy_id in STRATEGIES:
        before = len(lg.run_records(question.directory))
        try:
            recorded = lg.run_and_record(
                question, data, strategy_id, role=ROLE, permit=permit, loaded=loaded
            ).record
            result["runs"][strategy_id] = {
                "run_id": recorded["run_id"],
                "status": recorded["status"],
                "window_read_id": recorded["window_read_id"],
                **_run_comparison(recorded, result["readings"][strategy_id]),
                "rules": recorded["rules"],
                "sample_floor_met": recorded["sample_floor_met"],
                "note": RECORD_NOTE,
            }
            progress(f"记账 {recorded['run_id']}")
        except Exception as exc:
            records = lg.run_records(question.directory)
            text = error_text(exc)
            result["runs"][strategy_id] = {
                "run_id": records[-1]["run_id"] if len(records) > before else None,
                "status": "error",
                "error_type": type(exc).__name__,
                "error": text,
                "note": RECORD_NOTE,
            }
            result["errors"].append(f"记账 {strategy_id}: {text}")
            progress(f"记账 {strategy_id} 出错")


def read(
    question: lg.Question,
    data: BenchData,
    loaded: LoadedSettings,
    *,
    anchors: Anchors | None = None,
    ranges: Ranges | None = None,
    full_ranges: Ranges | None = None,
    progress: Progress | None = None,
) -> Record:
    anchors = ANCHORS if anchors is None else anchors
    ranges = RANGES if ranges is None else ranges
    full_ranges = FULL_RANGES if full_ranges is None else full_ranges
    say: Progress = progress if progress is not None else lambda text: None
    result: Record = {
        "run_id": RUN_ID,
        "decision": DECISION,
        "question": question.question_id,
        "records": dict(RECORDS),
        "label": LABEL,
        "tolerance": TOLERANCE,
        "anchors": anchors,
        "stage": "dev_check",
        "dev_check": None,
        "read_id": None,
        "window": None,
        "segments": None,
        "readings": {
            item: {"record": RECORDS[item], "verdict": "incomplete"} for item in STRATEGIES
        },
        "runs": {},
        "errors": [],
        "complete": False,
    }
    try:
        problems = _question_problems(question)
        if problems:
            raise ValueError("; ".join(problems))
        checked = dev_check(question, data, loaded, anchors)
        result["dev_check"] = checked
        say("开发窗口核对 " + ("过" if checked["passed"] else "不过"))
        if not checked["passed"]:
            return _result(result)
        result["stage"] = "register"
        problems = _decision_problems(question, loaded)
        if problems:
            raise ValueError("; ".join(problems))
        permit = lg.register_read(question, ROLE, loaded)
        result["read_id"] = getattr(permit, "read_id", None)
        say(f"登记 {result['read_id']}")
        result["stage"] = "segments"
        window = open_window(loaded, question.prereg.question_type, ROLE, permit)
        for strategy_id in STRATEGIES:
            lg._check_permit(question, strategy_id, ROLE, permit)
        daily = sg.window_returns(data.returns["qqq"], window)
        result["window"] = {
            **_window_row(window),
            "data_end": pd.Timestamp(daily.index[-1]).date().isoformat(),
        }
        level, found = sg.monthly_level(daily), sg.segments(daily)
        cut = _segment_check(level, found)
        result["segments"] = cut
        say("划段 " + ("过" if cut["passed"] else "不过"))
        if not cut["passed"]:
            return _result(result)
    except Exception as exc:
        result["errors"].append(f"{STAGES[result['stage']]}: {error_text(exc)}")
        return _result(result)
    bear = next(item for item in found if item.kind == "slow_bear")
    outside = outside_segments(level, bear, found)
    for strategy_id in STRATEGIES:
        try:
            result["readings"][strategy_id] = _reading(
                question,
                data,
                loaded,
                strategy_id,
                window,
                bear,
                outside,
                ranges[strategy_id],
                full_ranges[strategy_id],
            )
        except Exception as exc:
            result["errors"].append(f"算读数 {strategy_id}: {error_text(exc)}")
    say("读数算完")
    _record_runs(question, data, loaded, permit, result, say)
    result["stage"] = "done"
    result["complete"] = all(
        item["verdict"] != "incomplete" for item in result["readings"].values()
    )
    return _result(result)


def _text(value: Any) -> str:
    return "—" if value is None else str(value)


def _points(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f}"


def _exposure(value: float | None) -> str:
    return "—" if value is None else f"{value:.3f}"


def _yes(value: bool | None) -> str:
    return "—" if value is None else "对" if value else "错"


def _table(lines: list[str], headers: Sequence[str], rows: Sequence[Sequence[str]]) -> None:
    lines += ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)


def _name(strategy_id: str) -> str:
    return f"{strategy_id}（{RECORDS[strategy_id]}）"


def _render_checks(lines: list[str], result: Record) -> None:
    lines += ["", "## 开发窗口核对（和 TRADING-2565-34 的全精度数比）", ""]
    checked = result["dev_check"]
    if checked is None:
        lines.append("没算到。")
    else:
        window = checked["window"]
        lines.append(
            f"窗口 {window['window_id']}（{window['role']}），"
            f"请求 {window['start']} 到 {_text(window['end'])}，慢熊 {checked['slow_bears']} 段。"
        )
        _table(
            lines,
            ("策略", "数", "算的", "参照", "绝对差", "核对"),
            [
                [
                    row["strategy"],
                    row["name"],
                    "—" if row["value"] is None else repr(row["value"]),
                    "—" if row["anchor"] is None else repr(row["anchor"]),
                    "—" if row["difference"] is None else f"{row['difference']:.1e}",
                    _yes(row["passed"]),
                ]
                for row in checked["rows"]
            ],
        )
    window = result["window"]
    if window is not None:
        lines += [
            "",
            f"一致性窗口 {window['window_id']}（{window['role']}），"
            f"请求 {window['start']} 到 {_text(window['end'])}，数据末 {window['data_end']}。",
        ]
    lines += ["", "## 划段（QQQ 月末累计水平）", ""]
    cut = result["segments"]
    if cut is None:
        lines.append("没算到。")
        return
    lines.append(
        f"{cut['first_month']} 到 {cut['last_month']}，慢熊 {cut['slow_bears']} 段，"
        f"核对{_yes(cut['passed'])}。"
    )
    _table(
        lines,
        ("段", "种类", "首月", "末月", "高点", "谷底", "回到高点", "月数"),
        [
            [
                row["id"],
                row["kind"],
                row["start"],
                row["end"],
                _text(row["peak"]),
                _text(row["trough"]),
                "没回到"
                if row["kind"] in ("slow_bear", "fast_drop") and row["recovered"] is None
                else _text(row["recovered"]),
                str(row["months"]),
            ]
            for row in cut["rows"]
        ],
    )


def _render_readings(lines: list[str], readings: Record) -> None:
    lines += ["", f"## 慢熊段读数（{LABEL}）", ""]
    _table(
        lines,
        (
            "策略",
            "D（点）",
            "判用的区间（点）",
            "全精度区间（点，只印）",
            "D > 0",
            "区间",
            "策略收益（点）",
            "对照收益（点）",
            "QQQ 收益（点）",
            "段内平均敞口",
            "整窗平均敞口",
        ),
        [
            [
                _name(strategy_id),
                _points(item["difference"]),
                " 到 ".join(_points(value) for value in item["range"]),
                " 到 ".join(
                    "—" if value is None else repr(value * 100) for value in item["full_range"]
                ),
                _yes(item["positive"]),
                _yes(item["in_range"]) + ("（落在两种区间之间）" if item["between"] else ""),
                *[
                    _points(item[key])
                    for key in ("strategy_return", "benchmark_return", "qqq_return")
                ],
                _exposure(item["segment_exposure"]),
                _exposure(item["window_exposure"]),
            ]
            for strategy_id, item in readings.items()
        ],
    )
    lines += ["", "## 段外两段和整个窗口", "", "### 段外", ""]
    _table(
        lines,
        (
            "策略",
            "种类",
            "首月",
            "末月",
            "月数",
            "策略收益（点）",
            "对照收益（点）",
            "差（点）",
            "年化差（点）",
            "平均敞口",
        ),
        [
            [
                _name(strategy_id),
                row["kind"] + ("（没完）" if row["unfinished"] else ""),
                row["start"],
                row["end"],
                str(row["months"]),
                _points(row["strategy_return"]),
                _points(row["benchmark_return"]),
                _points(row["difference"]),
                _points(row.get("annual_difference")),
                _exposure(row["average_exposure"]),
            ]
            for strategy_id, item in readings.items()
            for row in item["outside"]
        ],
    )
    lines += ["", "### 整个窗口", ""]
    _table(
        lines,
        ("策略", "序列", "平均敞口", "几何年化收益（点）", "最大回撤（点）", "整窗对数差（点）"),
        [
            [
                _name(strategy_id),
                side,
                _exposure(item["window"][side]["average_exposure"]),
                _points(item["window"][side]["geometric_annual_return"]),
                _points(item["window"][side]["max_drawdown"]),
                _points(item["window"]["log_difference"]),
            ]
            for strategy_id, item in readings.items()
            for side in ("strategy", "benchmark", "qqq")
        ],
    )
    lines += ["", "### 慢熊段逐月平均持有敞口", ""]
    _table(
        lines,
        ("策略", "月份", "平均持有敞口"),
        [
            [_name(strategy_id), row["month"], _exposure(row["exposure"])]
            for strategy_id, item in readings.items()
            for row in item["monthly_exposure"]
        ],
    )


def _render_runs(lines: list[str], runs: Record) -> None:
    lines += ["", f"## 记账的运行（{RECORD_NOTE}）", ""]
    _table(
        lines,
        ("策略", "运行号", "状态", "和读数比", "规则成立几条"),
        [
            [
                _name(strategy_id),
                _text(run["run_id"]),
                run["status"] + (f"（{run['error_type']}）" if run["status"] == "error" else ""),
                "—" if run.get("consistent") is None else "一致" if run["consistent"] else "不一致",
                "—"
                if run.get("rules") is None
                else f"{sum(bool(row['holds']) for row in run['rules'])}/{len(run['rules'])}",
            ]
            for strategy_id, run in runs.items()
        ],
    )
    for strategy_id, run in runs.items():
        for rule in run.get("rules") or []:
            lines.append(
                f"- {_name(strategy_id)}：{rule['metric']} {rule['op']} "
                f"{_points(rule['value'])} 点，实际 {_points(rule['observed'])} 点，"
                f"成立 {_yes(rule['holds'])}。"
            )
        if run.get("error"):
            lines.append(f"- {_name(strategy_id)}：{run['error']}")


def render(result: Record) -> str:
    lines = [
        "# QQQ 一致性窗口 2022 慢熊段的一次读取",
        "",
        f"依据 {result['decision']}，第 6 期章程第 18 节；"
        f"停在{STAGES[result['stage']]}；读取号：{result['read_id'] or '没登记'}。",
        f"{LABEL}。",
    ]
    for strategy_id in STRATEGIES:
        item = result["readings"][strategy_id]
        if item["verdict"] == "incomplete":
            lines.append(f"- {_name(strategy_id)}：incomplete，没判")
        else:
            lines.append(
                f"- {_name(strategy_id)}：D = {_points(item['difference'])} 点，"
                f"D > 0 {_yes(item['positive'])}，区间 {_yes(item['in_range'])}"
            )
    _render_checks(lines, result)
    readings = {key: item for key, item in result["readings"].items() if item["verdict"] == "read"}
    if readings:
        _render_readings(lines, readings)
    if result["runs"]:
        _render_runs(lines, result["runs"])
    if result["errors"]:
        lines += ["", "## 错误", "", *[f"- {error}" for error in result["errors"]]]
    lines += ["", "production_effect=none；broker_action=none。", ""]
    return "\n".join(lines)


def write_result(result: Record, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / SUMMARY).write_text(
        json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


def load(root: Path = qlh.DEFAULT_SNAPSHOT_ROOT) -> tuple[LoadedSettings, lg.Question, BenchData]:
    loaded = load_settings()
    question = lg.load_question(lg.research_root(loaded) / QUESTION)
    problems = _question_problems(question)
    if problems:
        raise ValueError("; ".join(problems))
    prereg = question.prereg
    data = from_snapshot(
        qlh.resolve_snapshot_dir(root, prereg.data.snapshot_id), qqq_series=prereg.data.qqq_series
    )
    return loaded, question, data


def _say(text: str) -> None:
    typer.echo(f"consistency_2022_read: {text}")


@app.callback()
def main() -> None:
    """先只读核对，获签预注册卡后才读一次 QQQ 的 2022 慢熊段。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option(help="长历史快照目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """只核对开发窗口、台账和数据天数，不登记、不开一致性窗口。"""
    try:
        loaded, question, data = load(root)
    except Exception as exc:
        _say(f"载入出错 {error_text(exc)}")
        raise typer.Exit(code=1) from None
    result = check(question, data, loaded)
    checked = result["dev_check"]
    if checked is not None:
        for row in checked["rows"]:
            _say(f"开发窗口 {row['strategy']} {row['name']} {_yes(row['passed'])}")
    ledger = result["ledger"]
    if ledger is not None:
        _say(f"台账 已有读取 {ledger['reads']} 次，下一个读取号 {ledger['next_read']}")
        for problem in ledger["problems"]:
            _say(f"台账 {problem}")
    if result["data"] is not None:
        for name, column in result["data"]["columns"].items():
            _say(
                f"数据 {name} 有值 {column['days']} 天，缺 {column['missing']} 天，"
                f"最后 {_text(column['last'])}"
            )
    for error in result["errors"]:
        _say(error)
    _say("check " + ("过" if result["passed"] else "不过"))
    raise typer.Exit(code=0 if result["passed"] else 1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，必须不存在或为空。")],
    root: Annotated[Path, typer.Option(help="长历史快照目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    """核对、登记、读数、记账全部结束后才写文件，不得静默重读。"""
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        _say("输出路径不是空目录，不载入、不登记")
        raise typer.Exit(code=2)
    try:
        loaded, question, data = load(root)
    except Exception as exc:
        _say(f"载入出错 {error_text(exc)}")
        raise typer.Exit(code=1) from None
    base = git_provenance(PROJECT_ROOT)
    result = read(question, data, loaded, progress=_say)
    result["base"] = base
    write_result(result, out)
    for strategy_id in STRATEGIES:
        item = result["readings"][strategy_id]
        _say(
            f"{_name(strategy_id)}incomplete"
            if item["verdict"] == "incomplete"
            else f"{_name(strategy_id)}D > 0 {_yes(item['positive'])}，"
            f"区间 {_yes(item['in_range'])}"
        )
    _say(f"写入 {out / SUMMARY} 和 {out / REPORT}")
    raise typer.Exit(code=0 if result["complete"] else 1)


if __name__ == "__main__":
    app()
