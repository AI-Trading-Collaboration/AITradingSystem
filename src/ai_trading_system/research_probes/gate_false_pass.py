"""False-pass probe (TRADING-2565-22): how often the trial gate lets zero-effect rules through.

This measures the gate, not a strategy. The gate is a frozen, byte-for-byte copy,
``trial_gate_v2.yaml``; no trial gate or ledger is changed. Data and the 2010-12-31 cutoff
are exactly gate_power's (trial plan T0001), with no validation or consistency read.
The two development windows are US market 1929-01-02..2010-12-31 (stage 1) and QQQ
1999-03-10..2010-12-31 (stage 2). Planted forecasts deliberately look ahead.

Three designated zero-effect families use consecutive shared seeds: pure noise, volatility
plus noise 3, and drawdown plus noise 3. Two volatility controls, noise 1 and 0.5, only
report detection rates. Warmup is 24 months: US data begin on 1926-07-01, leaving only
29 month ends before stage 1; a target must already exist on the window's first day.
Each evaluation computes only what the gate reads: simulation, matched exposure benchmark,
comparison, the 25 bps rerun, and the gate-confidence bootstrap interval. It does not run
delay, leave-out or placebo checks, enter any ledger, count as a trial, or test a rule.

Reading fixed before any run (requirement R1, Baker's card 先做探针): pool the three
zero-effect families, treating each (family, seed) as a unit that passes only if both
stages hold. A 95% Wilson upper bound <= 2.5% is usable (resume 20 signals per period),
<= 5% is reduced (resume 10, await the canary), otherwise loose (stay paused, report
to Baker). Name any zero family whose upper bound exceeds 10%; controls never decide.
Any zero-family evaluation error or no seeds makes the reading incomplete. These are
probe readings, not strategy eligibility or authorization to resume anything automatically.
"""

from __future__ import annotations

import json
import math
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.falsify import (
    FalsificationInputs,
    _rerun,
    bootstrap_interval,
    cost_section,
)
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import PRIMARY_BENCHMARK, benchmark_simulations
from ai_trading_system.research_bench.settings import (
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.simulate import simulate
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import ruler_power as rp

RUN_ID = "TRADING-2565-22"
GATE_COPY = Path(__file__).with_name("trial_gate_v2.yaml")
# Prerun design from requirement R1 and Baker's card 先做探针 (2026-10-18).
SEED_START = 20261018
DEFAULT_SEEDS = 150
DEFAULT_CONTROL_SEEDS = 20
WARMUP_MONTHS = 24
CONFIDENCE = 0.95
COST_BPS = 25.0
STAGES: tuple[tuple[str, tr.Market], ...] = (("stage1", "us_market"), ("stage2", "qqq"))
SECTIONS = ("comparison", cost_section(COST_BPS), "ci_low", "ci_high")
# Standard normal two-sided 95% quantile; fixed reading lines from the same prerun design.
WILSON_Z = 1.959963984540054
USABLE_LINE = 0.025
REDUCED_LINE = 0.05
FAMILY_LINE = 0.10
JOURNAL_FILE = "false_pass_rows.jsonl"
RESULT_JSON = "false_pass_result.json"
RESULT_MD = "false_pass_result.md"
# The three comparison fields the design reports, not additional gate conditions.
EFFECT_FIELDS = (gp.TAIL_FIELD, "max_drawdown", "geometric_annual_return")

app = typer.Typer(help="误放率探针：只量两段试跑门放过零效果规则的比例，不算试验。")


@dataclass(frozen=True)
class Family:
    family_id: str
    role: str
    signal: str
    noise: float | None


ZERO_FAMILIES = (
    Family("null", "zero", "volatility", None),
    Family("vol_noise_3", "zero", "volatility", 3.0),
    Family("dd_noise_3", "zero", "drawdown", 3.0),
)
CONTROL_FAMILIES = (
    Family("vol_noise_1", "control", "volatility", 1.0),
    Family("vol_noise_0.5", "control", "volatility", 0.5),
)
FAMILIES: dict[str, Family] = {
    family.family_id: family for family in (*ZERO_FAMILIES, *CONTROL_FAMILIES)
}


def seed_list(count: int) -> list[int]:
    if count < 0:
        raise ValueError("seed count must not be negative")
    return list(range(SEED_START, SEED_START + count))


def family_targets(
    family: Family, inputs: pd.DataFrame, seed: int, *, warmup: int = WARMUP_MONTHS
) -> pd.Series:
    if family.signal == "volatility":
        return gp.planted_targets(inputs, family.noise, seed=seed, warmup=warmup)
    if family.signal == "drawdown":
        return rp.drawdown_targets(inputs, family.noise, seed=seed, warmup=warmup)
    raise ValueError(f"unknown signal {family.signal}")


def wilson(passes: int, total: int, z: float = WILSON_Z) -> tuple[float, float]:
    if total <= 0 or not 0 <= passes <= total:
        raise ValueError("total must be positive and passes in 0..total")
    p = passes / total
    denominator = 1.0 + z**2 / total
    center = (p + z**2 / (2 * total)) / denominator
    half = z * math.sqrt(p * (1.0 - p) / total + z**2 / (4 * total**2)) / denominator
    return (
        0.0 if passes == 0 else max(0.0, center - half),
        1.0 if passes == total else min(1.0, center + half),
    )


def check_gate(gate: tr.TrialGate) -> None:
    if gate.confidence != CONFIDENCE:
        raise ValueError(f"the gate's confidence {gate.confidence} is not {CONFIDENCE}")
    for _, market in STAGES:
        if market not in gate.markets:
            raise ValueError(f"the gate has no {market} market")
        for rule in gate.markets[market].rules:
            if rule.metric.partition(".")[0] not in SECTIONS:
                raise ValueError(f"{market}: the probe cannot compute {rule.metric}")


def stage_windows(loaded: LoadedSettings, gate: tr.TrialGate) -> dict[str, Window]:
    windows: dict[str, Window] = {
        market: resolve_window(loaded, gate.markets[market].question_type, "development")
        for _, market in STAGES
    }
    _check_windows(windows)
    return windows


def _check_windows(windows: Mapping[str, Window]) -> None:
    for _, market in STAGES:
        if market not in windows:
            raise ValueError(f"missing {market} window")
        end = windows[market].end
        if end is None or end > gp.CUTOFF:
            raise ValueError(f"{market}: the window runs past {gp.CUTOFF}")


def evaluate(
    data: BenchData,
    targets: pd.Series,
    window: Window,
    loaded: LoadedSettings,
    rules: tuple[Rule, ...],
    confidence: float = CONFIDENCE,
) -> dict[str, Any]:
    """The gate-read subset of a trial's full falsification, using the same algorithms."""

    sim = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=gp.MONTH_END,
        cost_bps=loaded.cost_bps,
    )
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=gp.MONTH_END,
        cost_bps=loaded.cost_bps,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    cash = data.returns["cash"]
    comparison = compare(summarize(sim, cash, loaded), summarize(benchmark, cash, loaded), loaded)
    cost = _rerun(
        FalsificationInputs(
            returns=data.returns,
            targets=targets,
            start=window.start,
            end=window.end,
            rebalance=gp.MONTH_END,
            delay_days=0,
            cost_bps=loaded.cost_bps,
            strategy=sim,
            benchmark=benchmark,
        ),
        loaded,
        cost_bps=COST_BPS,
    )
    dates = sim.returns.index
    low, high = bootstrap_interval(
        sim.returns.to_numpy(dtype=float),
        benchmark.returns.reindex(dates).to_numpy(dtype=float),
        cash.reindex(dates).to_numpy(dtype=float),
        loaded,
        confidence,
    )
    rows = evaluate_rules(
        rules,
        {"comparison": comparison, cost_section(COST_BPS): cost, "ci_low": low, "ci_high": high},
    )
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": gp._iso(window.end),
        "average_exposure": float(sim.exposure.mean()),
        "return_days": len(sim.returns),
        "effect": {field: comparison[field] for field in EFFECT_FIELDS},
        "rules": rows,
        "holds": all(row["holds"] for row in rows),
        "error": None,
    }


def failed(window: Window, error: str) -> dict[str, Any]:
    return {
        "window_id": window.window_id,
        "start": window.start.isoformat(),
        "end": gp._iso(window.end),
        "average_exposure": None,
        "return_days": None,
        "effect": {},
        "rules": [],
        "holds": False,
        "error": error,
    }


_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _task(task: tuple[str, int, tr.Market]) -> dict[str, Any]:
    family_id, seed, market = task
    family = FAMILIES[family_id]
    window = _WORKER["windows"][market]
    gate: tr.TrialGate = _WORKER["gate"]
    try:
        data = _WORKER["data"][market]
        targets = family_targets(family, data.inputs, seed)
        row = evaluate(
            data, targets, window, _WORKER["loaded"], gate.markets[market].rules, gate.confidence
        )
    except Exception as exc:  # Preserve every attempted evaluation, including a refusal.
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        row = failed(window, error)
    return {
        "family": family_id,
        "role": family.role,
        "seed": seed,
        "market": market,
        "stage": dict((market, stage) for stage, market in STAGES)[market],
        **row,
    }


def _line(value: Mapping[str, Any]) -> str:
    return json.dumps(gp._clean(dict(value)), ensure_ascii=False, allow_nan=False) + "\n"


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    seeds: int = DEFAULT_SEEDS,
    control_seeds: int = DEFAULT_CONTROL_SEEDS,
    workers: int = 1,
    windows: Mapping[str, Window] | None = None,
    progress: Callable[[int, int], None] | None = None,
    journal: Path | None = None,
) -> dict[str, Any]:
    """Evaluate the fixed design in order; optionally flush each arriving row to a journal."""

    if seeds < 1:
        raise ValueError("at least one seed is required")
    if control_seeds < 0:
        raise ValueError("control seeds must not be negative")
    for _, market in STAGES:
        if market not in datasets:
            raise ValueError(f"missing {market} data")
        for frame in (datasets[market].returns, datasets[market].inputs):
            if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > gp.CUTOFF:
                raise ValueError(f"{market} data run past {gp.CUTOFF}; cut them first")
    check_gate(gate)
    windows = stage_windows(loaded, gate) if windows is None else windows
    _check_windows(windows)
    tasks = [
        (family.family_id, seed, market)
        for family in FAMILIES.values()
        for seed in seed_list(seeds if family.role == "zero" else control_seeds)
        for _, market in STAGES
    ]
    trials = root_dir.joinpath(tr.TRIALS_DIR)
    # Capture provenance and file contents before any evaluation, not after a long run.
    result: dict[str, Any] = {
        "probe": "gate_false_pass",
        "run": RUN_ID,
        "provenance": git_provenance(root_dir),
        "settings_sha256": loaded.sha256,
        "gate_sha256": lg.chain_digest(GATE_COPY.read_bytes()),
        "prereg_sha256": lg.chain_digest(trials.joinpath("prereg.yaml").read_bytes()),
        "plan_sha256": lg.chain_digest(trials.joinpath(f"{gp.TRIAL_ID}.yaml").read_bytes()),
        "falsification_settings": loaded.settings.falsification.model_dump(mode="json"),
        "cutoff": gp.CUTOFF.isoformat(),
        "confidence": gate.confidence,
        "cost_bps": COST_BPS,
        "warmup_months": WARMUP_MONTHS,
        "seed_start": SEED_START,
        "seeds": seeds,
        "control_seeds": control_seeds,
        "families": [
            {"family": f.family_id, "role": f.role, "signal": f.signal, "noise": f.noise}
            for f in FAMILIES.values()
        ],
        "windows": {
            market: {
                "stage": stage,
                "window_id": windows[market].window_id,
                "start": windows[market].start.isoformat(),
                "end": gp._iso(windows[market].end),
            }
            for stage, market in STAGES
        },
        "rules": {
            market: [
                {"metric": r.metric, "op": r.op, "value": r.value}
                for r in gate.markets[market].rules
            ]
            for _, market in STAGES
        },
    }
    payload = {"data": dict(datasets), "windows": dict(windows), "loaded": loaded, "gate": gate}
    evaluations: list[dict[str, Any]] = []
    sink = None if journal is None else journal.open("w", encoding="utf-8")

    def keep(done: int, row: dict[str, Any]) -> None:
        evaluations.append(row)
        if sink is not None:
            sink.write(_line(row))
            sink.flush()
        if progress is not None:
            progress(done, len(tasks))

    try:
        if sink is not None:
            sink.write(_line({**result, "tasks": len(tasks)}))
            sink.flush()
        if workers <= 1:
            _init(payload)
            for done, task in enumerate(tasks, start=1):
                keep(done, _task(task))
        else:
            context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(
                max_workers=workers, mp_context=context, initializer=_init, initargs=(payload,)
            ) as pool:
                for done, row in enumerate(pool.map(_task, tasks), start=1):
                    keep(done, row)
    finally:
        if sink is not None:
            sink.close()
    result["evaluations"] = evaluations
    result["summary"] = summary(result)
    result["reading"] = reading(result)
    return result


def _ratio(passes: int, total: int) -> dict[str, Any]:
    low, high = wilson(passes, total) if total else (None, None)
    return {
        "passes": passes,
        "total": total,
        "share": passes / total if total else None,
        "low": low,
        "high": high,
    }


def family_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """A unit is (family, seed), never two independent stage observations."""

    units: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
    for row in rows:
        units.setdefault((row["family"], row["seed"]), []).append(row)
    both = sum(
        all(
            len(stage_rows := [row for row in items if row["stage"] == stage]) == 1
            and bool(stage_rows[0]["holds"])
            and not stage_rows[0].get("error")
            for stage, _ in STAGES
        )
        for items in units.values()
    )
    out: dict[str, Any] = {
        "seeds": len(units),
        "both": _ratio(both, len(units)),
        "errors": sum(row.get("error") is not None for row in rows),
    }
    for stage, _ in STAGES:
        picked = [row for row in rows if row["stage"] == stage]
        metrics = list(dict.fromkeys(rule["metric"] for row in picked for rule in row["rules"]))
        out[stage] = {
            **_ratio(
                sum(bool(row["holds"]) and not row.get("error") for row in picked), len(picked)
            ),
            "rules": {
                metric: sum(
                    not row.get("error")
                    and any(rule["metric"] == metric and rule["holds"] for rule in row["rules"])
                    for row in picked
                )
                / len(picked)
                for metric in metrics
            },
        }
    medians: dict[str, float | None] = {}
    for field in EFFECT_FIELDS:
        values = [
            float(value)
            for row in rows
            if row["stage"] == "stage1"
            and (value := row["effect"].get(field)) is not None
            and math.isfinite(float(value))
        ]
        medians[field] = float(np.median(values)) if values else None
    out["stage1_effect_median"] = medians
    return out


def summary(result: Mapping[str, Any]) -> dict[str, Any]:
    rows = result["evaluations"]
    zero_ids = {f.family_id for f in ZERO_FAMILIES}
    return {
        "families": {
            f["family"]: family_summary([row for row in rows if row["family"] == f["family"]])
            for f in result["families"]
        },
        "zero": family_summary([row for row in rows if row["family"] in zero_ids]),
    }


def reading(result: Mapping[str, Any]) -> dict[str, Any]:
    summaries = result["summary"]
    zero = summaries["zero"]
    both = zero["both"]
    upper = both["high"]
    if zero["errors"] or both["total"] == 0:
        verdict = "incomplete"
    elif upper <= USABLE_LINE:
        verdict = "usable"
    elif upper <= REDUCED_LINE:
        verdict = "reduced"
    else:
        verdict = "loose"
    return {
        "passes": both["passes"],
        "total": both["total"],
        "share": both["share"],
        "upper": upper,
        "usable_line": USABLE_LINE,
        "reduced_line": REDUCED_LINE,
        "family_line": FAMILY_LINE,
        "flagged_families": [
            f.family_id
            for f in ZERO_FAMILIES
            if (item := summaries["families"].get(f.family_id)) is not None
            and item["both"]["high"] is not None
            and item["both"]["high"] > FAMILY_LINE
        ],
        "verdict": verdict,
    }


VERDICT_TEXT = {
    "usable": "可用：95% 上限不超过 2.5%，读法为循环恢复每期 20 个信号。",
    "reduced": "降额：95% 上限超过 2.5% 但不超过 5%，读法为恢复每期 10 个，等金丝雀。",
    "loose": "偏松：95% 上限超过 5%，继续暂停，连同放得最多的规则报 Baker。",
    "incomplete": "零效果评估出错或没有种子：判读不完整，先查错误再读。",
}


def render_markdown(result: Mapping[str, Any]) -> str:
    read = result["reading"]
    families = result["summary"]["families"]
    lines = [
        f"# 误放率探针（{RUN_ID}）",
        "",
        f"- 判读：{read['verdict']}；{VERDICT_TEXT[read['verdict']]}",
        f"- 零效果三族两段都过：{read['passes']}/{read['total']}"
        f"（{gp._pct(read['share'])}）；95% 上限 {gp._pct(read['upper'])}。",
        "- 单族 95% 上限超过 10%：" + ("、".join(read["flagged_families"]) or "无") + "。",
        f"- 门：{GATE_COPY.name}，sha256 {result['gate_sha256'][:12]}；"
        f"试跑 prereg.yaml sha256 {result['prereg_sha256'][:12]}。",
        f"- 种子从 {result['seed_start']} 起，零效果每族 {result['seeds']} 个、"
        f"对照每族 {result['control_seeds']} 个；热身 {result['warmup_months']} 个月。",
        "- 窗口："
        + "；".join(
            f"{market} {w['window_id']} {w['start']}..{w['end']}"
            for market, w in result["windows"].items()
        )
        + "。",
        "",
        "| 族 | 角色 | 第一段 | 第二段 | 两段都过 | 第一段左尾改善中位数 | 出错数 |",
        "|---|---|---|---|---|---|---|",
    ]
    for family in result["families"]:
        row = families[family["family"]]
        shares = " | ".join(gp._pct(row[key]["share"]) for key in ("stage1", "stage2", "both"))
        lines.append(
            f"| {family['family']} | {'零效果' if family['role'] == 'zero' else '对照'} | "
            f"{shares} | {gp._pct(row['stage1_effect_median'][gp.TAIL_FIELD])} | {row['errors']} |"
        )
    lines += ["", "## 每条规则成立比例", "", "| 族 | 阶段 | 规则 | 成立比例 |", "|---|---|---|---|"]
    for family in result["families"]:
        for stage, market in STAGES:
            shares_by_metric = families[family["family"]][stage]["rules"]
            for rule in result["rules"][market]:
                metric = rule["metric"]
                lines.append(
                    f"| {family['family']} | {stage} | {metric} | "
                    f"{gp._pct(shares_by_metric.get(metric))} |"
                )
    lines += ["", "只量门，不进评估台账、筛选账和试跑账，不算试验，不改试跑门。", ""]
    return "\n".join(lines)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_gate() -> tr.TrialGate:
    return tr.load_gate(GATE_COPY)


def load_datasets(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = lg.research_root(loaded)
    _, plan = tr.load_trial(root_dir, gp.TRIAL_ID)
    gate = load_gate()
    datasets: dict[str, BenchData] = {
        market: gp.cut(
            tr.market_data(
                market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
            )
        )
        for _, market in STAGES
    }
    return datasets, gate, loaded, root_dir


@app.callback()
def main() -> None:
    """量试跑门的误放率。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，保存逐行记录、JSON 和中文报告。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(help="并行进程数。")] = 1,
    seeds: Annotated[int, typer.Option(help="每个零效果族的种子数。")] = DEFAULT_SEEDS,
    control_seeds: Annotated[
        int, typer.Option(help="每个对照族的种子数。")
    ] = DEFAULT_CONTROL_SEEDS,
) -> None:
    """在两个开发窗口上量误放率，不写任何研究账本。"""

    if any(out.joinpath(name).exists() for name in (JOURNAL_FILE, RESULT_JSON)):
        typer.echo("gate_false_pass: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        datasets, gate, loaded, root_dir = load_datasets(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"gate_false_pass: 数据载入失败：{exc}")
        raise typer.Exit(code=1) from None

    def progress(done: int, total: int) -> None:
        if done % 25 == 0 or done == total:
            typer.echo(f"gate_false_pass: {done}/{total}")

    out.mkdir(parents=True, exist_ok=True)
    typer.echo("gate_false_pass: 数据已载入并截到 2010-12-31，开始两段门误放率评估。")
    result = run_probe(
        datasets,
        loaded,
        gate,
        root_dir,
        seeds=seeds,
        control_seeds=control_seeds,
        workers=workers,
        progress=progress,
        journal=out.joinpath(JOURNAL_FILE),
    )
    result["source"] = {market: dict(data.source) for market, data in datasets.items()}
    write_result(result, out)
    read = result["reading"]
    typer.echo(
        f"gate_false_pass: 判读 {read['verdict']}；两段都过 {read['passes']}/{read['total']}；"
        f"95% 上限 {gp._pct(read['upper'])}；写入 {out.joinpath(RESULT_JSON)}"
    )


if __name__ == "__main__":
    app()
