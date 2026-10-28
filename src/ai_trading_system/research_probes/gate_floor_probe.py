"""Gate-floor probe (TRADING-2565-82), period 6 charter section 22.

Authority: owner_decision:TRADING-2565:2026-10-28:gate_floor_probe. This measures
only the gate, not strategies. Both gates are read-only; changing either requires a
separate decision on the November 20 renewal card. gate_false_pass is imported,
never changed. Data, windows, cutoff, evaluation and the three zero/near-zero
families are exactly TRADING-2565-22: US market 1929-2010 and QQQ 1999-2010,
cut at 2010-12-31, without validation or consistency reads. Planted rules look ahead.

Each row is evaluated under the current gate; floors are read back from those rows.
A unit passes floor f if both v2 stages pass and the stage-1 tail point is >= f;
f = 0 means v2 without a point floor. Zero families choose the lowest holding
positive floor, volatility effect families measure detection, and held-out drawdown
families only cross-check it. Before outcomes, the reading order is fixed: incomplete,
10% loose, keep 10%, then a detection gain of at least ten points or a small gain.
Seed and inferred path measures are distinct; neither authorizes any strategy.
Before the full design, rerun the old seeds and compare every row with October 18.
No ledger is written, no trial counted, and no gate or strategy is changed.
"""

from __future__ import annotations

import json
import math
import multiprocessing
import statistics
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer

from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.prereg import Rule
from ai_trading_system.research_bench.settings import LoadedSettings, Window
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import gate_false_pass as gfp
from ai_trading_system.research_probes import gate_power as gp

RUN_ID = "TRADING-2565-82"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-28:gate_floor_probe"
# Requirement 2.1's grid: zero is v2 without a floor, printed but never selected as f*.
FLOORS = (0.0, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10)
CURRENT_FLOOR = 0.10
FIRST_SCREEN = (0.05, 0.10)
# tail_floor10's added stage-1 rule, represented as (metric, operator, value).
FLOOR_RULE = (f"comparison.{gp.TAIL_FIELD}", ">=", CURRENT_FLOOR)
CI_LOW_METRIC = f"ci_low.{gp.TAIL_FIELD}"
# Requirement 2.3: target effects, lower_to_f gain, and section 20's direction line (not a gate).
TARGETS = (0.10, 0.13, 0.15)
GAIN_LINE, SELECT_LINE = 0.10, 0.50
# Floating-point tolerance for gain comparisons, not a research threshold.
EPSILON = 1e-9
# Maximum allowed difference from the same seed's October 18 evaluation.
TOLERANCE = 1e-12
# Section 22's fixed planted effect and held-out noise grid and proportional run budget.
EFFECT_NOISES = (1.5, 1.75, 2.0, 2.25, 2.5, 2.75)
HOLDOUT_NOISES = (1.5, 2.0, 2.5)
DEFAULT_SEEDS = {"zero": 150, "effect": 80, "holdout": 40}
MIN_ZERO_SEEDS, TIMING_SEEDS, LIMIT_MINUTES = 100, 5, 150.0
# Disjoint segments, also disjoint from October 18's 20261018..20261167 seeds.
SEED_STARTS = {"zero": 20261200, "effect": 20261400, "holdout": 20261600}
SEGMENT = 200
# Data identity fields compared per market with the October 18 result.
SOURCE_KEYS = ("market", "snapshot_id", "signal_snapshot_id", "qqq_series")
# Path measure: one percentage point per bin, normal mass through this many standard errors.
BINS_PER_UNIT, SPAN = 100, 8.0
# Options document section 3's analytic estimates (pessimistic, central), reference only.
ANALYTIC = {
    0.05: {0.10: (0.35, 0.65), 0.13: (0.49, 0.80), 0.15: (0.58, 0.86)},
    0.10: {0.10: (0.35, 0.45), 0.13: (0.49, 0.67), 0.15: (0.58, 0.78)},
}
JOURNAL_FILE = "gate_floor_rows.jsonl"
EQUIVALENCE_JSON = "gate_floor_equivalence.json"
RESULT_JSON = "gate_floor_result.json"
RESULT_MD = "gate_floor_result.md"
ZERO_FAMILIES = gfp.ZERO_FAMILIES
EFFECT_FAMILIES = tuple(
    gfp.Family(f"vol_noise_{noise:g}", "effect", "volatility", noise) for noise in EFFECT_NOISES
)
HOLDOUT_FAMILIES = tuple(
    gfp.Family(f"dd_noise_{noise:g}", "holdout", "drawdown", noise) for noise in HOLDOUT_NOISES
)
FAMILIES = {f.family_id: f for f in (*ZERO_FAMILIES, *EFFECT_FAMILIES, *HOLDOUT_FAMILIES)}
STAGE_OF = {market: stage for stage, market in gfp.STAGES}
Task = tuple[str, int, tr.Market]

VERDICT_TEXT = {
    "incomplete": (
        "有评估出错、逐行门核对不同、零和近零族没有种子或效果族排不到 10%："
        "判读不完整，先查原因再读。"
    ),
    "floor10_loose": ("新种子上连 10% 线都守不住：现在的门可能比 10-18 量的松，照实报 Baker。"),
    "keep_10": "只有 10% 线守得住，降线就漏：第一道线留着，新信号靠大效果或前瞻账本。",
    "lower_to_f": (
        "线降到 f* 还守得住，效果 10% 的检出率多出至少 10 个点："
        "11-20 续期卡问 Baker 要不要把线降到 f*（C 区，另定）。"
    ),
    "small_gain": "线降到 f* 还守得住，但效果 10% 的检出率多出的不到 10 个点。",
}
SEED_MEASURE = (
    "种子口径：同一条 1929–2010 路径上，种子之间变的只是埋进去的噪声；"
    "P 量的是「在这条路径上样本内效果约 X 的规则过门的比例」，"
    "不是「真实效果 X 的规则换一条历史过门的机会」。"
)
ANCHOR_NOTE = "锚等于线，按构造最多约一半"
PATH_MEASURE = (
    "路径口径（推断）：对 x ~ N(效果, 标准误²) 求「x 不低于线且其余规则全过」的期望；"
    "标准误取包住效果的两族的隐含自助标准误中位数插值，"
    "其余规则的过率取效果族所有单元按点估计 1 个点一箱的 v2 两段都过的比例"
    "（空箱取最近的箱）。隐含自助标准误 =（点估计 − 区间下沿）÷ 1.96，"
    "区间是两侧 95% 百分位，不对称，所以是约数。"
)
LIMITS = (
    "局限：埋的规则偷看未来、没有真实代价（10-18 噪声 3 两族回撤好 11.5、29.6 点，"
    "几何每年多 1.35、3.9 点），其余几条几乎都过；真实规则的检出率不会比量出来的高。"
)
NOT_A_TRIAL = "只量门，不改试跑门，不进评估台账、筛选账和试跑账，不算试验，k 不动。"

app = typer.Typer(help="门的线探针：量试跑门第一段的 10% 线降到多低还守得住，不改门，不算试验。")


def floor_key(value: float) -> str:
    return f"{value:.2f}"


def seed_list(role: str, count: int) -> list[int]:
    if not 0 <= count <= SEGMENT:
        raise ValueError(f"seed count must be in 0..{SEGMENT}")
    return list(range(SEED_STARTS[role], SEED_STARTS[role] + count))


def scaled_seeds(zero: int) -> dict[str, int]:
    if not MIN_ZERO_SEEDS <= zero <= DEFAULT_SEEDS["zero"]:
        raise ValueError(f"near-zero seeds must be in {MIN_ZERO_SEEDS}..{DEFAULT_SEEDS['zero']}")
    return {role: count * zero // DEFAULT_SEEDS["zero"] for role, count in DEFAULT_SEEDS.items()}


def design_tasks(seeds: Mapping[str, int]) -> list[Task]:
    return [
        (family.family_id, seed, market)
        for family in FAMILIES.values()
        for seed in seed_list(family.role, seeds[family.role])
        for _, market in gfp.STAGES
    ]


def timing_tasks() -> list[Task]:
    return [
        (family.family_id, seed, market)
        for family in ZERO_FAMILIES
        for seed in gfp.seed_list(TIMING_SEEDS)
        for _, market in gfp.STAGES
    ]


def _rule_key(rule: Mapping[str, Any]) -> tuple[str, str, float]:
    return rule["metric"], rule["op"], float(rule["value"])


def _rules(rules: Sequence[Rule]) -> list[tuple[str, str, float]]:
    return [(r.metric, r.op, float(r.value)) for r in rules]


def check_gates(v2: tr.TrialGate, gate: tr.TrialGate) -> None:
    gfp.check_gate(v2)
    gfp.check_gate(gate)
    for stage, market in gfp.STAGES:
        if gate.markets[market].question_type != v2.markets[market].question_type:
            raise ValueError(f"{market}: the question type is not v2's")
        expected = _rules(v2.markets[market].rules)
        if stage == "stage1":
            expected.append(FLOOR_RULE)
        if _rules(gate.markets[market].rules) != expected:
            raise ValueError(f"{market}: the trial gate is not v2 plus the 10% floor")


def load_current_gate(root_dir: Path) -> tr.TrialGate:
    return tr.load_gate(root_dir.joinpath(tr.TRIALS_DIR, tr.GATE_FILE))


def _check_data(datasets: Mapping[str, BenchData]) -> None:
    for _, market in gfp.STAGES:
        if market not in datasets:
            raise ValueError(f"missing {market} data")
        for frame in (datasets[market].returns, datasets[market].inputs):
            if pd.Timestamp(pd.DatetimeIndex(frame.index).max()).date() > gp.CUTOFF:
                raise ValueError(f"{market} data run past {gp.CUTOFF}; cut them first")


_WORKER: dict[str, Any] = {}


def _init(payload: dict[str, Any]) -> None:
    _WORKER.clear()
    _WORKER.update(payload)


def _task(task: Task) -> dict[str, Any]:
    family_id, seed, market = task
    family = FAMILIES[family_id]
    window = _WORKER["windows"][market]
    gate: tr.TrialGate = _WORKER["gate"]
    started = time.perf_counter()
    try:
        data = _WORKER["data"][market]
        targets = gfp.family_targets(family, data.inputs, seed)
        row = gfp.evaluate(
            data, targets, window, _WORKER["loaded"], gate.markets[market].rules, gate.confidence
        )
    except Exception as exc:  # Preserve every attempted evaluation, including refusals.
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        row = gfp.failed(window, error)
    return {
        "family": family_id,
        "role": family.role,
        "seed": seed,
        "market": market,
        "stage": STAGE_OF[market],
        **row,
        "seconds": time.perf_counter() - started,
    }


def evaluate_tasks(
    tasks: Sequence[Task],
    payload: dict[str, Any],
    keep: Callable[[int, dict[str, Any]], None],
    *,
    workers: int = 1,
) -> None:
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


def _payload(
    datasets: Mapping[str, BenchData],
    windows: Mapping[str, Window],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
) -> dict[str, Any]:
    return {"data": dict(datasets), "windows": dict(windows), "loaded": loaded, "gate": gate}


def run_timing(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    *,
    workers: int = 1,
    windows: Mapping[str, Window] | None = None,
) -> dict[str, Any]:
    _check_data(datasets)
    windows = gfp.stage_windows(loaded, gate) if windows is None else windows
    gfp._check_windows(windows)
    tasks = timing_tasks()
    rows: list[dict[str, Any]] = []
    started = time.perf_counter()
    evaluate_tasks(
        tasks,
        _payload(datasets, windows, loaded, gate),
        lambda done, row: rows.append(row),
        workers=workers,
    )
    return {"tasks": len(tasks), "elapsed_seconds": time.perf_counter() - started, "rows": rows}


def _line(value: Mapping[str, Any]) -> str:
    return json.dumps(gp._clean(dict(value)), ensure_ascii=False, allow_nan=False) + "\n"


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    v2: tr.TrialGate,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    seeds: Mapping[str, int] = DEFAULT_SEEDS,
    workers: int = 1,
    windows: Mapping[str, Window] | None = None,
    progress: Callable[[int, int], None] | None = None,
    journal: Path | None = None,
) -> dict[str, Any]:
    if set(seeds) != set(DEFAULT_SEEDS) or any(count < 1 for count in seeds.values()):
        raise ValueError("every role needs at least one seed")
    _check_data(datasets)
    check_gates(v2, gate)
    windows = gfp.stage_windows(loaded, gate) if windows is None else windows
    gfp._check_windows(windows)
    tasks = design_tasks(seeds)
    trials = root_dir.joinpath(tr.TRIALS_DIR)
    # Capture the complete design and provenance before any evaluation.
    result: dict[str, Any] = {
        "probe": "gate_floor_probe",
        "run": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "provenance": git_provenance(root_dir),
        "settings_sha256": loaded.sha256,
        "gate_sha256": lg.chain_digest(gfp.GATE_COPY.read_bytes()),
        "prereg_sha256": lg.chain_digest(trials.joinpath("prereg.yaml").read_bytes()),
        "plan_sha256": lg.chain_digest(trials.joinpath(f"{gp.TRIAL_ID}.yaml").read_bytes()),
        "falsification_settings": loaded.settings.falsification.model_dump(mode="json"),
        "cutoff": gp.CUTOFF.isoformat(),
        "confidence": gate.confidence,
        "cost_bps": gfp.COST_BPS,
        "warmup_months": gfp.WARMUP_MONTHS,
        "floors": list(FLOORS),
        "targets": list(TARGETS),
        "seed_starts": dict(SEED_STARTS),
        "seeds": {role: seeds[role] for role in DEFAULT_SEEDS},
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
            for stage, market in gfp.STAGES
        },
        "rules": {
            market: [
                {"metric": r.metric, "op": r.op, "value": r.value}
                for r in gate.markets[market].rules
            ]
            for _, market in gfp.STAGES
        },
    }
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
        evaluate_tasks(tasks, _payload(datasets, windows, loaded, gate), keep, workers=workers)
    finally:
        if sink is not None:
            sink.close()
    result["evaluations"] = evaluations
    result["summary"] = summary(evaluations)
    result["reading"] = reading(result["summary"])
    return result


def read_baseline(directory: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = []
    for line in directory.joinpath(gfp.JOURNAL_FILE).read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            if "family" in row:
                rows.append(row)
    result = json.loads(directory.joinpath(gfp.RESULT_JSON).read_text(encoding="utf-8"))
    if not isinstance(result, dict):
        raise ValueError("baseline result must be a JSON object")
    return rows, result


def _close(new: Any, old: Any) -> bool:
    if new is None or old is None:
        return new is old
    return bool(abs(new - old) <= TOLERANCE)


def compare_rows(
    rows: Sequence[Mapping[str, Any]], baseline: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    old_rows = {(r["family"], r["seed"], r["market"]): r for r in baseline}
    problems: list[dict[str, Any]] = []
    for row in gp._clean(rows):
        key = (row["family"], row["seed"], row["market"])
        label = " ".join(str(x) for x in key)

        def problem(field: str, new: Any, old: Any, *, row_label: str = label) -> None:
            problems.append({"row": row_label, "field": field, "new": new, "old": old})

        old = old_rows.get(key)
        if old is None:
            problem("row", "present", None)
            continue
        if row.get("error") is not None or old.get("error") is not None:
            problem("error", row.get("error"), old.get("error"))
            continue
        for field in ("window_id", "start", "end", "return_days"):
            if row[field] != old[field]:
                problem(field, row[field], old[field])
        if not _close(row["average_exposure"], old["average_exposure"]):
            problem("average_exposure", row["average_exposure"], old["average_exposure"])
        for field in gfp.EFFECT_FIELDS:
            new_value, old_value = row["effect"].get(field), old["effect"].get(field)
            if not _close(new_value, old_value):
                problem(f"effect.{field}", new_value, old_value)
        new_rules = {_rule_key(r): r for r in row["rules"]}
        for rule in old["rules"]:
            rule_key = _rule_key(rule)
            new_rule = new_rules.get(rule_key)
            if (
                new_rule is None
                or not _close(new_rule["observed"], rule["observed"])
                or new_rule["holds"] != rule["holds"]
            ):
                metric, op, value = rule_key
                problem(f"{metric} {op} {value:g}", new_rule, rule)
    return problems


def equivalence(
    timing: Mapping[str, Any],
    baseline_rows: Sequence[Mapping[str, Any]],
    baseline_result: Mapping[str, Any],
    *,
    settings_sha256: str,
    gate_sha256: str,
    sources: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    problems = compare_rows(timing["rows"], baseline_rows)
    checks: dict[str, Any] = {
        "settings_sha256": [settings_sha256, baseline_result.get("settings_sha256")],
        "gate_sha256": [gate_sha256, baseline_result.get("gate_sha256")],
    }
    for _, market in gfp.STAGES:
        for key in SOURCE_KEYS:
            checks[f"source.{market}.{key}"] = [
                sources.get(market, {}).get(key),
                baseline_result.get("source", {}).get(market, {}).get(key),
            ]
    differ = [key for key, pair in checks.items() if pair[0] != pair[1]]
    count, expected = len(timing["rows"]), len(timing_tasks())
    return {
        "rows": count,
        "expected_rows": expected,
        "problems": problems,
        "checks": checks,
        "differ": differ,
        "ok": count == expected and not problems and not differ,
    }


def plan_seeds(timing: Mapping[str, Any], workers: int) -> dict[str, Any]:
    if not timing["rows"]:
        raise ValueError("timing needs at least one row")
    mean = statistics.fmean(row["seconds"] for row in timing["rows"])
    lanes = max(1, workers)
    full = len(design_tasks(DEFAULT_SEEDS))
    estimate = mean * full / lanes / 60
    zero = DEFAULT_SEEDS["zero"]
    if estimate > LIMIT_MINUTES:
        zero = max(MIN_ZERO_SEEDS, math.floor(zero * LIMIT_MINUTES / estimate))
    seeds = scaled_seeds(zero)
    tasks = len(design_tasks(seeds))
    return {
        "workers": workers,
        "mean_seconds": mean,
        "full_tasks": full,
        "estimate_minutes": estimate,
        "limit_minutes": LIMIT_MINUTES,
        "reduced": zero < DEFAULT_SEEDS["zero"],
        "seeds": seeds,
        "tasks": tasks,
        "planned_minutes": mean * tasks / lanes / 60,
    }


def _finite(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
        return float(value)
    return None


def units(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["family"], row["seed"]), []).append(row)
    out = []
    for (family, seed), items in grouped.items():
        stages = {stage: [r for r in items if r["stage"] == stage] for stage, _ in gfp.STAGES}
        error = any(len(rs) != 1 for rs in stages.values()) or any(
            r.get("error") is not None for r in items
        )
        point, ci_low = None, None
        v2, current = False, False
        if not error:
            first = stages["stage1"][0]
            point = _finite(first["effect"].get(gp.TAIL_FIELD))
            ci_low = next(
                (_finite(r["observed"]) for r in first["rules"] if r["metric"] == CI_LOW_METRIC),
                None,
            )
            v2 = all(
                r["holds"]
                for stage, _ in gfp.STAGES
                for r in stages[stage][0]["rules"]
                if stage != "stage1" or _rule_key(r) != FLOOR_RULE
            )
            current = all(stages[stage][0]["holds"] for stage, _ in gfp.STAGES)
        out.append(
            {
                "family": family,
                "role": items[0]["role"],
                "seed": seed,
                "point": point,
                "ci_low": ci_low,
                "v2": v2,
                "current": current,
                "error": error,
            }
        )
    return out


def passes(unit: Mapping[str, Any], floor: float) -> bool:
    if unit["error"] or not unit["v2"]:
        return False
    if floor <= 0:
        return True
    return unit["point"] is not None and bool(unit["point"] >= floor)


def floor_rows(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    zero = [u for u in items if u["role"] == "zero"]
    out = []
    for floor in FLOORS:
        ratio = gfp._ratio(sum(passes(u, floor) for u in zero), len(zero))
        families = {}
        for family in ZERO_FAMILIES:
            picked = [u for u in zero if u["family"] == family.family_id]
            families[family.family_id] = gfp._ratio(
                sum(passes(u, floor) for u in picked), len(picked)
            )
        out.append(
            {
                "floor": floor,
                **ratio,
                "holds": bool(zero) and ratio["high"] <= gfp.USABLE_LINE,
                "families": families,
            }
        )
    return out


def family_rows(
    items: Sequence[Mapping[str, Any]], families: Sequence[gfp.Family]
) -> list[dict[str, Any]]:
    out = []
    for family in families:
        picked = [u for u in items if u["family"] == family.family_id]
        points = [u["point"] for u in picked if u["point"] is not None]
        errors = [
            (u["point"] - u["ci_low"]) / gfp.WILSON_Z
            for u in picked
            if u["point"] is not None and u["ci_low"] is not None
        ]
        out.append(
            {
                "family": family.family_id,
                "noise": family.noise,
                "seeds": len(picked),
                "errors": sum(u["error"] for u in picked),
                "effect": statistics.median(points) if points else None,
                "sd": statistics.stdev(points) if len(points) >= 2 else None,
                "se": statistics.median(errors) if errors else None,
                "pass": {
                    floor_key(f): sum(passes(u, f) for u in picked) / len(picked)
                    if picked
                    else None
                    for f in FLOORS
                },
            }
        )
    return out


def interpolate(points: Sequence[tuple[float | None, float | None]], x: float) -> float | None:
    # Sort by effect only: duplicate effects retain the caller's order.
    ordered = sorted(
        ((a, b) for a, b in points if a is not None and b is not None), key=lambda p: p[0]
    )
    if not ordered or x < ordered[0][0] or x > ordered[-1][0]:
        return None
    for a, b in ordered:
        if x == a:
            return b
    for (a, b), (c, d) in zip(ordered, ordered[1:], strict=False):
        if a < x < c:
            return b + (d - b) * (x - a) / (c - a)
    return None


def detection(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, float | None]]:
    return {
        floor_key(f): {
            floor_key(t): interpolate([(r["effect"], r["pass"][floor_key(f)]) for r in rows], t)
            for t in TARGETS
        }
        for f in FLOORS
    }


def other_rules(items: Sequence[Mapping[str, Any]]) -> dict[int, float]:
    bins: dict[int, list[bool]] = {}
    for unit in items:
        if unit["role"] == "effect" and unit["point"] is not None:
            bins.setdefault(math.floor(unit["point"] * BINS_PER_UNIT), []).append(passes(unit, 0.0))
    return {k: sum(bins[k]) / len(bins[k]) for k in sorted(bins)}


def _nearest(bins: Mapping[int, float], k: int) -> float:
    return bins[k] if k in bins else bins[min(bins, key=lambda j: (abs(j - k), j))]


def path_detection(
    rows: Sequence[Mapping[str, Any]], bins: Mapping[int, float], floor: float, target: float
) -> float | None:
    se = interpolate([(r["effect"], r["se"]) for r in rows], target)
    if se is None or se <= 0 or not bins:
        return None
    normal = statistics.NormalDist(target, se)
    total = 0.0
    for k in range(
        math.floor((target - SPAN * se) * BINS_PER_UNIT),
        math.floor((target + SPAN * se) * BINS_PER_UNIT) + 1,
    ):
        low, high = k / BINS_PER_UNIT, (k + 1) / BINS_PER_UNIT
        if floor > 0:
            low = max(low, floor)
        if low < high:
            total += (normal.cdf(high) - normal.cdf(low)) * _nearest(bins, k)
    return total


def summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    items = units(rows)
    effect = family_rows(items, EFFECT_FAMILIES)
    holdout = family_rows(items, HOLDOUT_FAMILIES)
    bins = other_rules(items)
    return {
        "units": len(items),
        "errors": sum(u["error"] for u in items),
        "gate_mismatches": [
            f"{u['family']} {u['seed']}" for u in items if passes(u, CURRENT_FLOOR) != u["current"]
        ],
        "floors": floor_rows(items),
        "zero": family_rows(items, ZERO_FAMILIES),
        "effect": effect,
        "holdout": holdout,
        "detection": detection(effect),
        "path": {
            floor_key(f): {floor_key(t): path_detection(effect, bins, f, t) for t in TARGETS}
            for f in FLOORS
        },
        "holdout_detection": detection(holdout),
        "bins": {str(k): v for k, v in bins.items()},
    }


def reading(value: Mapping[str, Any]) -> dict[str, Any]:
    floors = {r["floor"]: r for r in value["floors"]}
    fstar = next((f for f in FLOORS[1:] if floors[f]["holds"]), None)
    fnext = next((f for f in FLOORS if fstar is not None and f > fstar), None)
    current_key, target_key = floor_key(CURRENT_FLOOR), floor_key(TARGETS[0])
    p10_current = value["detection"][current_key][target_key]
    p10_fstar = None if fstar is None else value["detection"][floor_key(fstar)][target_key]

    def gain(table: str) -> float | None:
        if fstar is None:
            return None
        low = value[table][floor_key(fstar)][target_key]
        high = value[table][current_key][target_key]
        return None if low is None or high is None else float(low - high)

    seed_gain, path_gain, holdout_gain = (
        gain(t) for t in ("detection", "path", "holdout_detection")
    )
    if (
        value["errors"]
        or value["gate_mismatches"]
        or floors[0.0]["total"] == 0
        or p10_current is None
        or any(r["seeds"] == 0 for r in value["effect"])
    ):
        verdict = "incomplete"
    elif not floors[CURRENT_FLOOR]["holds"]:
        verdict = "floor10_loose"
    elif fstar == CURRENT_FLOOR:
        verdict = "keep_10"
    elif seed_gain is not None and seed_gain >= GAIN_LINE - EPSILON:
        verdict = "lower_to_f"
    else:
        verdict = "small_gain"

    def differs(other: float | None) -> bool:
        return (
            verdict in ("lower_to_f", "small_gain")
            and seed_gain is not None
            and other is not None
            and (seed_gain >= GAIN_LINE - EPSILON) != (other >= GAIN_LINE - EPSILON)
        )

    return {
        "verdict": verdict,
        "fstar": fstar,
        "fnext": fnext,
        "p10_current": p10_current,
        "p10_fstar": p10_fstar,
        "gain": seed_gain,
        "path_gain": path_gain,
        "holdout_gain": holdout_gain,
        "measures_differ": differs(path_gain),
        "holdout_differ": differs(holdout_gain),
        "select": None if p10_fstar is None else p10_fstar >= SELECT_LINE,
        "gain_line": GAIN_LINE,
        "select_line": SELECT_LINE,
    }


def _gain(value: float | None) -> str:
    return "–" if value is None else f"{value * 100:+.1f} 个点"


def _detection(value: float | None) -> str:
    return "不在范围" if value is None else gp._pct(value)


def _count(row: Mapping[str, Any]) -> str:
    return f"{row['passes']}/{row['total']}"


def _table(lines: list[str], headers: Sequence[str]) -> None:
    lines.extend(["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)])


def _cells(lines: list[str], cells: Sequence[str]) -> None:
    lines.append("| " + " | ".join(cells) + " |")


def render_markdown(result: Mapping[str, Any]) -> str:
    read, summed = result["reading"], result["summary"]
    fstar, fnext = read["fstar"], read["fnext"]
    table = summed["detection"]
    floors = {r["floor"]: r for r in summed["floors"]}
    lines = [
        f"# 试跑门的线能降到多低（{RUN_ID}）",
        "",
        f"- 依据 `{OWNER_DECISION}`（第 6 期章程第 22 节）。{NOT_A_TRIAL}",
        f"- 判定：{read['verdict']}；{VERDICT_TEXT[read['verdict']]}",
    ]
    if read["measures_differ"]:
        lines.append(
            f"- 两种口径判定不同：种子口径多 {_gain(read['gain'])}，"
            f"路径口径多 {_gain(read['path_gain'])}；11-20 续期卡两个数都写。"
        )
    if read["holdout_differ"]:
        lines.append(f"- 留出的回撤族和波动族判定不同：回撤族多 {_gain(read['holdout_gain'])}。")
    if fstar is None:
        lines.append("- f* 没有：格子里没有一条线守得住。")
    else:
        next_text = "没有" if fnext is None else floor_key(fnext)
        select_text = "–" if read["select"] is None else ("到了" if read["select"] else "没到")
        lines.append(
            f"- f* = {fstar:.2f}，f* 上面一格 {next_text}；"
            f"P10(f*) {_detection(read['p10_fstar'])}，{select_text} 50%"
            "（第 20 节选方向的线，不是门的线）。"
        )
    lines.append("")
    _table(lines, ["线", "零和近零三族两段都过", "95% 上限", "守得住", "P10", "P13", "P15"])
    first = list(FIRST_SCREEN)
    for f in (fstar, fnext):
        if f is not None and f not in first:
            first.append(f)
    for f in first:
        row = floors[f]
        _cells(
            lines,
            [
                floor_key(f),
                _count(row),
                gp._pct(row["high"]),
                "是" if row["holds"] else "否",
                *(_detection(table[floor_key(f)][floor_key(t)]) for t in TARGETS),
            ],
        )
    lines += [
        "",
        f"- {SEED_MEASURE}",
        f"- P10 在 0.10 上{ANCHOR_NOTE}：效果取的是种子中位数，"
        "按定义最多一半的种子点估计不低于 10%。",
        "",
        "## 每条线（零和近零三族，两段都过）",
        "",
    ]
    _table(lines, ["线", "null", "vol_noise_3", "dd_noise_3", "合计", "95% 上限", "守得住"])
    for f in FLOORS:
        row = floors[f]
        _cells(
            lines,
            [
                floor_key(f),
                *(_count(row["families"][z.family_id]) for z in ZERO_FAMILIES),
                _count(row),
                gp._pct(row["high"]),
                "是" if row["holds"] else "否",
            ],
        )
    lines += [
        "- 线 0 是 v2（没有这条线）；10-18 在旧种子上是 31/450。"
        "守得住 = 95% Wilson 上限不超过 2.5%（450 个里最多 4 个，300 个里最多 2 个）。",
        "- 第一段左尾改善中位数："
        + "；".join(f"{r['family']} {gp._pct(r['effect'])}" for r in summed["zero"])
        + "。",
        "",
        "## 效果族（波动族，种子口径）",
        "",
    ]
    _table(lines, ["族", "噪声", "种子", "出错", "效果", *(floor_key(f) for f in FLOORS)])
    for row in summed["effect"]:
        _cells(
            lines,
            [
                row["family"],
                f"{row['noise']:g}",
                str(row["seeds"]),
                str(row["errors"]),
                gp._pct(row["effect"]),
                *(gp._pct(row["pass"][floor_key(f)]) for f in FLOORS),
            ],
        )
    lines += [
        "效果 = 第一段左尾 CVaR 5% 相对改善点估计的种子中位数；格子里是两段都过的种子比例。",
        "",
        "## 检出率（插值，种子口径）",
        "",
    ]
    _table(lines, ["线", f"P10（{ANCHOR_NOTE}）", "P13", "P15"])
    for f in FLOORS:
        _cells(
            lines, [floor_key(f), *(_detection(table[floor_key(f)][floor_key(t)]) for t in TARGETS)]
        )
    lines += ["", "## 口径对照", ""]
    _table(lines, ["族", "效果", "种子间标准差", "隐含自助标准误（约，中位数）"])
    for row in summed["effect"]:
        _cells(
            lines,
            [row["family"], gp._pct(row["effect"]), gp._pct(row["sd"]), f"约 {gp._pct(row['se'])}"],
        )
    _table(lines, ["线", "路径口径 P10", "P13", "P15"])
    for f in FLOORS:
        _cells(
            lines,
            [
                floor_key(f),
                *(_detection(summed["path"][floor_key(f)][floor_key(t)]) for t in TARGETS),
            ],
        )
    lines += [
        f"- {PATH_MEASURE}",
        f"- 从 0.10 降到 f*，P10 种子口径多 {_gain(read['gain'])}，"
        f"路径口径多 {_gain(read['path_gain'])}。",
        "",
        "## 回撤族（留出，不参与挑线和插值）",
        "",
    ]
    _table(lines, ["族", "噪声", "种子", "出错", "效果", "0.10", "f*"])
    for row in summed["holdout"]:
        _cells(
            lines,
            [
                row["family"],
                f"{row['noise']:g}",
                str(row["seeds"]),
                str(row["errors"]),
                gp._pct(row["effect"]),
                gp._pct(row["pass"][floor_key(CURRENT_FLOOR)]),
                "–" if fstar is None else gp._pct(row["pass"][floor_key(fstar)]),
            ],
        )
    held = summed["holdout_detection"]
    held_star = (
        "f* 没有。" if fstar is None else f"f* 上 {_detection(held[floor_key(fstar)]['0.10'])}。"
    )
    lines += [
        f"- 回撤族插值的 P10：0.10 上 {_detection(held['0.10']['0.10'])}，{held_star}",
        "",
        "## 解析估算（不同口径，只作参照）",
        "",
    ]
    _table(lines, ["线", "效果", "解析 悲观 / 中心", "种子口径", "路径口径"])
    for f, targets in ANALYTIC.items():
        for t, (pessimistic, central) in targets.items():
            _cells(
                lines,
                [
                    floor_key(f),
                    f"{t:.0%}",
                    f"{pessimistic:.0%} / {central:.0%}",
                    _detection(table[floor_key(f)][floor_key(t)]),
                    _detection(summed["path"][floor_key(f)][floor_key(t)]),
                ],
            )
    lines += [
        "解析式是路径口径、其余几条用猜的 0.8 / 0.9，用的是「1.645 × 标准误」，"
        "和两侧 95% 区间也不一致。",
        "",
        "## 计时和等价核对",
        "",
    ]
    if "equivalence" not in result or "plan" not in result:
        lines.append("- 这份结果没有计时和等价核对。")
    else:
        eq, plan = result["equivalence"], result["plan"]
        z, e, h = (plan["seeds"][role] for role in DEFAULT_SEEDS)
        reduction = "预计超过 150 分钟，种子按同一比例减了。" if plan["reduced"] else "没减。"
        lines += [
            f"- 10-18 的旧种子 20261018–20261022 重跑 {eq['rows']} 行，"
            f"逐行比：不同 {len(eq['problems'])} 处；"
            f"设置、门和快照号不同 {len(eq['differ'])} 项。",
            f"- 每次评估平均 {plan['mean_seconds']:.1f} 秒，{plan['workers']} 个进程，"
            f"全量 {plan['full_tasks']} 次估约 {plan['estimate_minutes']:.0f} 分钟；"
            f"种子 零和近零 {z}、效果 {e}、留出 {h}，{reduction}",
        ]
    provenance = result["provenance"]
    lines += [
        "",
        "## 门、数据和代码",
        "",
        "- 现在的门 `research/_trials/prereg.yaml` "
        f"sha256 {result['prereg_sha256'][:12]}；量的 v2 `trial_gate_v2.yaml` "
        f"sha256 {result['gate_sha256'][:12]}。",
        f"- 代码 {provenance['commit']}（改动：{provenance['code_modified']}）。",
        "- 窗口："
        + "；".join(
            f"{market} {w['window_id']} {w['start']}..{w['end']}"
            for market, w in result["windows"].items()
        )
        + "。",
        "- 种子："
        + "；".join(
            f"{role} 从 {result['seed_starts'][role]} 起 {result['seeds'][role]} 个"
            for role in DEFAULT_SEEDS
        )
        + f"；热身 {result['warmup_months']} 个月。",
        "",
        LIMITS,
        "",
        "production_effect=none；broker_action=none。",
        "",
    ]
    return "\n".join(lines)


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(gp._clean(dict(value)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_equivalence(value: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(value, out_dir.joinpath(EQUIVALENCE_JSON))


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(result, out_dir.joinpath(RESULT_JSON))
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


@app.callback()
def main() -> None:
    """量试跑门第一段的线能降到多低（TRADING-2565-82）。"""


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，拒绝覆盖已有结果。")],
    baseline: Annotated[Path, typer.Option(help="10-18 的结果目录，用于逐行等价核对。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    workers: Annotated[int, typer.Option(help="并行进程数。")] = 1,
) -> None:
    """先核对旧种子，再按固定设计量门，不写研究账本。"""
    if any(out.joinpath(name).exists() for name in (EQUIVALENCE_JSON, JOURNAL_FILE, RESULT_JSON)):
        typer.echo("gate_floor_probe: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    try:
        baseline_rows, baseline_result = read_baseline(baseline)
    except (OSError, ValueError) as exc:
        typer.echo(f"gate_floor_probe: 读不了 10-18 的结果：{exc}")
        raise typer.Exit(code=1) from None
    try:
        datasets, v2, loaded, root_dir = gfp.load_datasets(root)
        gate = load_current_gate(root_dir)
        check_gates(v2, gate)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"gate_floor_probe: 数据或门载入失败：{exc}")
        raise typer.Exit(code=1) from None
    out.mkdir(parents=True, exist_ok=True)
    typer.echo("gate_floor_probe: 数据已载入并截到 2010-12-31，先用 10-18 的旧种子计时并逐行核对。")
    timing = run_timing(datasets, loaded, gate, workers=workers)
    sources = {market: dict(data.source) for market, data in datasets.items()}
    eq = equivalence(
        timing,
        baseline_rows,
        baseline_result,
        settings_sha256=loaded.sha256,
        gate_sha256=lg.chain_digest(gfp.GATE_COPY.read_bytes()),
        sources=sources,
    )
    plan = plan_seeds(timing, workers)
    write_equivalence({"timing": timing, "equivalence": eq, "plan": plan}, out)
    if not eq["ok"]:
        details = (
            "；".join(f"{p['row']} {p['field']}" for p in eq["problems"][:3])
            if eq["problems"]
            else "、".join(eq["differ"])
        )
        typer.echo(
            f"gate_floor_probe: 和 10-18 不同：{len(eq['problems'])} 行字段、"
            f"{len(eq['differ'])} 项设置或快照（{details}），不跑全量。"
        )
        raise typer.Exit(code=1)
    z, e, h = (plan["seeds"][role] for role in DEFAULT_SEEDS)
    suffix = "（减了）。" if plan["reduced"] else "。"
    typer.echo(
        f"gate_floor_probe: 等价核对通过；每次评估约 {plan['mean_seconds']:.1f} 秒，"
        f"全量估 {plan['estimate_minutes']:.0f} 分钟；"
        f"种子 零和近零 {z}、效果 {e}、留出 {h}{suffix}"
    )

    def progress(done: int, total: int) -> None:
        if done % 50 == 0 or done == total:
            typer.echo(f"gate_floor_probe: {done}/{total}")

    result = run_probe(
        datasets,
        loaded,
        v2,
        gate,
        root_dir,
        seeds=plan["seeds"],
        workers=workers,
        progress=progress,
        journal=out.joinpath(JOURNAL_FILE),
    )
    result.update(source=sources, equivalence=eq, plan=plan)
    write_result(result, out)
    read = result["reading"]
    star = "没有" if read["fstar"] is None else floor_key(read["fstar"])
    typer.echo(
        f"gate_floor_probe: 判定 {read['verdict']}；f* {star}；写入 {out.joinpath(RESULT_JSON)}"
    )


if __name__ == "__main__":
    app()
