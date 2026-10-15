"""Development-only trials before opening a research question (TRADING-2565 S5b).

Three files define the process: the frozen gate ``_trials/prereg.yaml``, each candidate's
fixed plan ``_trials/T0001.yaml`` and the global append-only chain ``_trials/runs/NNNN.json``.
Unapproved or changed files, stale signal pins, duplicate candidates, wrong snapshots, broken
chains and repeat successful config/market runs are refused without recording anything.
Once a run starts, failures and errors are recorded too. Every planned configuration counts
as a trial. Only the primary configuration decides: its first successful run on either market
failing the gate means fail; both markets holding means pass; otherwise it is incomplete.
Trials never read the validation window, and passing is not investment or broker approval.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from ai_trading_system.core.provenance import git_output, git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import screen as sc
from ai_trading_system.research_bench.data import (
    BenchData,
    from_snapshot,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.prereg import (
    OWNER_DECISION_PATTERN,
    Rule,
    StrategyEntry,
    evaluate_rules,
    rule_sections,
)
from ai_trading_system.research_bench.run import BenchCheckError, RunSpec, run_strategy, spec_record
from ai_trading_system.research_bench.screen_ledger import ScreenLedgerError, screen_records
from ai_trading_system.research_bench.screen_settings import LoadedScreenSettings
from ai_trading_system.research_bench.settings import (
    EpisodeSpec,
    LoadedSettings,
    load_settings,
    resolve_window,
)
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources
from ai_trading_system.yaml_loader import safe_load_yaml_path

TRIALS_DIR = "_trials"
GATE_FILE = "prereg.yaml"
RUNS_DIR = "runs"
GATE_SCHEMA = "research_trial_gate.v1"
PLAN_SCHEMA = "research_trial_plan.v1"
RUN_SCHEMA = "research_trial_run.v1"
TRIAL_ID_PATTERN = r"^T[0-9]{4}$"
PLAN_NAME = re.compile(r"^T[0-9]{4}\.yaml$")
MARKETS: tuple[str, ...] = ("qqq", "us_market")
MARKET_NAMES = {"qqq": "QQQ", "us_market": "美股大盘"}
VERDICT_NAMES = {"pass": "过门", "fail": "没过", "incomplete": "未完成"}
# Frozen research-loop error budget: owner_decision:TRADING-2565:2026-10-15:research_loop_p1.
# Each next validation read spends 0.05/k; this display never grants permission for that read.
VALIDATION_ALPHA = 0.05
STRATEGY_FUNCTION = "signal_exposure"
Market = Literal["qqq", "us_market"]


class TrialError(RuntimeError):
    """The trial is refused before running; nothing is recorded."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MarketGate(_Strict):
    question_type: str = Field(min_length=1)
    qqq_series: Literal["proxy", "actual"] | None = None
    rules: tuple[Rule, ...] = Field(min_length=1)
    event_episodes: tuple[EpisodeSpec, ...] | None = None

    @model_validator(mode="after")
    def _episodes(self) -> MarketGate:
        for episode in self.event_episodes or ():
            if episode.end < episode.start:
                raise ValueError(f"episode {episode.id} ends before it starts")
        return self


class TrialGate(_Strict):
    schema_version: Literal["research_trial_gate.v1"]
    owner_decision: str = Field(pattern=OWNER_DECISION_PATTERN)
    max_configs_per_trial: int = Field(gt=0)
    confidence: float = Field(gt=0, lt=1)
    markets: dict[Market, MarketGate]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="before")
    @classmethod
    def _market_names(cls, value: Any) -> Any:
        if isinstance(value, dict) and isinstance(value.get("markets"), dict):
            if set(value["markets"]) != set(MARKETS):
                raise ValueError(f"the gate names exactly the markets {list(MARKETS)}")
        return value

    @model_validator(mode="after")
    def _series(self) -> TrialGate:
        if self.markets["qqq"].qqq_series is None:
            raise ValueError("the qqq market needs qqq_series")
        if self.markets["us_market"].qqq_series is not None:
            raise ValueError("the us_market market has no qqq_series")
        return self


class Candidate(_Strict):
    signals: tuple[Annotated[str, Field(pattern=r"^[a-z0-9_]+$")], ...] = Field(min_length=1)
    screens: tuple[Annotated[str, Field(pattern=r"^S[0-9]{4,}$")], ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self) -> Candidate:
        if len(set(self.signals)) != len(self.signals):
            raise ValueError("candidate signals must be unique")
        return self


class TrialData(_Strict):
    snapshot_id: str = Field(min_length=1)
    signal_snapshot_id: str = Field(pattern=r"^sig-[0-9a-f]{12}$")

    @field_validator("snapshot_id")
    @classmethod
    def _fixed_snapshot(cls, value: str) -> str:
        if value.upper() == "LATEST":
            raise ValueError("a trial plan names a fixed snapshot, not LATEST")
        return value


class TrialPlan(_Strict):
    schema_version: Literal["research_trial_plan.v1"]
    trial_id: str = Field(pattern=TRIAL_ID_PATTERN)
    candidate: Candidate
    hypothesis_card: str = Field(min_length=1)
    from_validation_window: StrictBool
    data: TrialData
    primary: str
    configs: tuple[StrategyEntry, ...] = Field(min_length=1)
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="after")
    def _configs(self) -> TrialPlan:
        ids = [config.id for config in self.configs]
        if len(set(ids)) != len(ids):
            raise ValueError("config ids must be unique")
        if self.primary not in ids:
            raise ValueError(f"primary {self.primary} is not a config")
        for config in self.configs:
            if config.function != STRATEGY_FUNCTION:
                raise ValueError(f"{config.id}: function must be signal_exposure")
            if config.delay_days != 0 or config.leveraged_benchmarks:
                raise ValueError(f"{config.id}: no delay and no leveraged benchmarks")
            signals = config.params.get("signals")
            if (
                not isinstance(signals, list)
                or not all(isinstance(signal, dict) for signal in signals)
                or sorted(str(signal.get("id")) for signal in signals)
                != sorted(self.candidate.signals)
            ):
                raise ValueError(f"{config.id}: signals must be exactly the candidate's")
            mapping = config.params.get("mapping")
            if mapping in ("linear", "threshold"):
                levels: Any = [config.params.get("low"), config.params.get("high")]
            elif mapping == "steps":
                levels = config.params.get("levels")
            else:
                raise ValueError("mapping must be linear, threshold or steps")
            if (
                not isinstance(levels, list | tuple)
                or not levels
                or any(
                    isinstance(level, bool) or not isinstance(level, int | float)
                    for level in levels
                )
            ):
                raise ValueError("exposure levels must be numbers")
            if any(not 0 <= level <= 1 for level in levels):
                raise ValueError(f"{config.id}: exposures lie between 0 and 1")
        return self

    def config(self, config_id: str) -> StrategyEntry:
        for config in self.configs:
            if config.id == config_id:
                return config
        raise KeyError(f"{self.trial_id} has no config {config_id}")


def trials_directory(root_dir: Path) -> Path:
    return root_dir.joinpath(TRIALS_DIR)


def load_gate(path: Path) -> TrialGate:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"trial gate root must be a mapping: {path}")
    return TrialGate.model_validate(raw)


def load_plan(path: Path, gate: TrialGate | None = None) -> TrialPlan:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"trial plan root must be a mapping: {path}")
    plan = TrialPlan.model_validate(raw)
    if path.name != f"{plan.trial_id}.yaml":
        raise ValueError(f"{path.name} names trial {plan.trial_id}")
    if gate is not None and len(plan.configs) > gate.max_configs_per_trial:
        raise ValueError(
            f"{plan.trial_id} has {len(plan.configs)} configs; "
            f"the gate allows {gate.max_configs_per_trial}"
        )
    return plan


def plan_paths(root_dir: Path) -> list[Path]:
    directory = trials_directory(root_dir)
    if not directory.exists():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and PLAN_NAME.match(p.name))


def load_trial(root_dir: Path, trial_id: str) -> tuple[TrialGate, TrialPlan]:
    if not re.fullmatch(TRIAL_ID_PATTERN, trial_id):
        raise TrialError(f"{trial_id} is not a trial id (T and four digits)")
    directory = trials_directory(root_dir)
    try:
        gate = load_gate(directory.joinpath(GATE_FILE))
        plan = load_plan(directory.joinpath(f"{trial_id}.yaml"), gate)
    except (OSError, ValueError) as exc:
        raise TrialError(str(exc).splitlines()[0]) from exc
    return gate, plan


def gate_problems(gate: TrialGate, loaded: LoadedSettings) -> list[str]:
    sections = rule_sections(loaded.settings)
    problems = []
    for market, entry in gate.markets.items():
        for rule in entry.rules:
            section, _, name = rule.metric.partition(".")
            if name not in sections.get(section, ()):
                problems.append(f"{market}: no metric {rule.metric}")
        try:
            resolve_window(loaded, entry.question_type, "development")
        except KeyError as exc:
            problems.append(f"{market}: {str(exc).strip(chr(39))}")
    return problems


def plan_problems(root_dir: Path, plan: TrialPlan) -> list[str]:
    problems = []
    for path in plan_paths(root_dir):
        if path.name == f"{plan.trial_id}.yaml":
            continue
        try:
            other = load_plan(path)
        except (OSError, ValueError) as exc:
            problems.append(f"{path.name} is not a valid trial plan: {str(exc).splitlines()[0]}")
            continue
        common = sorted(set(plan.candidate.signals) & set(other.candidate.signals))
        if common:
            problems.append(f"{other.trial_id} already tries {', '.join(common)}")
    for config in plan.configs:
        for pin in config.params["signals"]:
            signal_id = pin["id"]
            try:
                signal = sc.load_signal(signal_id)
            except sc.ScreenCheckError as exc:
                problems.extend(f"{config.id}: {problem}" for problem in exc.problems)
                continue
            if signal.module_sha256 != pin.get("module_sha256") or signal.spec_sha256 != pin.get(
                "spec_sha256"
            ):
                problems.append(f"{config.id}: signal {signal_id} changed since the plan")
    return problems


def approval_problems(root_dir: Path, plan: TrialPlan, gate: TrialGate, ref: str) -> list[str]:
    directory = trials_directory(root_dir)
    toplevel = git_output(directory, "rev-parse", "--show-toplevel")
    if toplevel is None:
        return [f"{directory} is not inside a git repository"]
    top = Path(toplevel).resolve()
    problems = []
    for path in (directory.joinpath(GATE_FILE), directory.joinpath(f"{plan.trial_id}.yaml")):
        rel = path.resolve().relative_to(top).as_posix()
        on_ref = git_output(top, "rev-parse", "--verify", "--quiet", f"{ref}:{rel}")
        here = git_output(top, "hash-object", "--", rel)
        if on_ref is None:
            problems.append(f"{rel} is not on {ref}")
        elif on_ref != here:
            problems.append(f"{rel} differs from the version on {ref}")
    if not git_output(
        top, "grep", "-F", "-l", "-e", gate.owner_decision, ref, "--", "docs/requirements"
    ):
        problems.append(f"{gate.owner_decision} is not in docs/requirements on {ref}")
    try:
        records = {record["screen_id"]: record for record in screen_records(top)}
    except (OSError, ScreenLedgerError) as exc:
        return [*problems, f"screening ledger is broken: {exc}"]
    for screen_id in plan.candidate.screens:
        if screen_id not in records:
            problems.append(f"screen {screen_id} is not in the screening ledger")
        elif records[screen_id]["signal_id"] not in plan.candidate.signals:
            signal_id = records[screen_id]["signal_id"]
            problems.append(
                f"screen {screen_id} screened {signal_id}, not a signal of {plan.trial_id}"
            )
    return problems


def _record_paths(root_dir: Path) -> list[Path]:
    directory = trials_directory(root_dir).joinpath(RUNS_DIR)
    if not directory.exists():
        return []
    return sorted(
        (p for p in directory.iterdir() if p.is_file() and lg.RECORD_NAME.match(p.name)),
        key=lambda p: int(p.stem),
    )


def verify_trial_runs(root_dir: Path) -> list[str]:
    directory = trials_directory(root_dir).joinpath(RUNS_DIR)
    if not directory.exists():
        return []
    problems = [
        f"runs/{p.name} is not a trial run record"
        for p in sorted(directory.iterdir())
        if not (p.is_file() and lg.RECORD_NAME.match(p.name))
    ]
    previous: str | None = None
    for position, path in enumerate(_record_paths(root_dir), start=1):
        name, number = f"runs/{path.name}", int(path.stem)
        raw = path.read_bytes()
        if number != position:
            problems.append(f"{name}: expected record {position:04d}; a record is missing")
        try:
            record = json.loads(raw)
        except ValueError:
            problems.append(f"{name} is not JSON")
            previous = lg.chain_digest(raw)
            continue
        if not isinstance(record, dict) or record.get("schema_version") != RUN_SCHEMA:
            problems.append(f"{name}: schema_version is not {RUN_SCHEMA}")
        elif record.get("seq") != number:
            problems.append(f"{name}: seq {record.get('seq')} does not match the file name")
        elif record.get("run_id") != f"trial-r{number:04d}":
            problems.append(f"{name}: run_id {record.get('run_id')} does not match the file name")
        elif record.get("prev_sha256") != previous:
            problems.append(f"{name}: prev_sha256 does not match the record before it")
        previous = lg.chain_digest(raw)
    return problems


def trial_records(root_dir: Path) -> list[dict[str, Any]]:
    problems = verify_trial_runs(root_dir)
    if problems:
        raise TrialError(f"trial runs are broken: {'; '.join(problems)}")
    return [json.loads(path.read_bytes()) for path in _record_paths(root_dir)]


def _append(root_dir: Path, body: dict[str, Any]) -> dict[str, Any]:
    seq = len(trial_records(root_dir)) + 1
    paths = _record_paths(root_dir)
    record: dict[str, Any] = lg._plain(
        {
            "schema_version": RUN_SCHEMA,
            "seq": seq,
            "run_id": f"trial-r{seq:04d}",
            "recorded_at": lg._now(),
            "prev_sha256": lg.chain_digest(paths[-1].read_bytes()) if paths else None,
            **body,
        }
    )
    directory = trials_directory(root_dir).joinpath(RUNS_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    text = json.dumps(record, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n"
    with directory.joinpath(f"{seq:04d}.json").open("xb") as handle:
        handle.write(text.encode("utf-8"))
    return record


def market_data(
    market: str,
    gate: TrialGate,
    plan: TrialPlan,
    *,
    snapshot_root: Path,
    project_root: Path,
) -> BenchData:
    snapshot = signal_sources.load_snapshot(project_root, plan.data.signal_snapshot_id)
    lags = {spec.source_id: spec.known_lag_trading_days for spec in snapshot.specs}
    if market == "qqq":
        series = gate.markets["qqq"].qqq_series
        assert series is not None  # TrialGate requires a QQQ series.
        base = from_snapshot(
            qlh.resolve_snapshot_dir(snapshot_root, plan.data.snapshot_id), qqq_series=series
        )
    elif market == "us_market":
        base = from_us_market(snapshot.frames, snapshot_id=plan.data.signal_snapshot_id)
    else:
        raise TrialError(f"unknown market {market}")
    return with_signal_sources(
        base, snapshot.frames, lags, snapshot_id=plan.data.signal_snapshot_id
    )


def _data_problems(market: Market, gate: TrialGate, plan: TrialPlan, data: BenchData) -> list[str]:
    source = data.source
    problems = []
    if source.get("signal_snapshot_id") != plan.data.signal_snapshot_id:
        problems.append(f"signal snapshot is not {plan.data.signal_snapshot_id}")
    if market == "qqq":
        if "market" in source:
            problems.append(f"data are the {source['market']} market, not qqq")
        if source.get("snapshot_id") != plan.data.snapshot_id:
            problems.append(f"long-history snapshot is not {plan.data.snapshot_id}")
        series = gate.markets["qqq"].qqq_series
        if source.get("qqq_series") != series:
            problems.append(f"QQQ series is not {series}")
    elif (
        source.get("market") != "us_market"
        or source.get("snapshot_id") != plan.data.signal_snapshot_id
    ):
        problems.append(f"data are not the us_market of {plan.data.signal_snapshot_id}")
    return problems


def run_trial(
    root_dir: Path,
    trial_id: str,
    config_id: str,
    market: str,
    data: BenchData,
    *,
    loaded: LoadedSettings | None = None,
    screen: LoadedScreenSettings | None = None,
) -> dict[str, Any]:
    """Run one planned config/market in development, recording every attempted outcome.

    Treat CRLF as LF so recorded file hashes match the committed files.
    """
    loaded = loaded or load_settings()
    gate, plan = load_trial(root_dir, trial_id)
    problems = gate_problems(gate, loaded)
    if problems:
        raise TrialError(f"the trial gate has rules no run can meet: {'; '.join(problems)}")
    if market not in gate.markets:
        raise TrialError(f"the gate has no market {market}")
    # Narrow only after checking the frozen gate's exact market set.
    market_key: Market = "qqq" if market == "qqq" else "us_market"
    try:
        config = plan.config(config_id)
    except KeyError as exc:
        raise TrialError(str(exc).strip(chr(39))) from exc
    problems = approval_problems(root_dir, plan, gate, loaded.settings.approval_ref)
    problems += plan_problems(root_dir, plan)
    if problems:
        raise TrialError(f"{trial_id} is not approved: {'; '.join(problems)}")
    problems = _data_problems(market_key, gate, plan, data)
    if problems:
        raise TrialError(f"{trial_id} data: {'; '.join(problems)}")
    directory = trials_directory(root_dir)
    plan_sha = lg.chain_digest(directory.joinpath(f"{trial_id}.yaml").read_bytes())
    for record in trial_records(root_dir):
        if record.get("trial_id") != trial_id:
            continue
        run_id = record["run_id"]
        if record.get("strategy", {}).get("module_sha256") != plan_sha:
            raise TrialError(
                f"{trial_id} changed after its run {run_id}; a changed plan is a new hypothesis"
            )
        if (
            record.get("config") == config_id
            and record.get("market") == market
            and record.get("status") == "ok"
        ):
            raise TrialError(f"{run_id} already ran {config_id} on {market}")
    entry = gate.markets[market_key]
    spec = RunSpec(
        question_type=entry.question_type,
        strategy_id=config.id,
        params=config.params,
        window_role="development",
        rebalance=config.rebalance.to_rebalance(),
        delay_days=config.delay_days,
        leveraged_benchmarks=config.leveraged_benchmarks,
    )
    window = resolve_window(loaded, entry.question_type, "development")
    body: dict[str, Any] = {
        "trial_id": trial_id,
        "config": config_id,
        "market": market,
        "strategy": {
            "id": config.id,
            "function": config.function,
            "module": f"{trial_id}.yaml",
            "module_sha256": plan_sha,
        },
        "prereg_sha256": lg.chain_digest(directory.joinpath(GATE_FILE).read_bytes()),
        "spec": spec_record(spec),
        "cost_bps": loaded.cost_bps,
        "falsification_confidence": gate.confidence,
        "window": {
            "window_id": window.window_id,
            "role": "development",
            "start": window.start.isoformat(),
            "end": None if window.end is None else window.end.isoformat(),
        },
        "source": dict(data.source),
        "settings_sha256": loaded.sha256,
        "provenance": git_provenance(directory),
    }
    try:
        result = run_strategy(
            data,
            signal_exposure,
            spec,
            loaded,
            falsification_confidence=gate.confidence,
            screen=screen,
            episodes=entry.event_episodes,
        )
    except BenchCheckError as exc:
        return _append(root_dir, {**body, "status": "check_failed", "problems": exc.problems})
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        return _append(root_dir, {**body, "status": "error", "error": error})
    outcome = result.to_dict()
    rows = evaluate_rules(entry.rules, {**(outcome.get("falsification") or {}), **outcome})
    return _append(
        root_dir,
        {
            **body,
            "status": "ok",
            "result": {
                key: outcome[key]
                for key in (
                    "strategy",
                    "benchmarks",
                    "comparison",
                    "exposure_gap",
                    "checks",
                    "fingerprint",
                    "falsification",
                    "events",
                )
            },
            "gate": rows,
            "gate_holds": all(row["holds"] for row in rows),
        },
    )


def primary_verdict(records: list[dict[str, Any]], trial_id: str, primary: str) -> str:
    first: dict[str, bool] = {}
    for record in records:
        if (
            record.get("trial_id") == trial_id
            and record.get("config") == primary
            and record.get("status") == "ok"
            and record.get("market") in MARKETS
        ):
            first.setdefault(record["market"], bool(record.get("gate_holds")))
    if any(not holds for holds in first.values()):
        return "fail"
    return "pass" if all(market in first for market in MARKETS) else "incomplete"


def trial_ledger_summary(root_dir: Path) -> dict[str, Any]:
    problems = verify_trial_runs(root_dir)
    records = [] if problems else trial_records(root_dir)
    try:
        gate: TrialGate | None = load_gate(trials_directory(root_dir).joinpath(GATE_FILE))
    except (OSError, ValueError) as exc:
        gate = None
        problems.append(f"prereg.yaml: {str(exc).splitlines()[0]}")
    trials: dict[str, Any] = {}
    for path in plan_paths(root_dir):
        try:
            plan = load_plan(path, gate)
        except (OSError, ValueError) as exc:
            problems.append(f"{path.name}: {str(exc).splitlines()[0]}")
            continue
        runs = []
        for record in records:
            if record.get("trial_id") != plan.trial_id:
                continue
            rows = record.get("gate") or []
            runs.append(
                {
                    **{
                        key: record.get(key)
                        for key in ("run_id", "config", "market", "status", "gate_holds")
                    },
                    "rules_held": sum(bool(row["holds"]) for row in rows),
                    "rules": len(rows),
                    "failed": [row["metric"] for row in rows if not row["holds"]],
                }
            )
        trials[plan.trial_id] = {
            "configs": len(plan.configs),
            "max_configs": gate.max_configs_per_trial if gate else None,
            "primary": plan.primary,
            "runs": runs,
            "verdict": primary_verdict(records, plan.trial_id, plan.primary),
        }
    trial_configs = sum(item["configs"] for item in trials.values())
    question_trials = int(lg.ledger_summary(root_dir)["global_trials"])
    return {
        "problems": problems,
        "trials": trials,
        "trial_configs": trial_configs,
        "question_trials": question_trials,
        "global_trials": question_trials + trial_configs,
    }


def validation_reads(root_dir: Path, window_id: str) -> int:
    return sum(
        read.get("role") == "validation" and read.get("window_id") == window_id
        for directory in lg.question_directories(root_dir)
        for read in lg.window_reads(directory)
    )


def trial_summary(root_dir: Path, trial_id: str, loaded: LoadedSettings) -> dict[str, Any]:
    gate, plan = load_trial(root_dir, trial_id)
    records = trial_records(root_dir)
    records = [record for record in records if record.get("trial_id") == trial_id]
    directory = trials_directory(root_dir)
    toplevel = git_output(directory, "rev-parse", "--show-toplevel")
    if toplevel is None:
        raise TrialError(f"{directory} is not inside a git repository")
    families = [
        record["family_size"]
        for record in screen_records(Path(toplevel))
        if record["screen_id"] in plan.candidate.screens
    ]
    events = None
    for record in records:
        if (
            record.get("config") == plan.primary
            and record.get("market") == "us_market"
            and record.get("status") == "ok"
        ):
            events = {
                "run_id": record["run_id"],
                "episodes": (record.get("result", {}).get("events") or {}).get("episodes", {}),
            }
            break
    window = resolve_window(loaded, gate.markets["qqq"].question_type, "validation")
    reads = validation_reads(root_dir, window.window_id)
    ledger = trial_ledger_summary(root_dir)
    return {
        "trial_id": trial_id,
        "signals": list(plan.candidate.signals),
        "screens": list(plan.candidate.screens),
        "family_size": max(families) if families else None,
        "hypothesis_card": plan.hypothesis_card,
        "from_validation_window": plan.from_validation_window,
        "data": plan.data.model_dump(),
        "configs": [config.id for config in plan.configs],
        "max_configs": gate.max_configs_per_trial,
        "primary": plan.primary,
        "verdict": primary_verdict(records, trial_id, plan.primary),
        "runs": [
            {
                key: record.get(key)
                for key in (
                    "run_id",
                    "config",
                    "market",
                    "status",
                    "gate_holds",
                    "gate",
                    "problems",
                    "error",
                )
            }
            for record in records
        ],
        "events": events,
        "validation": {
            "window_id": window.window_id,
            "reads": reads,
            "next_k": reads + 1,
            "alpha": VALIDATION_ALPHA / (reads + 1),
        },
        **{key: ledger[key] for key in ("trial_configs", "question_trials", "global_trials")},
    }


def _number(value: Any, fmt: str = "", scale: float = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    return format(value if scale == 1 else value * scale, fmt)


def _percent(value: Any) -> str:
    text = _number(value, ".1f", 100)
    return text if text == "-" else text + "%"


def trial_report(root_dir: Path, trial_id: str, loaded: LoadedSettings) -> list[str]:
    item = trial_summary(root_dir, trial_id, loaded)
    validation = item["validation"]
    lines = [
        f"# 试跑 {trial_id}",
        "",
        f"- 信号：{', '.join(item['signals'])}；筛选记录：{', '.join(item['screens'])}"
        f"（筛选时家族大小 {_number(item['family_size'])}）",
        f"- 假设卡：{item['hypothesis_card']}",
        f"- 想法来自验证窗口：{'是' if item['from_validation_window'] else '否'}",
        f"- 数据：长历史快照 {item['data']['snapshot_id']}；"
        f"信号快照 {item['data']['signal_snapshot_id']}",
        f"- 判定（主配置 {item['primary']}）：{VERDICT_NAMES[item['verdict']]}",
        f"- 配置 {len(item['configs'])} 个（上限 {item['max_configs']}）；"
        f"全局总试验数 {item['global_trials']}（各题开发试验 {item['question_trials']}，"
        f"试跑配置 {item['trial_configs']}）",
        f"- 验证窗口 {validation['window_id']} 已读 {validation['reads']} 次；"
        f"再读是第 {validation['next_k']} 次，显著性 0.05/{validation['next_k']} = "
        f"{validation['alpha']:.4f}",
    ]
    for run in item["runs"]:
        lines.extend(
            [
                "",
                f"## {run['config']} · {MARKET_NAMES[run['market']]}"
                f"（{run['run_id']}，{run['status']}）",
                "",
            ]
        )
        if run["status"] == "check_failed":
            lines.append(f"检查未通过：{'; '.join(run['problems'])}")
        elif run["status"] == "error":
            lines.append(f"出错：{run['error']}")
        else:
            lines.extend(["| 规则 | 阈值 | 实际 | 成立 |", "|---|---|---|---|"])
            for row in run["gate"]:
                lines.append(
                    f"| {row['metric']} | {row['op']} {_number(row['value'], 'g')} | "
                    f"{_number(row['observed'], '.4f')} | {'是' if row['holds'] else '否'} |"
                )
    events = item["events"]
    if events is not None:
        lines.extend(
            [
                "",
                f"## 美股大盘快跌段（{item['primary']}，{events['run_id']}）",
                "",
                "| 段 | 天数 | 减仓天数占比 | 没躲开的涨跌 | 退出延迟（天） | 回补延迟（天） |",
                "|---|---|---|---|---|---|",
            ]
        )
        for episode_id, row in events["episodes"].items():
            cells = [
                episode_id,
                _number(row["days"]),
                _percent(row["derisked_share"]),
                _percent(row["missed_loss"]),
                _number(row["exit_lag_days"]),
                _number(row["reentry_lag_days"]),
            ]
            lines.append("| " + " | ".join(cells) + " |")
    return lines
