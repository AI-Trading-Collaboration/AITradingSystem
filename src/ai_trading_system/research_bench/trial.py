"""Development-only trials before opening a research question (TRADING-2565 S5b).

Three files define the process: the frozen gate ``_trials/prereg.yaml``, a fixed plan per
candidate or owner decision (``_trials/T0001.yaml``), and the global append-only chain
``_trials/runs/NNNN.json``.
Unapproved or changed files, stale signal pins, duplicate candidates, wrong snapshots, broken
chains and repeat successful config/market runs are refused without recording anything.
Once a run starts, failures and errors are recorded too. Every planned configuration counts
as a trial. Only the primary configuration decides: its first successful run on either market
failing the gate means fail; both markets holding means pass; otherwise it is incomplete.
Trials never read the validation window, and passing is not investment or broker approval.

持有现金以外储备的固定比例配置按组合尺子判（``research_bench.portfolio``，
owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler）：门的 ``portfolio_rules``
判它点名的窗口，择时规则只读；门没点名的开发窗口上记 ``no_ruler``：
照印尺子的数，不算过也不算没过，这个配置过不了。
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import asdict
from numbers import Real
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator, model_validator

from ai_trading_system.core.provenance import git_output, git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import screen as sc
from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench.checks import Strategy
from ai_trading_system.research_bench.data import (
    RESERVES,
    BenchData,
    from_snapshot,
    from_us_market,
    strategy_returns,
    with_reserves,
    with_signal_sources,
)
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.prereg import (
    OWNER_DECISION_PATTERN,
    Rule,
    StrategyEntry,
    evaluate_rules,
    rule_sections,
)
from ai_trading_system.research_bench.run import (
    MATCHED_VOLATILITY,
    BenchCheckError,
    RunSpec,
    run_strategy,
    spec_record,
)
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
VERDICT_NAMES = {
    "pass": "过门",
    "fail": "没过",
    "incomplete": "未完成",
    "no_ruler": "没有尺子（不算过，也不算没过）",
}
PORTFOLIO_FUNCTION = "fixed_exposure"
PORTFOLIO_RULE_SET = "portfolio"
NO_RULER = "no_ruler"
# The two-stage ruler (owner_decision:TRADING-2565:2026-10-17:ruler_40y): an opened question's
# validation read is stage 2 and only checks the direction, with the gate's QQQ rules. The research
# loop charter still allows one validation read per period; this display never grants that read.
STRATEGY_FUNCTION = "signal_exposure"
STRATEGY_FUNCTIONS: dict[str, Strategy] = {
    "signal_exposure": signal_exposure,
    "fixed_exposure": fixed_exposure,
}
RESERVE_NAMES = {
    "cash": "现金",
    "treasury_10y": "10 年期国债",
    "treasury_10y_shiller": "10 年期国债（Shiller 长期利率按月合成）",
}
# Display-only unit boundary: less than 0.1 percentage point is described as no return given up.
COST_BENEFIT_RETURN_TOLERANCE = 0.001
Market = Literal["qqq", "us_market"]
# Read-only segment table order: stage 1 first, never a rule or promotion boundary.
SEGMENT_MARKETS: tuple[Market, ...] = ("us_market", "qqq")


class TrialError(RuntimeError):
    """The trial is refused before running; nothing is recorded."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _segment_list_problems(window_id: str, items: tuple[EpisodeSpec, ...]) -> str | None:
    ordered = sorted(items, key=lambda item: item.start)
    if len({item.id for item in ordered}) != len(ordered):
        return f"{window_id}: segment ids repeat"
    for item in ordered:
        if item.end < item.start:
            return f"segment {item.id} ends before it starts"
    for a, b in zip(ordered, ordered[1:], strict=False):
        if b.start <= a.end:
            return f"{window_id}: segments {a.id} and {b.id} overlap"
    return None


class PortfolioGate(_Strict):
    windows: tuple[Annotated[str, Field(min_length=1)], ...] = Field(min_length=1)
    rules: tuple[Rule, ...] = Field(min_length=1)

    @field_validator("windows")
    @classmethod
    def _unique_windows(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("portfolio rule windows must be unique")
        return value


class MarketGate(_Strict):
    question_type: str = Field(min_length=1)
    qqq_series: Literal["proxy", "actual"] | None = None
    rules: tuple[Rule, ...] = Field(min_length=1)
    event_episodes: tuple[EpisodeSpec, ...] | None = None
    reserve_question_types: dict[str, Annotated[str, Field(min_length=1)]] | None = None
    # Like event_episodes, descriptive lists by window id, never rules:
    # owner_decision:TRADING-2565:2026-10-21:slow_bear_readonly.
    slow_bear_episodes: dict[str, tuple[EpisodeSpec, ...]] | None = None
    long_bull_episodes: dict[str, tuple[EpisodeSpec, ...]] | None = None
    # owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler：储备配置的规则和
    # 每个开发窗口的 10% 下跌段（描述性）。
    portfolio_rules: PortfolioGate | None = None
    decline_episodes: dict[str, tuple[EpisodeSpec, ...]] | None = None

    @field_validator("reserve_question_types")
    @classmethod
    def _reserve_types(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        for reserve, question_type in (value or {}).items():
            if reserve == "cash" or reserve not in RESERVES:
                raise ValueError(f"invalid reserve {reserve} in reserve_question_types")
            if not question_type.strip():
                raise ValueError("reserve question type must not be blank")
        return value

    def question_type_for(self, reserve: str) -> str:
        return (self.reserve_question_types or {}).get(reserve, self.question_type)

    def question_types(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys((self.question_type, *(self.reserve_question_types or {}).values()))
        )

    def segment_lists(
        self, window_id: str
    ) -> tuple[tuple[EpisodeSpec, ...], tuple[EpisodeSpec, ...]]:
        return (
            (self.slow_bear_episodes or {}).get(window_id, ()),
            (self.long_bull_episodes or {}).get(window_id, ()),
        )

    def segment_windows(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys((*(self.slow_bear_episodes or {}), *(self.long_bull_episodes or {})))
        )

    def decline_list(self, window_id: str) -> tuple[EpisodeSpec, ...]:
        return (self.decline_episodes or {}).get(window_id, ())

    def portfolio_rules_for(self, window_id: str) -> tuple[Rule, ...] | None:
        if self.portfolio_rules is not None and window_id in self.portfolio_rules.windows:
            return self.portfolio_rules.rules
        return None

    @model_validator(mode="after")
    def _episodes(self) -> MarketGate:
        for episode in self.event_episodes or ():
            if episode.end < episode.start:
                raise ValueError(f"episode {episode.id} ends before it starts")
        for window_id in self.segment_windows():
            slow, long = self.segment_lists(window_id)
            problem = _segment_list_problems(window_id, (*slow, *long))
            if problem is not None:
                raise ValueError(problem)
        for window_id, items in (self.decline_episodes or {}).items():
            problem = _segment_list_problems(window_id, items)
            if problem is not None:
                raise ValueError(problem)
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


class TrialConfig(StrategyEntry):
    reserve: str = "cash"

    @field_validator("reserve")
    @classmethod
    def _reserve(cls, value: str) -> str:
        if value not in RESERVES:
            raise ValueError(f"unknown reserve {value}")
        return value


def uses_portfolio_ruler(config: TrialConfig) -> bool:
    return config.function == PORTFOLIO_FUNCTION and config.reserve != "cash"


class TrialPlan(_Strict):
    schema_version: Literal["research_trial_plan.v1"]
    trial_id: str = Field(pattern=TRIAL_ID_PATTERN)
    candidate: Candidate | None = None
    owner_decision: str | None = Field(default=None, pattern=OWNER_DECISION_PATTERN)
    hypothesis_card: str = Field(min_length=1)
    from_validation_window: StrictBool
    data: TrialData
    primary: str
    configs: tuple[TrialConfig, ...] = Field(min_length=1)
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="after")
    def _configs(self) -> TrialPlan:
        if self.candidate is not None and self.owner_decision is not None:
            raise ValueError("a plan needs a candidate or owner decision, not both")
        if self.candidate is None and self.owner_decision is None:
            raise ValueError("a plan needs a screened candidate or the owner decision")
        ids = [config.id for config in self.configs]
        if len(set(ids)) != len(ids):
            raise ValueError("config ids must be unique")
        if self.primary not in ids:
            raise ValueError(f"primary {self.primary} is not a config")
        for config in self.configs:
            if config.function not in STRATEGY_FUNCTIONS:
                raise ValueError(f"{config.id}: function must be signal_exposure or fixed_exposure")
            if config.delay_days != 0 or config.leveraged_benchmarks:
                raise ValueError(f"{config.id}: no delay and no leveraged benchmarks")
            levels: Any
            if config.function == "fixed_exposure":
                if set(config.params) != {"exposure"}:
                    raise ValueError("fixed_exposure takes exactly one parameter, exposure")
                levels = [config.params["exposure"]]
            else:
                if self.candidate is None:
                    raise ValueError(f"{config.id}: signal_exposure needs a candidate")
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
                    levels = [config.params.get("low"), config.params.get("high")]
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

    def config(self, config_id: str) -> TrialConfig:
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
        for rule in (*entry.rules, *(entry.portfolio_rules.rules if entry.portfolio_rules else ())):
            section, _, name = rule.metric.partition(".")
            if name not in sections.get(section, ()):
                problems.append(f"{market}: no metric {rule.metric}")
        windows = set()
        resolved = True
        for question_type in entry.question_types():
            try:
                windows.add(resolve_window(loaded, question_type, "development").window_id)
            except KeyError as exc:
                problems.append(f"{market}: {str(exc).strip(chr(39))}")
                resolved = False
        if resolved:
            problems.extend(
                f"{market}: segments listed for {window_id}, not a development window here"
                for window_id in entry.segment_windows()
                if window_id not in windows
            )
            problems.extend(
                f"{market}: declines listed for {window_id}, not a development window here"
                for window_id in entry.decline_episodes or {}
                if window_id not in windows
            )
            if entry.portfolio_rules is not None:
                problems.extend(
                    f"{market}: portfolio rules for {window_id}, not a development window here"
                    for window_id in entry.portfolio_rules.windows
                    if window_id not in windows
                )
        if entry.portfolio_rules is not None:
            problems.extend(
                f"{market}: portfolio rules for {window_id}, which lists no declines"
                for window_id in entry.portfolio_rules.windows
                if not entry.decline_list(window_id)
            )
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
        if plan.candidate is not None and other.candidate is not None:
            common = sorted(set(plan.candidate.signals) & set(other.candidate.signals))
            if common:
                problems.append(f"{other.trial_id} already tries {', '.join(common)}")
        if plan.owner_decision is not None and plan.owner_decision == other.owner_decision:
            problems.append(f"{other.trial_id} already tries {plan.owner_decision}")
    for config in plan.configs:
        if config.function != STRATEGY_FUNCTION:
            continue
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
    if plan.owner_decision is not None and not git_output(
        top, "grep", "-F", "-l", "-e", plan.owner_decision, ref, "--", "docs/requirements"
    ):
        problems.append(f"{plan.owner_decision} is not in docs/requirements on {ref}")
    if plan.candidate is None:
        return problems
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
    lags = {spec.source_id: spec for spec in snapshot.specs}
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
    return with_reserves(
        with_signal_sources(base, snapshot.frames, lags, snapshot_id=plan.data.signal_snapshot_id),
        snapshot.frames,
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


def segment_check(
    gate: TrialGate,
    plan: TrialPlan,
    datasets: Mapping[Market, BenchData],
    loaded: LoadedSettings,
    *,
    declines: bool = False,
) -> list[dict[str, Any]]:
    """重算只读段表；``declines`` 为真时改为核对组合尺子的 10% 下跌段。"""
    problems = gate_problems(gate, loaded)
    for market in SEGMENT_MARKETS:
        problems.extend(
            f"{market}: {problem}"
            for problem in _data_problems(market, gate, plan, datasets[market])
        )
    if problems:
        raise TrialError("; ".join(problems))
    rows = []
    for market in SEGMENT_MARKETS:
        entry = gate.markets[market]
        seen = set()
        for question_type in entry.question_types():
            window = resolve_window(loaded, question_type, "development")
            if window.window_id in seen:
                continue
            seen.add(window.window_id)
            daily = seg.window_returns(datasets[market].returns["qqq"], window)
            if declines:
                found = seg.declines(daily)
                listed = window.window_id in (entry.decline_episodes or {})
                differences = seg.decline_problems(found, entry.decline_list(window.window_id))
            else:
                found = seg.segments(daily)
                listed = window.window_id in entry.segment_windows()
                differences = seg.list_problems(found, *entry.segment_lists(window.window_id))
            rows.append(
                {
                    "market": market,
                    "window_id": window.window_id,
                    "start": window.start.isoformat(),
                    "end": None if window.end is None else window.end.isoformat(),
                    "segments": [asdict(s) for s in found],
                    "listed": listed,
                    "problems": differences if listed else [],
                }
            )
    return rows


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
    question_type = entry.question_type_for(config.reserve)
    window = resolve_window(loaded, question_type, "development")
    slow_bears, long_bulls = entry.segment_lists(window.window_id)
    portfolio = uses_portfolio_ruler(config)
    declines = entry.decline_list(window.window_id) if portfolio else ()
    if portfolio and not declines:
        raise TrialError(
            f"{window.window_id}: the gate lists no declines here; the portfolio ruler needs them"
        )
    if config.reserve != "cash":
        try:
            traded = strategy_returns(data, config.reserve)
        except ValueError as exc:
            raise TrialError(str(exc)) from exc
        missing = traded.loc[
            (traded.index > str(window.start))
            & (True if window.end is None else traded.index <= str(window.end)),
            "cash",
        ].isna()
        if missing.any():
            when = missing.index[missing][0].date()
            raise TrialError(f"{config.reserve} has no return on {when} inside the window")
    spec = RunSpec(
        question_type=question_type,
        strategy_id=config.id,
        params=config.params,
        window_role="development",
        rebalance=config.rebalance.to_rebalance(),
        delay_days=config.delay_days,
        leveraged_benchmarks=config.leveraged_benchmarks,
        reserve=config.reserve,
    )
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
            STRATEGY_FUNCTIONS[config.function],
            spec,
            loaded,
            falsification_confidence=gate.confidence,
            screen=screen,
            episodes=entry.event_episodes,
            slow_bears=slow_bears,
            long_bulls=long_bulls,
            declines=declines,
            portfolio_weight=float(config.params["exposure"]) if portfolio else None,
        )
    except BenchCheckError as exc:
        return _append(root_dir, {**body, "status": "check_failed", "problems": exc.problems})
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}".splitlines()[0][: lg.ERROR_TEXT_LIMIT]
        return _append(root_dir, {**body, "status": "error", "error": error})
    outcome = result.to_dict()
    values = {**(outcome.get("falsification") or {}), **outcome}
    rows = evaluate_rules(entry.rules, values)
    extra: dict[str, Any] = {}
    if portfolio:
        rules = entry.portfolio_rules_for(window.window_id)
        extra = {"rule_set": PORTFOLIO_RULE_SET, "gate_readonly": rows}
        rows = evaluate_rules(rules, values) if rules is not None else []
        extra["gate_holds"] = all(row["holds"] for row in rows) if rules is not None else None
        extra["gate_outcome"] = (
            ("pass" if extra["gate_holds"] else "fail") if rules is not None else NO_RULER
        )
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
                    "diagnostics",
                    "segments",
                )
                + (("portfolio",) if portfolio else ())
            },
            "gate": rows,
            "gate_holds": all(row["holds"] for row in rows),
            **extra,
        },
    )


def primary_verdict(records: list[dict[str, Any]], trial_id: str, primary: str) -> str:
    """只看主配置每个市场第一条成功记录；没有尺子既不算过也不算没过。"""
    first: dict[str, bool | None] = {}
    for record in records:
        if (
            record.get("trial_id") == trial_id
            and record.get("config") == primary
            and record.get("status") == "ok"
            and record.get("market") in MARKETS
        ):
            first.setdefault(
                record["market"],
                None if record.get("gate_outcome") == NO_RULER else bool(record.get("gate_holds")),
            )
    if any(holds is False for holds in first.values()):
        return "fail"
    if any(holds is None for holds in first.values()):
        return NO_RULER
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
                        for key in (
                            "run_id",
                            "config",
                            "market",
                            "status",
                            "gate_holds",
                            "gate_outcome",
                        )
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
    families = (
        [
            record["family_size"]
            for record in screen_records(Path(toplevel))
            if record["screen_id"] in plan.candidate.screens
        ]
        if plan.candidate is not None
        else []
    )
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
    segments = {}
    for market in SEGMENT_MARKETS:
        for record in records:
            if (
                record.get("config") == plan.primary
                and record.get("market") == market
                and record.get("status") == "ok"
            ):
                section = (record.get("result") or {}).get("segments")
                if section is not None:
                    segments[market] = {
                        "run_id": record["run_id"],
                        "window_id": record["window"]["window_id"],
                        **section,
                    }
                break
    window = resolve_window(loaded, gate.markets["qqq"].question_type, "validation")
    reads = validation_reads(root_dir, window.window_id)
    ledger = trial_ledger_summary(root_dir)
    return {
        "trial_id": trial_id,
        "signals": list(plan.candidate.signals) if plan.candidate is not None else [],
        "screens": list(plan.candidate.screens) if plan.candidate is not None else [],
        "owner_decision": plan.owner_decision,
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
                **{
                    key: record.get(key)
                    for key in (
                        "run_id",
                        "config",
                        "market",
                        "status",
                        "gate_holds",
                        "gate",
                        "rule_set",
                        "gate_outcome",
                        "gate_readonly",
                        "problems",
                        "error",
                    )
                },
                "function": record.get("strategy", {}).get("function"),
                "window_id": record.get("window", {}).get("window_id"),
                "portfolio": (record.get("result") or {}).get("portfolio"),
                "reserve": record.get("spec", {}).get("reserve", "cash"),
                "comparison": (record.get("result") or {}).get("comparison"),
                "diagnostics": (record.get("result") or {}).get("diagnostics"),
            }
            for record in records
        ],
        "events": events,
        "segments": segments,
        "validation": {
            "window_id": window.window_id,
            "reads": reads,
            "next_k": reads + 1,
            "rules": [
                f"{rule.metric} {rule.op} {rule.value:g}" for rule in gate.markets["qqq"].rules
            ],
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


def _finite_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, Real) and math.isfinite(value)


def cost_benefit_line(comparison: dict[str, Any]) -> str:
    """Describe the tail/drawdown change per percentage point of return given up."""

    geometric = comparison.get("geometric_annual_return", math.nan)
    tail = comparison.get("cvar_5pct_relative_improvement", math.nan)
    drawdown = comparison.get("max_drawdown", math.nan)
    if not all(_finite_number(value) for value in (geometric, tail, drawdown)):
        return "- 代价换效果：-"
    if geometric > -COST_BENEFIT_RETURN_TOLERANCE:
        return f"- 代价换效果：没有放弃收益（几何年化 {geometric * 100:+.1f} 个点）"
    points = abs(geometric) / 0.01
    return (
        f"- 代价换效果：每放弃 1 个点几何年化，左尾改善 {tail / points:.1%}，"
        f"回撤浅 {drawdown * 100 / points:.1f} 个点"
    )


def matched_volatility_line(diagnostics: dict[str, Any] | None) -> str:
    """Render the descriptive control; undefined numbers stay explicitly missing."""

    diagnostic = (diagnostics or {}).get(MATCHED_VOLATILITY)
    if diagnostic is None:
        return "- 同波动率对照：-"
    comparison = diagnostic.get("comparison") or {}

    def number(value: Any, fmt: str, scale: float = 1) -> str:
        return format(value * scale, fmt) if _finite_number(value) else "-"

    exposure = number(diagnostic.get("exposure"), ".2f")
    geometric = number(comparison.get("geometric_annual_return"), "+.1f", 100)
    drawdown = number(comparison.get("max_drawdown"), ".1f", 100)
    tail = number(comparison.get("cvar_5pct_relative_improvement"), ".1%")
    return (
        f"- 同波动率对照（QQQ 加现金，固定敞口 {exposure}）：几何年化 {geometric} 个点，"
        f"回撤浅 {drawdown} 个点，左尾改善 {tail}"
    )


def segment_lines(market: str, primary: str, section: dict[str, Any]) -> list[str]:
    where = f"{primary}，{section['run_id']}，{section['window_id']}"
    lines = [
        "",
        f"## {market}慢熊段（只读，不进判定；{where}）",
        "",
        "基准是同平均敞口的固定敞口；D 是策略段内收益减基准段内收益（点）。",
        "",
        "| 段 | 月份 | 月数 | 策略段内 | 基准段内 | D（点） | 段内平均敞口 |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in section["slow_bear"]:
        lines.append(
            f"| {row['id']} | {row['start']}..{row['end']} | {_number(row['months'])} | "
            f"{_percent(row['strategy_return'])} | {_percent(row['benchmark_return'])} | "
            f"{_number(row['difference'], '+.1f', 100)} | "
            f"{_number(row['average_exposure'], '.2f')} |"
        )
    slow = section["slow_bear_summary"]
    positive = None if slow["nonpositive"] is None else slow["count"] - slow["nonpositive"]
    lines.extend(
        [
            "",
            f"- 慢熊段 {slow['count']} 段：D 中位 "
            f"{_number(slow['median_difference'], '+.1f', 100)} 点，"
            f"为正 {_number(positive)} 段，最小 "
            f"{_number(slow['min_difference'], '+.1f', 100)} 点。",
            "",
            f"## {market}长牛段（只读，不进判定；{where}）",
            "",
            "| 段 | 月份 | 月数 | 策略年化几何 | 基准年化几何 | 差（点） | 段内平均敞口 |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in section["long_bull"]:
        lines.append(
            f"| {row['id']} | {row['start']}..{row['end']} | {_number(row['months'])} | "
            f"{_percent(row['strategy_annual'])} | {_percent(row['benchmark_annual'])} | "
            f"{_number(row['annual_difference'], '+.1f', 100)} | "
            f"{_number(row['average_exposure'], '.2f')} |"
        )
    long = section["long_bull_summary"]
    lines.extend(
        [
            "",
            f"- 长牛段 {long['count']} 段：差的平均 "
            f"{_number(long['mean_annual_difference'], '+.1f', 100)} 点，"
            f"最差 {_number(long['worst_annual_difference'], '+.1f', 100)} 点。",
        ]
    )
    return lines


def rule_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines = ["| 规则 | 阈值 | 实际 | 成立 |", "|---|---|---|---|"]
    for row in rows:
        lines.append(
            f"| {row['metric']} | {row['op']} {_number(row['value'], 'g')} | "
            f"{_number(row['observed'], '.4f')} | {'是' if row['holds'] else '否'} |"
        )
    return lines


def portfolio_lines(run: dict[str, Any]) -> list[str]:
    section = run["portfolio"] or {}
    lines = [
        "组合尺子（储备配置按它判；owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler）",
        "",
    ]
    if run["gate_outcome"] == NO_RULER:
        lines.append(
            f"- 判定：没有尺子。门的组合尺子没有 {run['window_id']} 这个窗口的门槛，"
            "不算过，也不算没过，这个配置过不了；下面的数照印。"
        )
    else:
        lines.extend(rule_lines(run["gate"]))
    days = _number(section.get("quiet_days"))
    quiet_note = (
        f"（段外 {days} 天，不到一半，用全窗口超额）"
        if section.get("quiet_fallback")
        else f"（段外 {days} 天）"
    )
    excluded = "、".join(section.get("excluded") or []) or "无"
    lines.extend(
        [
            "",
            f"- 分解：几何差 {_number(section.get('geometric_difference'), '+.2f', 100)} 点 = "
            f"储备自身超额（全窗口）{_number(section.get('reserve_excess'), '+.2f', 100)} 点 + "
            f"再平衡收益 {_number(section.get('rebalance_return'), '+.2f', 100)} 点"
            f"（QQQ 权重 {_number(section.get('weight'), '.2f')}）",
            f"- 储备对现金：全窗口超额 {_number(section.get('excess_annual'), '+.2f', 100)} 点/年；"
            f"平时超额（段外）{_number(section.get('quiet_excess'), '+.2f', 100)} 点/年"
            f"{quiet_note}",
            f"- 下跌段计入 {_number(section.get('segment_count'))} 段："
            f"r 中位 {_number(section.get('median_ratio'), '+.3f')}，"
            f"r > 0 的 {_number(section.get('positive_count'))} 段"
            f"（{_percent(section.get('positive_share'))}），"
            f"最小 {_number(section.get('min_ratio'), '+.3f')}；"
            f"不计入（B 段内跌不到 2%）：{excluded}",
            "",
            "| 段 | 月份 | 月数 | B 段内 | A 段内 | ΔD（点） | C（点） | r | 储备段内超额 |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in section.get("segments") or []:
        lines.append(
            f"| {row['id']} | {row['start']}..{row['end']} | {_number(row['months'])} | "
            f"{_percent(row['benchmark_return'])} | {_percent(row['strategy_return'])} | "
            f"{_number(row['difference'], '+.1f', 100)} | "
            f"{_number(row['cushion'], '+.1f', 100)} | {_number(row['ratio'], '+.3f')} | "
            f"{_percent(row['reserve_excess'])} |"
        )
    lines.extend(["", "择时尺子（只读，不进判定）", "", *rule_lines(run["gate_readonly"] or [])])
    return lines


def trial_report(root_dir: Path, trial_id: str, loaded: LoadedSettings) -> list[str]:
    item = trial_summary(root_dir, trial_id, loaded)
    validation = item["validation"]
    lines = [
        f"# 试跑 {trial_id}",
        "",
        (
            f"- 信号：{', '.join(item['signals'])}；筛选记录：{', '.join(item['screens'])}"
            f"（筛选时家族大小 {_number(item['family_size'])}）"
            if item["signals"]
            else f"- 设计：没有筛选候选，按 {item['owner_decision']} 试跑"
        ),
        f"- 假设卡：{item['hypothesis_card']}",
        f"- 想法来自验证窗口：{'是' if item['from_validation_window'] else '否'}",
        f"- 数据：长历史快照 {item['data']['snapshot_id']}；"
        f"信号快照 {item['data']['signal_snapshot_id']}",
        f"- 判定（主配置 {item['primary']}）：{VERDICT_NAMES[item['verdict']]}",
        f"- 配置 {len(item['configs'])} 个（上限 {item['max_configs']}）；"
        f"全局总试验数 {item['global_trials']}（各题开发试验 {item['question_trials']}，"
        f"试跑配置 {item['trial_configs']}）",
        f"- 验证窗口 {validation['window_id']} 已读 {validation['reads']} 次；"
        f"再读是第 {validation['next_k']} 次，只查方向，规则同 QQQ 开发窗口："
        f"{'；'.join(validation['rules'])}",
    ]
    for run in item["runs"]:
        reserve_label = (
            "" if run["reserve"] == "cash" else f" · 储备 {RESERVE_NAMES[run['reserve']]}"
        )
        lines.extend(
            [
                "",
                f"## {run['config']} · {MARKET_NAMES[run['market']]}{reserve_label}"
                f"（{run['run_id']}，{run['status']}）",
                "",
            ]
        )
        if run["status"] == "check_failed":
            lines.append(f"检查未通过：{'; '.join(run['problems'])}")
        elif run["status"] == "error":
            lines.append(f"出错：{run['error']}")
        else:
            if run["rule_set"] == PORTFOLIO_RULE_SET:
                lines.extend(portfolio_lines(run))
            else:
                lines.extend(rule_lines(run["gate"]))
                if run["function"] == PORTFOLIO_FUNCTION and run["reserve"] != "cash":
                    lines.append("- 这次运行早于组合尺子，按当时的门判；没有组合尺子的数，不重算。")
            lines.extend(
                [
                    "",
                    cost_benefit_line(run["comparison"] or {}),
                    matched_volatility_line(run["diagnostics"]),
                ]
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
    for market, section in item["segments"].items():
        lines.extend(segment_lines(MARKET_NAMES[market], item["primary"], section))
    return lines
